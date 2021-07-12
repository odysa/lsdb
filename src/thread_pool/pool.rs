use super::{supervisor::Supervisor, Message, ThreadPool};
use crate::error::Error;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::thread::{self, JoinHandle};
pub struct QueueThreadPool {
    sender: Sender<Message>,
    supervisor_sender: Sender<Message>,
    size: usize,
}

impl ThreadPool for QueueThreadPool {
    fn new(size: usize) -> crate::error::Result<Self>
    where
        Self: Sized,
    {
        let (supervisor_sender, supervisor_receiver) = unbounded::<Message>();
        let (worker_sender, worker_receiver) = unbounded::<Message>();
        let pool_supervisor_sender = supervisor_sender.clone();

        thread::spawn(move || {
            let mut supervisor = Supervisor::new(
                supervisor_receiver,
                supervisor_sender,
                worker_receiver,
                size,
            );
            // supervise all
            supervisor.watch();
        });

        Ok(QueueThreadPool {
            size,
            sender: worker_sender,
            supervisor_sender: pool_supervisor_sender,
        })
    }

    fn execute<F>(&self, job: F) -> crate::error::Result<()>
    where
        // since function works in a thread, it must have static lifetime
        F: Send + FnOnce() + 'static,
    {
        match self.sender.send(Message::Work(Box::new(job))) {
            Ok(()) => Ok(()),
            Err(err) => Err(Error::from(err)),
        }
    }
}
// destroy threads when pool is dead
impl Drop for QueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.size {
            self.sender
                .send(Message::Terminate)
                .expect("unable to terminate threads");
        }

        // let supervisor stop watch
        self.supervisor_sender
            .send(Message::Terminate)
            .expect("unable to terminate threads");
    }
}

#[derive(Clone)]
pub struct JobReceiver {
    receiver: Receiver<Message>,
    // notify Supervisor
    notifier: Sender<Message>,
    id: usize,
}

impl JobReceiver {
    pub fn new(receiver: Receiver<Message>, notifier: Sender<Message>, id: usize) -> Self {
        JobReceiver {
            receiver,
            notifier,
            id,
        }
    }
    pub fn receiver(&self) -> &Receiver<Message> {
        &self.receiver
    }
}

impl Drop for JobReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            self.notifier
                .send(Message::Dead(self.id))
                .expect("unable to revive thread: cannot send message to supervisor");
        }
    }
}

pub struct Worker {
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: usize, receiver: JobReceiver) -> Worker {
        let thread = thread::spawn(move || {
            do_job(receiver);
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

// complete job
fn do_job(receiver: JobReceiver) {
    // listen to job message
    loop {
        if let Ok(message) = receiver.receiver().recv() {
            match message {
                Message::Dead(_) => break,
                Message::Work(job) => {
                    job();
                }
                Message::Terminate => break,
            }
        }
    }
}
