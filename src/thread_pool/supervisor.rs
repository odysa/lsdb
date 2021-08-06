use super::{pool::JobReceiver, Message};
use crossbeam::channel::{Receiver, Sender};
use std::thread::{self, JoinHandle};
/// It supervises workers
pub struct Supervisor {
    workers: Vec<Worker>,
    receiver: Receiver<Message>,
    sender: Sender<Message>,
    worker_receiver: Receiver<Message>,
    size: usize,
}

impl Supervisor {
    pub fn new(
        receiver: Receiver<Message>,
        sender: Sender<Message>,
        worker_receiver: Receiver<Message>,
        size: usize,
    ) -> Self {
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            let job_receiver = JobReceiver::new(worker_receiver.clone(), sender.clone(), id);
            let worker = Worker::new(id, job_receiver.clone());
            workers.push(worker);
        }
        Supervisor {
            receiver,
            workers,
            sender,
            worker_receiver,
            size,
        }
    }
    // listen to channel
    pub fn watch(&mut self) {
        while let Ok(message) = self.receiver.recv() {
            match message {
                Message::Dead(id) => {
                    // spawn a new worker if previous one is dead
                    let new_id = self.size + id;
                    let job_receiver =
                        JobReceiver::new(self.worker_receiver.clone(), self.sender.clone(), id);
                    let worker = Worker::new(new_id, job_receiver);
                    // find original place of worker
                    self.workers[id % self.size] = worker;
                }
                Message::Work(_) => continue,
                Message::Terminate => {
                    break;
                }
            }
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
