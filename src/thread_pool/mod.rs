use crate::error::Result;

mod pool;
pub mod supervisor;
pub use pool::QueueThreadPool;

pub trait ThreadPool {
    fn new(size: usize) -> Result<Self>
    where
        Self: Sized;

    fn execute<F>(&self, job: F) -> Result<()>
    where
        // since function works in a thread, it must have static lifetime
        F: Send + FnOnce() + 'static;
}

pub type Job = Box<dyn Send + FnOnce() + 'static>;

pub enum Message {
    Dead(usize),
    Work(Job),
    Terminate,
}
