// ************************************************
// Imports
// ************************************************

use std::thread;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread::JoinHandle;


// ************************************************
// Enums
// ************************************************

// Object a worker will receive and process
pub enum Message {
    Terminate(),
    Dispatch(Workload),
}

pub enum WorkerState {
    Running(),
    Pending(),
}



// ************************************************
// Type aliases for cleaner syntax
// ************************************************

// Operations the threadpool should execute
type Workload = Box<dyn FnOnce() + 'static + Send>;

// non threadsafe channels
pub type TX = mpsc::Sender<Message>;    // a non thread safe sender
pub type RX = mpsc::Receiver<Message>;  // a non thread safe receiver

// threadsafe channels
pub type TSTX = Arc::<Mutex<mpsc::Sender<Message>>>;    // a thread safe sender
pub type TSRX = Arc::<Mutex<mpsc::Receiver<Message>>>;  // a thread safe receiver



// ************************************************
// Worker struct and implementations
// ************************************************

// Worker structure
pub struct Worker {
    pub id      : usize,                            // unique id representing the worker
    pub handle  : Option<thread::JoinHandle<()>>,   // thread handle for worker
    pub state   : WorkerState                       // indicates the current state of the worker
}

impl Worker {

    // create a new worker
    pub fn new(id: usize) -> Self {
        // allocate worker and return
        return Worker {
            id      : id,
            handle  : None,
            state   : WorkerState::Pending(),
        }
    }

    // start a worker
    pub fn run(&mut self, rx: &TSRX) {
        // clone threadsafe receiver
        let worker_id: usize = self.id;     // create copy of worker id for worker thread
        let worker_rx: TSRX = rx.clone();   // create copy of receiver for worker thread
        // spawning the new thread
        let handle: JoinHandle<_> = thread::spawn( move || {
            // allocate memory
            let mut result : Result<Message, mpsc::RecvError>;
            let mut message : Message;
            // run work loop
            loop {
                // get message from channel
                result = worker_rx.lock().unwrap().recv();
                // check if message was received successfully
                message = match result {
                    Ok(message) => message,                 // return message on success
                    Err(error) => panic!("{:?}", error),  // panic if receive fails  
                };
                // interpret the message
                match message {
                    Message::Dispatch(job) => {
                        println!("Running job on worker {:?}", worker_id);
                        job();
                    },
                    Message::Terminate() => {
                        println!("Terminating on worker {:?}", worker_id);
                        break
                    },
                }
            }
        });
        // updating worker status
        self.handle = Some(handle);
        self.state = WorkerState::Running();
    }

}



// ************************************************
// Pool struct and implementations
// ************************************************

pub struct Pool {
    workload_tx : TX,               // sender on which the pool distributes messages
    workload_rx : TSRX,             // receiver on which the workers receive messages
    workers     : Vec<Worker>       // contains worker informations
}

impl Pool {

    // create a new pool
    pub fn new(size: usize) -> Self {
        // allocate memory
        let workload_tx: TX;
        let workload_rx: RX;
        let mut workers: Vec<Worker> = Vec::<Worker>::with_capacity(size);
        // create channel
        (workload_tx, workload_rx) = mpsc::channel::<Message>();            // get non threadsafe channel sender(tx) and receiver(rx)
        let workload_tsrx: TSRX = Arc::new(Mutex::new(workload_rx));  // generate threadsafe receiver (tsrx) from non threadsafe receiver
        // spawn workers
        for id in 0..size {
            workers.push(Worker::new(id));
        }
        // return the pool
        return Self {
            workload_tx : workload_tx,
            workload_rx : workload_tsrx,
            workers     : workers,
        };
    }

    // spawn a new worker
    pub fn run(&mut self) {
        for worker in self.workers.iter_mut() {
            // clone threadsafe workload rx
            worker.run(&self.workload_rx);
        }
    }

    // eceute a job in pool
    pub fn execute<F>(&self, f:F) where F: FnOnce() + 'static + Send {
        let message: Message = Message::Dispatch(Box::new(f));
        self.workload_tx.send(message).unwrap();
    }

}

impl Drop for Pool {

    fn drop(&mut self) {
        for _ in 0..self.workers.len() {
            self.workload_tx.send(Message::Terminate()).unwrap();
        }

        for worker in self.workers.iter_mut() {
            if let Some(handle) = worker.handle.take() {
                handle.join().unwrap();
            }
        }
    }

}
