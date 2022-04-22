extern crate serde;
extern crate serde_with;

use std::io::Write;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, RwLock};
use std::{io, thread};

use crate::lib::Message;
use crate::lib::MessageType;

mod lib;

#[derive(Clone)]
struct State {
    node_id: Arc<RwLock<String>>, // TODO can we use a `Once`?
    next_message_id: Arc<AtomicUsize>,
    init_sync: Arc<(Mutex<bool>, Condvar)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            node_id: Arc::new(RwLock::new(String::from("Uninitialised Node"))),
            next_message_id: Arc::new(AtomicUsize::new(0)),
            init_sync: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }
}

fn main() {
    // Application State
    let state = State::default();

    let (sender, receiver) = mpsc::channel::<String>();
    // responder thread that receives String messages and sends them over the network
    let responder = thread::spawn(|| {
        let out = io::stdout();
        for response in receiver {
            let mut handle = out.lock();
            handle.write_all(response.as_bytes()).unwrap();
            handle.write_all("\n".as_bytes()).unwrap();
            handle.flush().unwrap();
        }
    });

    loop {
        let mut buffer = String::new();
        match io::stdin().read_line(&mut buffer) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    break;
                }
                let sender = sender.clone();
                let state = state.clone();
                thread::spawn(move || {
                    // TODO limit the number of threads using a semaphore, thread pool, or something else
                    let request = serde_json::from_str::<Message>(&buffer).unwrap();
                    eprintln!("Received: {:?}", request);

                    let request_id = request.body.msg_id.unwrap(); // TODO send error

                    match request.body.message_type {
                        MessageType::init => {
                            // set the node ID then unblock all other threads that were waiting for it
                            let &(ref lock, ref condition) = &*state.init_sync;
                            let mut init_guard = lock.lock().unwrap();
                            // TODO fail more gracefully by sending an error over the wire
                            assert!(!init_guard.deref(), "Node was already initialised.");
                            let mut node_id = state.node_id.write().unwrap();
                            *node_id = request.body.node_id.unwrap(); // TODO send error
                            *init_guard = true;
                            condition.notify_all();

                            let message_id = state.next_message_id.fetch_add(1, Ordering::Relaxed);
                            let response =
                                Message::init_ok(&node_id, &request.src, message_id, request_id);
                            eprintln!("Response: {:?}", response);
                            let response = serde_json::to_string(&response).unwrap();
                            sender.send(response).unwrap();
                            eprintln!("Initialized node {}", node_id);
                        }
                        MessageType::echo => {
                            eprintln!("-- processing echo request");
                            let &(ref lock, ref condition) = &*state.init_sync;
                            let _initialised = condition
                                .wait_while(lock.lock().unwrap(), |initialised| !*initialised)
                                .unwrap();

                            let node_id = state.node_id.read().unwrap();
                            let message_id = state.next_message_id.fetch_add(1, Ordering::Relaxed);
                            let response = Message::echo(
                                &node_id,
                                &request.src,
                                message_id,
                                request_id,
                                &request.body.echo.unwrap(),
                            );
                            eprintln!("Echoing: {:?}", response.body);
                            let response = serde_json::to_string(&response).unwrap();
                            sender.send(response).unwrap();
                        }
                        _ => {
                            let node_id = state.node_id.read().unwrap();
                            let response = Message::error(
                                &node_id,
                                &request.src,
                                request_id,
                                10,
                                "Not yet implemented",
                            );
                            eprintln!("Unhandled message: {:?}", response);
                            let response = serde_json::to_string(&response).unwrap();
                            sender.send(response).unwrap();
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Input Error: {}", e);
                panic!();
            }
        }
    }
    // All inputs have been received
    // Wait for all pending responses to be sent
    responder.join().unwrap();
}
