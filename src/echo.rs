extern crate serde;
extern crate serde_with;

use std::{io, thread};
use std::io::Write;
use std::ops::Deref;
use std::sync::{Arc, Condvar, mpsc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;

use crate::AppError::{AlreadyInitialised, MissingField};
use crate::lib::Message;
use crate::lib::MessageType;

mod lib;

#[derive(Clone)]
struct State {
    node_id: Arc<RwLock<String>>,
    // TODO can we use a `Once`?
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

#[derive(Debug)]
enum AppError {
    MissingField(String),
    AlreadyInitialised,
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
                // TODO limit the number of threads using a semaphore, thread pool, or something else
                thread::spawn(move || {
                    eprintln!("Received: {}", buffer);
                    let request = match serde_json::from_str::<Message>(&buffer) {
                        Ok(message) => message,
                        Err(e) => {
                            eprintln!("Unable to parse input, not responding: {}", e);
                            return;
                        }
                    };

                    if request.body.msg_id.is_none() {
                        eprintln!("Unable to extract message ID, not responding: {:?}", request);
                        return;
                    }
                    let request_id = request.body.msg_id.unwrap();
                    let node_id = match state.node_id.try_read() {
                        Ok(node_id) => node_id.to_string(),
                        Err(_) => "Uninitialised node".to_string(),
                    };

                    let result = match request.body.message_type {
                        MessageType::init => init(state, request_id, &request),
                        MessageType::echo => echo(&state, request_id, &request),
                        _ => unimplemented(&state, request_id, &request),
                    };

                    send_result(result, sender, &node_id, &request.src, request_id);
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

fn init(state: State, request_id: usize, request: &Message) -> Result<Message, AppError> {
    // set the node ID then unblock all other threads that were waiting for it
    let &(ref lock, ref condition) = &*state.init_sync;
    let mut init_guard = lock.lock().unwrap();
    if *init_guard.deref() {
        return Err(AlreadyInitialised);
    }
    let mut node_id = state.node_id.write().unwrap();
    if request.body.node_id.is_none() {
        return Err(MissingField(String::from("body.node_id")));
    }
    *node_id = request.body.node_id.clone().unwrap();
    *init_guard = true;
    condition.notify_all();

    let message_id = state.next_message_id.fetch_add(1, Ordering::Relaxed);
    let response = Message::init_ok(&node_id, &request.src, message_id, request_id);
    Ok(response)
}

fn echo(state: &State, request_id: usize, request: &Message) -> Result<Message, AppError> {
    let &(ref lock, ref condition) = &*state.init_sync;
    {
        // only hold the lock long enough to verify that the node is initialised
        let _initialised = condition
            .wait_while(lock.lock().unwrap(), |initialised| !*initialised)
            .unwrap();
    }

    if request.body.echo.is_none() {
        return Err(MissingField(String::from("body.echo")));
    }

    let node_id = state.node_id.read().unwrap();
    let message_id = state.next_message_id.fetch_add(1, Ordering::Relaxed);
    let response = Message::echo(
        &node_id,
        &request.src,
        message_id,
        request_id,
        &request.body.echo.clone().unwrap(),
    );
    Ok(response)
}

fn unimplemented(state: &State, request_id: usize, request: &Message) -> Result<Message, AppError> {
    let node_id = state.node_id.read().unwrap();
    let response = Message::error(
        &node_id,
        &request.src,
        request_id,
        10,
        "Not yet implemented",
    );
    Ok(response)
}

fn send_result(
    result: Result<Message, AppError>,
    sender: Sender<String>,
    node_id: &str,
    destination: &str,
    in_reply_to: usize,
) {
    eprintln!("Responding with: {:?}", result);
    match result {
        Ok(response) => send_message(response, sender),
        Err(e) => {
            eprintln!("Application error: {:?}", e);
            let message = match e {
                MissingField(field) => Message::error(
                    node_id,
                    destination,
                    in_reply_to,
                    12,
                    format!("Missing field: {}", field).as_str(),
                ),
                AlreadyInitialised => Message::error(
                    node_id,
                    destination,
                    in_reply_to,
                    22,
                    "Node was already initialised",
                ),
            };
            send_message(message, sender);
        }
    }
}

fn send_message(message: Message, sender: Sender<String>) {
    let response = serde_json::to_string(&message);
    match response {
        Ok(response) => sender.send(response).unwrap(),
        Err(e) => {
            // construct JSON manually to avoid further serialisation issues
            // `message.body.in_reply_to` should not be `None` but the reason why is not obvious
            eprintln!("Error serialising response: {}", e);
            sender.send(format!("{{\"src\":\"{}\",\"dest\":\"{}\",\"body\":{{\"type\":\"error\",\"in_reply_to\":{},\"code\":13,\"text\":\"Unable to serialise response\"}}}}",
                                message.src,
                                message.dest,
                                message.body.in_reply_to.unwrap())
                .to_string())
                .unwrap();
        }
    }
}
