use std::collections::HashMap;
use std::io::Write;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Condvar, Mutex, RwLock};
use std::{io, thread};

use threadpool::ThreadPool;

use AppError::{AlreadyInitialised, MissingField};

use crate::protocol::{Message, MessageType};

#[derive(Debug)]
pub enum AppError {
    MissingField(String),
    AlreadyInitialised,
}

#[derive(Clone)]
pub struct Node {
    node_id: Arc<RwLock<String>>,
    next_message_id: Arc<AtomicUsize>,
    init_sync: Arc<(Mutex<bool>, Condvar)>,

    // The other node IDs in the cluster
    // node_ids: Vec<String>, // TODO might need to wrap this in Arc
}

impl Node {
    fn init(&mut self, node_id: String) -> Result<(), AppError> {
        let &(ref lock, ref condition) = &*self.init_sync;
        let mut init_guard = lock.lock().unwrap();
        if *init_guard.deref() {
            return Err(AlreadyInitialised);
        }
        let mut target = self.node_id.write().unwrap();
        *target = node_id;
        *init_guard = true;
        condition.notify_all();

        Ok(())
    }

    pub fn get_and_increment_message_id(&mut self) -> usize {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn read_node_id(&self) -> String {
        let &(ref lock, ref condition) = &*self.init_sync;
        {
            // only hold the lock long enough to verify that the node is initialised
            let _initialised = condition
                .wait_while(lock.lock().unwrap(), |initialised| !*initialised)
                .unwrap();
        }
        self.node_id.read().unwrap().clone()
    }
}

impl Default for Node {
    fn default() -> Self {
        Self {
            node_id: Arc::new(RwLock::new(String::from("Uninitialised Node"))),
            next_message_id: Arc::new(AtomicUsize::new(0)),
            init_sync: Arc::new((Mutex::new(false), Condvar::new())),
            // node_ids: Vec::default(),
        }
    }
}

type RequestHandler =
    &'static (dyn Fn(&mut Node, usize, &Message) -> Result<Message, AppError> + Sync);

#[derive(Default)]
pub struct Server {
    node: Node,
    pool: ThreadPool,
    handlers: HashMap<MessageType, RequestHandler>,
}

impl Server {
    pub fn run(&mut self) {
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

        // listen for input
        loop {
            let handlers = self.handlers.clone();
            let mut buffer = String::new();
            match io::stdin().read_line(&mut buffer) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        // EOF
                        break;
                    }

                    // process each input entry on a worker thread
                    let sender = sender.clone();
                    let mut state = self.node.clone();

                    self.pool.execute(move || {
                        let request = match serde_json::from_str::<Message>(&buffer) {
                            Ok(message) => message,
                            Err(e) => {
                                eprintln!("Unable to parse input, not responding: {}", e);
                                return;
                            }
                        };

                        if request.body.msg_id.is_none() {
                            eprintln!(
                                "Unable to extract message ID, not responding: {:?}",
                                request
                            );
                            return;
                        }

                        let request_id = request.body.msg_id.unwrap();
                        let node_id = match state.node_id.try_read() {
                            // this might read an invalid node_id if the node is still initialising
                            // but this is only used for sending error messages
                            Ok(node_id) => node_id.to_string(),
                            Err(_) => "Uninitialised node".to_string(),
                        };

                        let result = match request.body.message_type {
                            MessageType::init => {
                                // TODO should this fail more gracefully?
                                match state.init(request.body.node_id.unwrap()) {
                                    Ok(_ignore) => Ok(Message::init_ok(
                                        &state.read_node_id(),
                                        &request.src,
                                        state.next_message_id.fetch_add(1, Relaxed),
                                        request_id,
                                    )),
                                    Err(e) => Err(e),
                                }
                            }
                            message_type => {
                                let handler = handlers.get(&message_type);
                                if let Some(handler) = handler {
                                    (*handler)(&mut state, request_id, &request)
                                } else {
                                    Self::unimplemented(&state, request_id, &request)
                                }
                            }
                        };
                        Self::send_result(result, sender, &node_id, &request.src, request_id);
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

    pub fn register_handler(&mut self, message_type: MessageType, handler: RequestHandler) {
        self.handlers.insert(message_type, handler);
    }

    fn unimplemented(
        node: &Node,
        request_id: usize,
        request: &Message,
    ) -> Result<Message, AppError> {
        let node_id = node.read_node_id();
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
        match result {
            Ok(response) => Self::send_message(response, sender),
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
                Self::send_message(message, sender);
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
                                    message.body.in_reply_to.unwrap()))
                    .unwrap();
            }
        }
    }
}
