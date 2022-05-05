use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc};
use std::{io, thread};

use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::response::Response;
use crate::AppError::{AlreadyInitialised, MissingField};
use crate::{AppError, Message, MessageType, Node};

pub trait RequestHandler: Sync + Send {
    fn handle_request(&self, node: &Node, request: &Message)
        -> Result<Box<dyn Response>, AppError>;
}

impl<F, R> RequestHandler for F
where
    F: Fn(&Node, &Message) -> Result<R, AppError> + Sync + Send,
    R: Response + 'static,
{
    fn handle_request(
        &self,
        node: &Node,
        request: &Message,
    ) -> Result<Box<dyn Response>, AppError> {
        let response = self(node, request)?;
        Ok(Box::new(response))
    }
}

pub struct Server {
    node: Node,
    pool: ThreadPool,
    handlers: Arc<HashMap<MessageType, Box<dyn RequestHandler>>>,
}

impl Server {
    pub fn new(handlers: HashMap<MessageType, Box<dyn RequestHandler>>) -> Self {
        Self {
            node: Default::default(),
            pool: ThreadPoolBuilder::new()
                .build()
                .expect("Unable to create thread pool"),
            handlers: Arc::new(handlers),
        }
    }

    pub fn run(&self) {
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

        self.pool.scope(move |scope| {
            // listen for input
            loop {
                let mut buffer = String::new();
                match io::stdin().read_line(&mut buffer) {
                    Err(e) => {
                        eprintln!("Input Error: {}", e);
                        panic!();
                    }
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            // EOF
                            break;
                        }

                        // process each input entry on a worker thread
                        let sender = sender.clone();
                        let mut node = self.node.clone();

                        scope.spawn(move |_| {
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

                            let result = match request.body.message_type {
                                MessageType::init => {
                                    if request.body.node_id.is_none() {
                                        Err(AppError::MissingField(String::from("body.node_id")))
                                    } else if request.body.node_ids.is_none() {
                                        Err(AppError::MissingField(String::from("body.node_ids")))
                                    } else {
                                        match node.init(
                                            request.body.node_id.unwrap(),
                                            request.body.node_ids.unwrap(),
                                        ) {
                                            Ok(_) => Ok(vec![Message::init_ok(
                                                &node.read_node_id(),
                                                &request.src,
                                                node.next_message_id.fetch_add(1, Relaxed),
                                                request_id,
                                            )]),
                                            Err(e) => Err(e),
                                        }
                                    }
                                }
                                message_type => {
                                    let handler = self.handlers.get(&message_type);
                                    if let Some(handler) = handler {
                                        match handler.handle_request(&node, &request) {
                                            Ok(response) => Ok(response.to_messages(
                                                &node,
                                                &request.src,
                                                (&node).get_and_increment_message_id(),
                                                request_id,
                                            )),
                                            Err(e) => Err(e),
                                        }
                                    } else {
                                        Self::unimplemented(&node, request_id, &request)
                                    }
                                }
                            };
                            let node_id = match node.node_id.try_read() {
                                // this might read an invalid node_id if the node is still initialising
                                // but this is only used for sending error messages
                                Ok(node_id) => node_id.to_string(),
                                Err(_) => "Uninitialised node".to_string(),
                            };

                            Self::send_response_result(
                                result,
                                sender,
                                &node_id,
                                &request.src,
                                request_id,
                            );
                        });
                    }
                }
            }
        });
        // All inputs have been received
        // Wait for all pending responses to be sent
        responder.join().unwrap();
    }

    fn unimplemented(
        node: &Node,
        request_id: usize,
        request: &Message,
    ) -> Result<Vec<Message>, AppError> {
        let node_id = node.read_node_id();
        let response = Message::error(
            &node_id,
            &request.src,
            request_id,
            10,
            "Not yet implemented",
        );
        Ok(vec![response])
    }

    fn send_response_result(
        result: Result<Vec<Message>, AppError>,
        sender: Sender<String>,
        node_id: &str,
        destination: &str,
        in_reply_to: usize,
    ) {
        match result {
            Ok(messages) => {
                for message in messages {
                    Self::send_response_message(message, &sender)
                }
            }
            Err(e) => {
                eprintln!("Application error: {:?}", e);
                let message = match e {
                    MissingField(ref field) => Message::error(
                        node_id,
                        destination,
                        in_reply_to,
                        e.code(),
                        format!("Missing field: {}", field).as_str(),
                    ),
                    AlreadyInitialised => Message::error(
                        node_id,
                        destination,
                        in_reply_to,
                        e.code(),
                        "Node was already initialised",
                    ),
                };
                Self::send_response_message(message, &sender);
            }
        }
    }

    fn send_response_message(message: Message, sender: &Sender<String>) {
        debug_assert!(message.body.in_reply_to.is_some());
        let response = serde_json::to_string(&message);
        match response {
            Ok(response) => sender.send(response).unwrap(),
            Err(e) => {
                // Construct JSON manually to avoid further serialisation issues.
                // `message.body.in_reply_to` should not be `None` because a valid `Message` was
                // generated.
                // We send back a "crash", code 13, which is also described as "internal-error". It
                // is likely that future serialisation attempts will also fail.
                eprintln!("Error serialising response: {}", e);
                sender.send(format!("{{\"src\":\"{}\",\"dest\":\"{}\",\"body\":{{\"type\":\"error\",\"in_reply_to\":{},\"code\":13,\"text\":\"Unable to serialise response\"}}}}",
                                    message.src,
                                    message.dest,
                                    message.body.in_reply_to.expect("A valid response should have already been generated.")))
                    .unwrap();
            }
        }
    }
}
