use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::{io, thread};

use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::AppError::{AlreadyInitialised, MissingField};
use crate::{AppError, Message, MessageType, Node};

/// A Maelstrom [workload](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md)
/// handler.
pub trait RequestHandler: Sync + Send {
    /// Process the workload request.
    /// Parameters:
    /// - `node` - the node in the cluster on which the request is being processed
    /// - `request` - a message received from either a client or another cluster member
    /// Returns:
    /// - `Box<dyn Response>` - if the request was successfully processed
    /// - `AppError` - if the request could not be processed
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

/// The result of processing a workload request.
/// This may result in zero or one responses to the caller. This may also produce zero or more
/// messages to other cluster members.
pub trait Response {
    /// All the messages to send over the network in the order they should be sent. Note, there is
    /// no guarantee that the messages will be received in any specific order.
    ///
    /// Parameters:
    /// - `node` - the node that processed the request
    /// - `caller` - the node that initiated the request
    /// - `in_reply_to` - the request message ID, unique to `caller`
    /// Returns: the Maelstrom messages to send over the network in the order they should be sent
    fn to_messages(&self, node: &Node, caller: &str, in_reply_to: usize) -> Vec<Message>;
}

/// The main entity responsible for listening on the Maelstrom network and sending out messages. It
/// has a limited number of request handlers preinstalled. Client code should install application-
/// specific handlers. Once all handlers are installed, call `run()` to listen for requests.
pub struct Server {
    /// A pool on which actual requests will be processed
    pool: ThreadPool,
    /// The client-defined handlers for each message type
    handlers: Arc<HashMap<MessageType, Box<dyn RequestHandler>>>,
}

impl Server {
    /// Create a new `Server` with the provided handlers
    pub fn new(handlers: HashMap<MessageType, Box<dyn RequestHandler>>) -> Self {
        Self {
            pool: ThreadPoolBuilder::new()
                .build()
                .expect("Unable to create thread pool"),
            handlers: Arc::new(handlers),
        }
    }

    /// Start the server. This will essentially block until the application is terminated or it
    /// receives an indication that there will be no more network input.
    pub fn run(&self) {
        let (sender, receiver) = mpsc::channel::<String>(); // TODO consider making these members

        // responder thread that receives String messages and sends them over the network
        let responder = Self::spawn_responder(receiver);

        let mut node = Node {
            node_id: "Uninitialised Node".to_string(),
            next_message_id: Default::default(),
            node_ids: vec![],
        };

        // listen for initial input sequentially
        loop {
            let mut buffer = String::new();
            match io::stdin().read_line(&mut buffer) {
                Err(e) => {
                    eprintln!("Input Error, quitting: {}", e);
                    panic!();
                }
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        // EOF
                        eprintln!("EOF before initialisation, quitting");
                        break;
                    }
                    let request = match serde_json::from_str::<Message>(&buffer) {
                        Ok(message) => message,
                        Err(e) => {
                            // Note: we cannot respond with an `AppError` because we cannot
                            // know where to send the response if we couldn't parse the
                            // JSON.
                            eprintln!("Unable to parse input, not responding: {}", e);
                            continue;
                        }
                    };
                    if request.body.msg_id.is_none() {
                        // Note: we cannot respond with an `AppError` because we cannot
                        // reference the requesting message ID.
                        eprintln!(
                            "Unable to extract message ID, not responding: {:?}",
                            request
                        );
                        continue;
                    }
                    let request_id = request.body.msg_id.unwrap();

                    if request.body.message_type != MessageType::init {
                        eprintln!("Node is not initialised, dropping request: {:?}", request);
                        continue;
                    }

                    let result = if request.body.node_id.is_none() {
                        Some(MissingField(String::from("body.node_id")))
                    } else if request.body.node_ids.is_none() {
                        Some(MissingField(String::from("body.node_ids")))
                    } else {
                        None
                    };
                    node.node_id = request.body.node_id.unwrap();
                    node.node_ids = request.body.node_ids.unwrap();
                    let result = if let Some(err) = result {
                        Err(err)
                    } else {
                        Ok(vec![Message::init_ok(
                            &node.node_id,
                            &request.src,
                            node.get_and_increment_message_id(),
                            request_id,
                        )])
                    };
                    Self::send_response_result(
                        result,
                        sender.clone(),
                        &node.node_id,
                        &request.src,
                        request_id,
                    );
                    // once the node is initialised, the remaining inputs can be processed
                    // concurrently
                    break;
                }
            }
        }

        let node = Arc::new(node);

        self.pool.scope(move |scope| {
            // listen for remaining input
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
                        let node = node.clone();

                        scope.spawn(move |_| {
                            let request = match serde_json::from_str::<Message>(&buffer) {
                                Ok(message) => message,
                                Err(e) => {
                                    // Note: we cannot respond with an `AppError` because we cannot
                                    // know where to send the response if we couldn't parse the
                                    // JSON.
                                    eprintln!("Unable to parse input, not responding: {}", e);
                                    return;
                                }
                            };
                            if request.body.msg_id.is_none() {
                                // Note: we cannot respond with an `AppError` because we cannot
                                // reference the requesting message ID.
                                eprintln!(
                                    "Unable to extract message ID, not responding: {:?}",
                                    request
                                );
                                return;
                            }

                            let request_id = request.body.msg_id.unwrap();
                            let result = match request.body.message_type {
                                MessageType::init => Err(AlreadyInitialised),
                                message_type => self.run_custom_handler(
                                    &node,
                                    &request,
                                    request_id,
                                    &message_type,
                                ),
                            };

                            Self::send_response_result(
                                result,
                                sender,
                                &node.node_id,
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

    fn spawn_responder(receiver: Receiver<String>) -> JoinHandle<()> {
        thread::spawn(|| {
            let out = io::stdout();
            for response in receiver {
                let mut handle = out.lock();
                handle.write_all(response.as_bytes()).unwrap();
                handle.write_all("\n".as_bytes()).unwrap();
                handle.flush().unwrap();
            }
        })
    }

    /// Run the custom middleware installed by the client
    fn run_custom_handler(
        &self,
        node: &Node,
        request: &Message,
        request_id: usize,
        message_type: &MessageType,
    ) -> Result<Vec<Message>, AppError> {
        let handler = self.handlers.get(message_type);
        if let Some(handler) = handler {
            match handler.handle_request(node, request) {
                Ok(response) => Ok(response.to_messages(node, &request.src, request_id)),
                Err(e) => Err(e),
            }
        } else {
            Self::unimplemented(node, request_id, request)
        }
    }

    /// Indicate that the request specified a message type for which there is no handler installed
    fn unimplemented(
        node: &Node,
        request_id: usize,
        request: &Message,
    ) -> Result<Vec<Message>, AppError> {
        let response = Message::error(
            &node.node_id,
            &request.src,
            request_id,
            10,
            &format!("Not yet implemented: {:?}", request.body.message_type),
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

pub struct NoOpHandler;

impl RequestHandler for NoOpHandler {
    fn handle_request(
        &self,
        _node: &Node,
        _request: &Message,
    ) -> Result<Box<dyn Response>, AppError> {
        Ok(Box::new(NoOpResponse {}))
    }
}

struct NoOpResponse;

impl Response for NoOpResponse {
    fn to_messages(&self, _node: &Node, _caller: &str, _in_reply_to: usize) -> Vec<Message> {
        vec![]
    }
}
