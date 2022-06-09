use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::{io, thread};

use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::AppError::{AlreadyInitialised, MissingField};
use crate::{AppError, Message, MessageType, Node};

pub trait Module: Sync + Send {
    fn init(&self, response_sender: Sender<Message>);
    fn handle_request(&self, response_sender: Sender<Message>, node: &Node, request: &Message);
}

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

struct RequestHandlerModule {
    delegate: Box<dyn RequestHandler>,
}

impl Module for RequestHandlerModule {
    fn init(&self, _response_sender: Sender<Message>) {
    }

    fn handle_request(&self, response_sender: Sender<Message>, node: &Node, request: &Message) {
        let result = self.delegate.handle_request(node, request);
        let messages = match result {
            Ok(response) => response.to_messages(node, &request.src, request.body.msg_id.unwrap()),
            Err(error) => vec![error.to_message(&node.node_id, &request.src, request.body.msg_id.unwrap())],
        };
        for message in messages {
            response_sender.send(message).unwrap();
        }
    }
}

impl From<Box<dyn RequestHandler>> for RequestHandlerModule {
    fn from(delegate: Box<dyn RequestHandler>) -> Self {
        Self { delegate }
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
    handlers: Arc<HashMap<MessageType, Box<dyn Module>>>,
    response_sender: Sender<Message>,
    response_receiver: Arc<Mutex<Receiver<Message>>>,
}

#[derive(Default)]
pub struct ServerBuilder {
    handlers: HashMap<MessageType, Box<dyn Module>>,
    thread_pool_builder: ThreadPoolBuilder,
}

impl ServerBuilder {
    pub fn build(self) -> Server {
        let (response_sender, response_receiver) = mpsc::channel();
        for handler in self.handlers.values() {
            handler.init(response_sender.clone());
        }
        let pool = self.thread_pool_builder
            .build()
            .expect("Unable to create thread pool");
        let handlers = Arc::new(self.handlers);
        Server {
            pool,
            handlers,
            response_sender,
            response_receiver: Arc::new(Mutex::new(response_receiver)),
        }
    }

    pub fn with_handler(self, message_type: MessageType, handler: Box<dyn RequestHandler>) -> Self {
        let module = RequestHandlerModule::from(handler);
        self.with_module(message_type, Box::new(module))
    }

    pub fn with_module(mut self, message_type: MessageType, module: Box<dyn Module>) -> Self {
        self.handlers.insert(message_type, module);
        self
    }

    // TODO make ThreadPool configurable
}

impl Server {

    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Start the server. This will essentially block until the application is terminated or it
    /// receives an indication that there will be no more network input.
    pub fn run(&self) {
        let (raw_sender, raw_receiver) = mpsc::channel::<String>();

        // responder thread that receives String messages and sends them over the network
        let responder = Self::spawn_responder(raw_receiver);
        // TODO consider inlining these two Receivers
        let receiver_guard = self.response_receiver.clone();
        thread::spawn(move || {
            for message in receiver_guard.lock().unwrap().iter() {
                let response = serde_json::to_string(&message);
                match response {
                    Ok(response) => raw_sender.send(response).unwrap(),
                    Err(e) => {
                        // Construct JSON manually to avoid further serialisation issues.
                        // `message.body.in_reply_to` should be `Some` because a valid `Message` was
                        // generated.
                        // We send back a "crash", code 13, which is also described as "internal-error". It
                        // is likely that future serialisation attempts will also fail.
                        eprintln!("Error serialising response: {}", e);
                        raw_sender.send(format!("{{\"src\":\"{}\",\"dest\":\"{}\",\"body\":{{\"type\":\"error\",\"in_reply_to\":{},\"code\":13,\"text\":\"Unable to serialise response\"}}}}",
                                            message.src,
                                            message.dest,
                                            message.body.in_reply_to.expect("A valid response should have already been generated.")))
                            .unwrap();
                    }
                }
            }
        });

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
                    let response_message =
                    if let Some(err) = result {
                        err.to_message(&node.node_id, &request.src, request_id)
                    } else {
                        Message::init_ok(
                            &node.node_id,
                            &request.src,
                            node.get_and_increment_message_id(),
                            request_id,
                        )
                    };
                    self.response_sender.send(response_message).unwrap();
                    // once the node is initialised, the remaining inputs can be processed
                    // concurrently
                    break;
                }
            }
        }

        let node = Arc::new(node);

        let message_sender = self.response_sender.clone();
        let handlers = self.handlers.clone();
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
                        let node = node.clone();
                        let message_sender = message_sender.clone();
                        let handlers = handlers.clone();

                        scope.spawn(move |_| Self::process_line(message_sender, handlers, &mut buffer, &node));
                    }
                }
            }
        });
        // All inputs have been received
        // Wait for all pending responses to be sent
        responder.join().unwrap();
    }

    fn process_line(sender: Sender<Message>, handlers: Arc<HashMap<MessageType, Box<dyn Module>>>, buffer: &mut str, node: &Arc<Node>) {
        let request = match serde_json::from_str::<Message>(buffer) {
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
        if request.body.message_type == MessageType::init {
            sender.send(AlreadyInitialised.to_message(&node.node_id, &request.src, request_id)).unwrap();
        }
        Self::run_custom_handler(
            sender,
            handlers,
            node,
            &request,
            request_id,
            &request.body.message_type,
        );
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
        sender: Sender<Message>,
        handlers: Arc<HashMap<MessageType, Box<dyn Module>>>,
        node: &Node,
        request: &Message,
        request_id: usize,
        message_type: &MessageType,
    ) {
        let handler = handlers.get(message_type);
        if let Some(handler) = handler {
            handler.handle_request(sender, node, request);
        } else {
            let response = Message::error(
                &node.node_id,
                &request.src,
                request_id,
                10,
                &format!("Not yet implemented: {:?}", request.body.message_type),
            );
            sender.send(response).unwrap();
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
