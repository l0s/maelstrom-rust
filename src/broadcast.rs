use serde_json::value::RawValue;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::{Excluded, Included};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::node::{AppError, Node};
use crate::protocol::{Message, MessageBody, MessageType};
use crate::server::{Module, RequestHandler, Response, Server};

pub mod node;
pub mod protocol;
pub mod server;

#[derive(Default)]
struct BroadcastServer {
    neighbours: Vec<String>,
    messages: HashSet<String>,
    acknowledged_broadcasts: HashSet<usize>,
}

struct TopologyHandler {
    broadcast_server: Arc<RwLock<BroadcastServer>>,
}

impl RequestHandler for TopologyHandler {
    fn handle_request(
        &self,
        node: &Node,
        request: &Message,
    ) -> Result<Box<dyn Response>, AppError> {
        if request.body.topology.is_none() {
            return Err(AppError::MissingField("body.topology".to_string()));
        }
        let topology = request.body.topology.clone().unwrap();
        let neighbours = topology
            .get(&node.node_id)
            .map(Vec::clone)
            .unwrap_or_default();
        let mut server = self
            .broadcast_server
            .write()
            .expect("Cannot update topology: broadcast server lock is poisoned");
        server.neighbours = neighbours;
        Ok(Box::new(TopologyOk {}))
    }
}

struct TopologyOk;

impl Response for TopologyOk {
    fn to_messages(&self, node: &Node, caller: &str, in_reply_to: usize) -> Vec<Message> {
        vec![Message {
            src: node.node_id.clone(),
            dest: caller.to_string(),
            body: MessageBody {
                message_type: MessageType::topology_ok,
                msg_id: Some(node.get_and_increment_message_id()),
                in_reply_to: Some(in_reply_to),
                node_id: None,
                node_ids: None,
                echo: None,
                code: None,
                text: None,
                topology: None,
                message: None,
                messages: None,
            },
        }]
    }
}

struct BroadcastHandler {
    broadcast_server: Arc<RwLock<BroadcastServer>>,
    response_sender: Arc<Mutex<Sender<Message>>>,
    pending_broadcasts: Arc<RwLock<BTreeMap<Instant, Vec<PendingBroadcast>>>>,
    running: Arc<AtomicBool>,
    daemon: Arc<Mutex<JoinHandle<()>>>,
}

const MAX_ATTEMPTS: u32 = 16;
const BASELINE_SLEEP_MS: u64 = 2;

impl Module for BroadcastHandler {
    fn init(&mut self, response_sender: Sender<Message>) {
        let mut guard = self.response_sender.lock().unwrap();
        *guard = response_sender.clone();

        // stop the existing daemon
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
        self.pending_broadcasts
            .write()
            .expect("Unable to reset pending broadcasts: lock poisoned")
            .clear();

        // start the new daemon
        let beginning = Instant::now();
        let pending_broadcasts = self.pending_broadcasts.clone();
        let broadcast_server = self.broadcast_server.clone();
        self.running
            .store(true, std::sync::atomic::Ordering::Release);
        let running = self.running.clone();
        let mut daemon_lock = self
            .daemon
            .lock()
            .expect("Unable init daemon: lock poisoned");
        *daemon_lock = thread::spawn(move || {
            while running.load(std::sync::atomic::Ordering::Acquire) {
                let mut keys_to_delete: Vec<Instant> = vec![];
                let mut pending: Vec<PendingBroadcast> = vec![];

                {
                    let broadcast_server = broadcast_server
                        .read()
                        .expect("Unable to read acknowledged messages: lock poisoned");
                    let pending_broadcasts = pending_broadcasts
                        .read()
                        .expect("Unable to read pending broadcasts: lock is poisoned");
                    for (instant, pending_broadcasts) in
                        pending_broadcasts.range((Excluded(beginning), Included(Instant::now())))
                    {
                        for pending_broadcast in pending_broadcasts {
                            let message_id = pending_broadcast
                                .broadcast
                                .body
                                .msg_id
                                .expect("Pending broadcast is missing a message ID");
                            if broadcast_server
                                .acknowledged_broadcasts
                                .contains(&message_id)
                            {
                                // message successfully delivered
                                continue;
                            } else if pending_broadcast.attempts > MAX_ATTEMPTS {
                                eprintln!(
                                    "Unable to deliver message after {} attempts: {}",
                                    MAX_ATTEMPTS, pending_broadcast.broadcast
                                );
                                continue;
                            }
                            // message not yet acknowledged, transmit
                            response_sender
                                .send(pending_broadcast.broadcast.clone())
                                .expect("Broadcast channel is closed.");

                            // wait for response
                            pending.push(PendingBroadcast {
                                broadcast: pending_broadcast.broadcast.clone(),
                                attempts: pending_broadcast.attempts + 1,
                            });
                        }
                        keys_to_delete.push(*instant);
                    }
                    // release locks on pending_broadcasts and broadcast_server
                }
                {
                    let mut pending_broadcasts = pending_broadcasts
                        .write()
                        .expect("Unable to write pending broadcasts: lock is poisoned");
                    for key_to_delete in keys_to_delete {
                        pending_broadcasts.remove(&key_to_delete);
                    }
                    for broadcast in pending {
                        // TODO add jitter
                        let sleep_time =
                            Duration::from_millis(2u64.pow(broadcast.attempts) * BASELINE_SLEEP_MS);
                        let broadcast = PendingBroadcast {
                            broadcast: broadcast.broadcast,
                            attempts: broadcast.attempts + 1,
                        };
                        let execution_time = Instant::now()
                            .checked_add(sleep_time)
                            .expect("Temporal overflow");
                        let execution_bucket =
                            pending_broadcasts.entry(execution_time).or_default();
                        execution_bucket.push(broadcast);
                    }
                    // release write lock on pending_broadcasts
                }
                sleep_until_ready(&pending_broadcasts);
            }
        });
    }

    fn handle_request(&self, response_sender: Sender<Message>, node: &Node, request: &Message) {
        let caller = &request.src;
        let in_reply_to = request
            .body
            .msg_id
            .expect("Broadcast message has no msg_id");
        if request.body.message.is_none() {
            let error = AppError::MissingField("body.message".to_string());
            let message = error.to_message(&node.node_id, caller, in_reply_to);
            response_sender.send(message).unwrap();
            return;
        }
        let acknowledgement = Message {
            src: node.node_id.clone(),
            dest: caller.to_string(),
            body: MessageBody {
                message_type: MessageType::broadcast_ok,
                msg_id: Some(node.get_and_increment_message_id()),
                in_reply_to: Some(in_reply_to),
                node_id: None,
                node_ids: None,
                echo: None,
                code: None,
                text: None,
                topology: None,
                message: None,
                messages: None,
            },
        };

        let message = request.body.message.clone().unwrap().to_string();
        {
            let mut server = self
                .broadcast_server
                .write()
                .expect("Cannot persist message: broadcast server lock is poisoned");
            if server.messages.contains(&message) {
                // already received this message by other means
                response_sender.send(acknowledgement).unwrap();
                return;
            }
            server.messages.insert(message.clone());
        }

        // gossip the message to the neighbours
        // except the neighbour that sent us the message to begin with
        let server = self
            .broadcast_server
            .read()
            .expect("Cannot find neighbours: broadcast server lock is poisoned");
        server
            .neighbours
            .iter()
            .filter(|neighbour| *neighbour != caller)
            .map(|neighbour| Broadcast {
                node: neighbour.to_string(),
                message: message.clone(),
            })
            .map(|broadcast| broadcast.to_message(node))
            .for_each(|message| self.gossip(message));

        // confirm receipt of the broadcast message
        response_sender.send(acknowledgement).unwrap();
    }
}

impl Drop for BroadcastHandler {
    fn drop(&mut self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
        {
            match self.pending_broadcasts.write() {
                Ok(mut guard) => guard.clear(),
                Err(e) => eprintln!(
                    "Unable to clear pending broadcasts: lock is poisoned: {}",
                    e
                ),
            }
        }
        match self.daemon.lock() {
            Ok(guard) => {
                guard.thread().unpark();
                // if let Err(e) = guard.join() {
                //     eprintln!("Unable to shut down daemon thread: {:?}", e);
                // }
            }
            Err(e) => eprintln!("Unable to stop daemon: lock poisoned: {}", e),
        }
    }
}

fn sleep_until_ready(pending_broadcasts: &Arc<RwLock<BTreeMap<Instant, Vec<PendingBroadcast>>>>) {
    let sleep_duration = pending_broadcasts
        .read()
        .expect("Unable to determine sleep time: pending broadcasts lock is poisoned")
        .keys()
        .take(1)
        .next()
        .map(|wakeup_time| wakeup_time.duration_since(Instant::now()));
    if let Some(sleep_duration) = sleep_duration {
        // park until it's time to send the first message
        thread::park_timeout(sleep_duration);
    } else {
        // park until a message is added
        thread::park();
    }
}

impl BroadcastHandler {
    fn gossip(&self, broadcast: Message) {
        // queue the message
        {
            let mut guard = self
                .pending_broadcasts
                .write()
                .expect("Unable to queue broadcast: lock is poisoned");
            guard
                .entry(Instant::now())
                .or_default()
                .push(PendingBroadcast {
                    broadcast,
                    attempts: 0,
                });
        }
        // wake the messenger daemon
        let guard = self
            .daemon
            .lock()
            .expect("Unable to wake messenger daemon: mutex is poisoned");
        guard.thread().unpark();
    }
}

struct Broadcast {
    node: String,
    message: String,
}

struct PendingBroadcast {
    broadcast: Message,
    attempts: u32,
}

impl Broadcast {
    fn to_message(&self, node: &Node) -> Message {
        Message {
            src: node.node_id.clone(),
            dest: self.node.to_string(),
            body: MessageBody {
                message_type: MessageType::broadcast,
                msg_id: Some(node.get_and_increment_message_id()),
                in_reply_to: None,
                node_id: None,
                node_ids: None,
                echo: None,
                code: None,
                text: None,
                topology: None,
                message: Some(
                    RawValue::from_string(self.message.clone())
                        .expect("Cannot convert back to JSON"),
                ),
                messages: None,
            },
        }
    }
}

struct ReadHandler {
    broadcast_server: Arc<RwLock<BroadcastServer>>, // FIXME use a more granular lock
}

impl RequestHandler for ReadHandler {
    fn handle_request(&self, _: &Node, _request: &Message) -> Result<Box<dyn Response>, AppError> {
        let server = self
            .broadcast_server
            .read()
            .expect("Cannot read messages: broadcast server lock is poisoned");
        Ok(Box::new(ReadOk {
            messages: server.messages.iter().cloned().collect(),
        }))
    }
}

struct ReadOk {
    messages: Vec<String>,
}

impl Response for ReadOk {
    fn to_messages(&self, node: &Node, caller: &str, in_reply_to: usize) -> Vec<Message> {
        vec![Message {
            src: node.node_id.clone(),
            dest: caller.to_string(),
            body: MessageBody {
                message_type: MessageType::read_ok,
                msg_id: Some(node.get_and_increment_message_id()),
                in_reply_to: Some(in_reply_to),
                node_id: None,
                node_ids: None,
                echo: None,
                code: None,
                text: None,
                topology: None,
                message: None,
                messages: Some(
                    self.messages
                        .iter()
                        .map(|string| {
                            RawValue::from_string(string.clone())
                                .expect("Cannot convert message back into JSON")
                        })
                        .collect(),
                ),
            },
        }]
    }
}

struct BroadcastAcknowledgementHandler {
    broadcast_server: Arc<RwLock<BroadcastServer>>,
}

impl Module for BroadcastAcknowledgementHandler {
    fn init(&mut self, _: Sender<Message>) {}

    fn handle_request(&self, _: Sender<Message>, _: &Node, request: &Message) {
        let mut guard = self
            .broadcast_server
            .write()
            .expect("Unable to process broadcast acknowledgement: broadcast_server lock poisoned");
        guard
            .acknowledged_broadcasts
            .insert(request.body.in_reply_to.unwrap());
        // .insert(request.body.msg_id.unwrap());
    }
}

fn main() {
    let broadcast_server = Arc::new(RwLock::new(BroadcastServer::default()));
    let topology_handler = TopologyHandler {
        broadcast_server: broadcast_server.clone(),
    };
    let (placeholder_sender, _receiver) = mpsc::channel::<Message>();
    let broadcast_handler = BroadcastHandler {
        broadcast_server: broadcast_server.clone(),
        response_sender: Arc::new(Mutex::new(placeholder_sender)),
        pending_broadcasts: Default::default(),
        running: Arc::new(AtomicBool::new(false)),
        daemon: Arc::new(Mutex::new(thread::spawn(|| {}))),
    };
    let read_handler = ReadHandler {
        broadcast_server: broadcast_server.clone(),
    };
    let broadcast_ok_handler = BroadcastAcknowledgementHandler { broadcast_server };

    let server = Server::builder()
        .with_handler(MessageType::topology, Box::new(topology_handler))
        .with_module(MessageType::broadcast, Box::new(broadcast_handler))
        .with_handler(MessageType::read, Box::new(read_handler))
        .with_module(MessageType::broadcast_ok, Box::new(broadcast_ok_handler))
        .build();
    server.run();
}
