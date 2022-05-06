use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::node::{AppError, Node};
use crate::protocol::{Message, MessageBody, MessageType};
use crate::server::{NoOpHandler, RequestHandler, Response, Server};

pub mod node;
pub mod protocol;
pub mod server;

#[derive(Default)]
struct BroadcastServer {
    neighbours: Vec<String>,
    messages: HashSet<String>,
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
            .get(&node.read_node_id())
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
            src: node.read_node_id(),
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
    broadcast_server: Arc<RwLock<BroadcastServer>>, // FIXME use a more granular lock
}

impl RequestHandler for BroadcastHandler {
    fn handle_request(
        &self,
        _node: &Node,
        request: &Message,
    ) -> Result<Box<dyn Response>, AppError> {
        if request.body.message.is_none() {
            return Err(AppError::MissingField("body.message".to_string()));
        }
        let message = request.body.message.clone().unwrap().to_string();
        {
            let mut server = self
                .broadcast_server
                .write()
                .expect("Cannot persist message: broadcast server lock is poisoned");
            if server.messages.contains(&message) {
                return Ok(Box::new(BroadcastOk { gossip: vec![] }));
            }
            server.messages.insert(message.clone());
        }

        // gossip the message to the neighbours
        let server = self
            .broadcast_server
            .read()
            .expect("Cannot find neighbours: broadcast server lock is poisoned");
        Ok(Box::new(BroadcastOk {
            gossip: server
                .neighbours
                .iter()
                .map(|neighbour| Broadcast {
                    node: neighbour.to_string(),
                    message: message.clone(),
                })
                .collect(),
        }))
    }
}

struct BroadcastOk {
    /// Additional broadcast messages to propagate prior to acknowledging the original request
    gossip: Vec<Broadcast>,
}

impl Response for BroadcastOk {
    fn to_messages(&self, node: &Node, caller: &str, in_reply_to: usize) -> Vec<Message> {
        // send the gossip messages
        let mut result: Vec<Message> = self
            .gossip
            .iter()
            .map(|broadcast| broadcast.to_message(node, node.get_and_increment_message_id()))
            .collect();
        // finally, acknowledge the broadcast request
        result.push(Message {
            src: node.read_node_id(),
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
        });
        result
    }
}

struct Broadcast {
    node: String,
    message: String,
}

impl Broadcast {
    fn to_message(&self, node: &Node, msg_id: usize) -> Message {
        Message {
            src: node.read_node_id(),
            dest: self.node.to_string(),
            body: MessageBody {
                message_type: MessageType::broadcast,
                msg_id: Some(msg_id),
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
    fn handle_request(
        &self,
        _node: &Node,
        _request: &Message,
    ) -> Result<Box<dyn Response>, AppError> {
        let server = self
            .broadcast_server
            .read()
            .expect("broadcast server lock is poisoned");
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
            src: node.read_node_id(),
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

fn main() {
    let broadcast_server = Arc::new(RwLock::new(BroadcastServer::default()));
    let topology_handler = TopologyHandler {
        broadcast_server: broadcast_server.clone(),
    };
    let broadcast_handler = BroadcastHandler {
        broadcast_server: broadcast_server.clone(),
    };
    let read_handler = ReadHandler { broadcast_server };

    let mut handlers: HashMap<MessageType, Box<dyn RequestHandler>> = HashMap::new();
    handlers.insert(MessageType::topology, Box::new(topology_handler));
    handlers.insert(MessageType::broadcast, Box::new(broadcast_handler));
    handlers.insert(MessageType::read, Box::new(read_handler));
    handlers.insert(MessageType::broadcast_ok, Box::new(NoOpHandler {}));

    let server = Server::new(handlers);
    server.run();
}
