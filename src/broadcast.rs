use std::collections::HashMap;
use std::sync::RwLock;

use crate::node::{AppError, Node};
use crate::protocol::{Message, MessageBody, MessageType};
use crate::response::Response;
use crate::server::{RequestHandler, Server};

pub mod node;
pub mod protocol;
pub mod response;
pub mod server;

#[derive(Default)]
struct BroadcastServer {
    neighbours: Vec<String>,
}

struct TopologyHandler {
    broadcast_server: RwLock<BroadcastServer>,
}

impl RequestHandler for TopologyHandler {
    fn handle_request(
        &self,
        node: &Node,
        _in_reply_to: usize,
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
            .expect("broadcast server lock is poisoned");
        server.neighbours = neighbours;
        Ok(Box::new(TopologyOk {}))
    }
}

struct TopologyOk;

impl Response for TopologyOk {
    fn to_message(&self, node: &Node, caller: &str, msg_id: usize, in_reply_to: usize) -> Message {
        Message {
            src: node.read_node_id(),
            dest: caller.to_string(),
            body: MessageBody {
                message_type: MessageType::topology_ok,
                msg_id: Some(msg_id),
                in_reply_to: Some(in_reply_to),
                node_id: None,
                node_ids: None,
                echo: None,
                code: None,
                text: None,
                topology: None,
            },
        }
    }
}

fn main() {
    let broadcast_server = BroadcastServer::default();
    let topology_handler = TopologyHandler {
        broadcast_server: RwLock::new(broadcast_server),
    };

    let mut handlers: HashMap<MessageType, Box<dyn RequestHandler>> = HashMap::new();
    handlers.insert(MessageType::topology, Box::new(topology_handler));

    let server = Server::new(handlers);
    server.run();
}
