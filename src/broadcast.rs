use std::collections::HashMap;
use std::sync::Arc;

use crate::node::{AppError, Node};
use crate::protocol::{Message, MessageType};
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
    broadcast_server: Arc<BroadcastServer>,
}

impl RequestHandler for TopologyHandler {
    fn handle_request(
        &self,
        _node: &Node,
        _in_reply_to: usize,
        _request: &Message,
    ) -> Result<Box<dyn Response>, AppError> {
        todo!()
    }
}

fn main() {
    let mut broadcast_server = BroadcastServer::default();
    let mut topology_handler = TopologyHandler {
        broadcast_server: Arc::new(broadcast_server),
    };

    let mut handlers: HashMap<MessageType, Box<dyn RequestHandler>> = HashMap::new();
    handlers.insert(MessageType::topology, Box::new(topology_handler));

    let mut server = Server::new(handlers);
    server.run();
}
