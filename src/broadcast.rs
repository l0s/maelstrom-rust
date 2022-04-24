pub mod node;
pub mod protocol;

use crate::node::{AppError, Node, Server};
use crate::protocol::{Message, MessageType};

fn main() {
    let mut server = Server::default();
    server.register_handler(MessageType::echo, &broadcast);
    server.register_handler(MessageType::echo_ok, &broadcast);
    // TODO add handlers
    server.run();
}

fn broadcast(_node: &mut Node, _in_reply_to: usize, _request: &Message) -> Result<Message, AppError> {
    todo!()
}
