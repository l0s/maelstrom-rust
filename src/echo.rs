extern crate serde;
extern crate serde_with;

use crate::node::{AppError, Node, Server};
use crate::protocol::Message;
use crate::protocol::MessageType;
use crate::AppError::MissingField;

pub mod node;
pub mod protocol;

fn main() {
    let mut server = Server::default();
    server.register_handler(MessageType::echo, &echo);
    server.run();
}

fn echo(state: &mut Node, request_id: usize, request: &Message) -> Result<Message, AppError> {
    if request.body.echo.is_none() {
        return Err(MissingField(String::from("body.echo")));
    }

    let response = Message::echo(
        &state.read_node_id(),
        &request.src,
        state.get_and_increment_message_id(),
        request_id,
        &request.body.echo.clone().unwrap(),
    );
    Ok(response)
}
