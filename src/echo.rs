extern crate serde;
extern crate serde_with;

use std::collections::HashMap;

use crate::node::{AppError, Node};
use crate::protocol::Message;
use crate::protocol::MessageType;
use crate::response::EchoResponse;
use crate::server::{RequestHandler, Server};
use crate::AppError::MissingField;

pub mod node;
pub mod protocol;
pub mod response;
pub mod server;

fn main() {
    let mut handlers: HashMap<MessageType, Box<dyn RequestHandler>> = Default::default();
    handlers.insert(MessageType::echo, Box::new(echo));
    let server = Server::new(handlers);

    server.run();
}

fn echo(_node: &Node, _request_id: usize, request: &Message) -> Result<EchoResponse, AppError> {
    if request.body.echo.is_none() {
        return Err(MissingField(String::from("body.echo")));
    }

    Ok(EchoResponse {
        text: request.body.echo.clone().unwrap(),
    })
}
