extern crate serde;
extern crate serde_with;

use std::collections::HashMap;

use crate::node::{AppError, Node};
use crate::protocol::MessageType;
use crate::protocol::{Message, MessageBody};
use crate::response::Response;
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

pub struct EchoResponse {
    pub text: String,
}

impl Response for EchoResponse {
    fn to_message(&self, node: &Node, caller: &str, msg_id: usize, in_reply_to: usize) -> Message {
        Message {
            src: node.read_node_id(),
            dest: caller.to_owned(),
            body: MessageBody {
                message_type: MessageType::echo_ok,
                msg_id: Some(msg_id),
                in_reply_to: Some(in_reply_to),
                node_id: None,
                node_ids: None,
                echo: Some(self.text.clone()),
                code: None,
                text: None,
                topology: None,
            },
        }
    }
}
