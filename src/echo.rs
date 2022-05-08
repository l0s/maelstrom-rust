extern crate serde;
extern crate serde_with;

use std::collections::HashMap;

use crate::node::{AppError, Node};
use crate::protocol::MessageType;
use crate::protocol::{Message, MessageBody};
use crate::server::{RequestHandler, Response, Server};
use crate::AppError::MissingField;

pub mod node;
pub mod protocol;
pub mod server;

fn main() {
    let mut handlers: HashMap<MessageType, Box<dyn RequestHandler>> = Default::default();
    handlers.insert(MessageType::echo, Box::new(echo));
    let server = Server::new(handlers);

    server.run();
}

fn echo(_node: &Node, request: &Message) -> Result<EchoResponse, AppError> {
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
    fn to_messages(&self, node: &Node, caller: &str, in_reply_to: usize) -> Vec<Message> {
        vec![Message {
            src: node.node_id.clone(),
            dest: caller.to_owned(),
            body: MessageBody {
                message_type: MessageType::echo_ok,
                msg_id: Some(node.get_and_increment_message_id()),
                in_reply_to: Some(in_reply_to),
                node_id: None,
                node_ids: None,
                echo: Some(self.text.clone()),
                code: None,
                text: None,
                topology: None,
                message: None,
                messages: None,
            },
        }]
    }
}
