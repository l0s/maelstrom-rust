use crate::protocol::MessageBody;
use crate::{Message, MessageType, Node};

pub trait Response {
    fn to_message(&self, node: &Node, caller: &str, msg_id: usize, in_reply_to: usize) -> Message;
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
