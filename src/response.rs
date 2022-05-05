use crate::{Message, Node};

pub trait Response {
    fn to_messages(
        &self,
        node: &Node,
        caller: &str,
        msg_id: usize,
        in_reply_to: usize,
    ) -> Vec<Message>;
}
