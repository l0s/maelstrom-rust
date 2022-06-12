use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_with::skip_serializing_none;

/// A Maelstrom message, which can be either an input to or output of the application.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Message {
    /// The sender of the message
    pub src: String,
    /// The recipient of the message
    pub dest: String,
    /// The message payload
    pub body: MessageBody,
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Message[src={}, dest={}, body.message_type={:#?}, body.msg_id={:?}, body.in_reply_to={:#?}]",
            self.src,
            self.dest,
            self.body.message_type,
            self.body.msg_id,
            self.body.in_reply_to)
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct MessageBody {
    #[serde(rename = "type")]
    pub message_type: MessageType,

    /// A message identifier unique to the sender
    pub msg_id: Option<usize>,
    /// For responses, the reference to the original request
    pub in_reply_to: Option<usize>,

    // init fields
    /// Applicable to `MessageType::init` messages only:
    /// The unique identifier for this node, retain this for future messages
    pub node_id: Option<String>,
    /// Applicable to `MessageType::init` messages only:
    /// All the nodes in the cluster including this one
    pub node_ids: Option<Vec<String>>,

    // echo fields
    pub echo: Option<String>,

    // error fields
    /// Applicable to `MessageType::error` messages only:
    /// Identifier for the error type, 0-9999 are Maelstrom errors
    /// everything higher is a custom error code
    pub code: Option<u16>,
    /// Applicable to `MessageType::error` messages only:
    /// A human-readable description of the error.
    pub text: Option<String>,

    // topology fields
    /// Applicable to `MessageType::topology` messages only:
    /// Identifies who the neighbours are for each node
    pub topology: Option<HashMap<String, Vec<String>>>,

    // broadcast fields
    /// Applicable to `MessageType::broadcast` messages only:
    /// A single message to broadcast to everyone
    pub message: Option<Box<RawValue>>,
    /// Applicable to `MessageType::read_ok` messages only:
    /// All messages present on a node
    pub messages: Option<Vec<Box<RawValue>>>,
}

impl PartialEq for MessageBody {
    fn eq(&self, other: &Self) -> bool {
        self.message_type == other.message_type
            && self.msg_id == other.msg_id
            && self.in_reply_to == other.in_reply_to
    }
}

impl Eq for MessageBody {}

/// For more details, see https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md
#[derive(Deserialize, Serialize, Debug, Hash, Eq, PartialEq, Copy, Clone)]
#[allow(non_camel_case_types)]
pub enum MessageType {
    init,
    init_ok,
    error,
    /// "A simple echo workload: sends a message, and expects to get that same message back."
    echo,
    echo_ok,
    /// "Sends a single message into the broadcast system, and requests that it be broadcast to
    /// everyone."
    broadcast,
    broadcast_ok,
    /// "A topology message is sent at the start of the test, after initialization, and informs the
    /// node of an optional network topology to use for broadcast. The topology consists of a map of
    /// node IDs to lists of neighbor node IDs."
    topology,
    topology_ok,
    /// "Requests all messages present on a node."
    read,
    read_ok,
}

impl Message {
    pub fn init_ok(source: &str, destination: &str, message_id: usize, in_reply_to: usize) -> Self {
        Self {
            src: source.to_owned(),
            dest: destination.to_owned(),
            body: MessageBody {
                message_type: MessageType::init_ok,
                node_id: None,
                node_ids: None,
                echo: None,
                code: None,
                msg_id: Some(message_id),
                in_reply_to: Some(in_reply_to),
                text: None,
                topology: None,
                message: None,
                messages: None,
            },
        }
    }

    pub fn error(
        source: &str,
        destination: &str,
        in_reply_to: usize,
        code: u16,
        text: &str,
    ) -> Self {
        Self {
            src: source.to_owned(),
            dest: destination.to_owned(),
            body: MessageBody {
                message_type: MessageType::error,
                node_id: None,
                node_ids: None,
                echo: None,
                code: Some(code),
                msg_id: None,
                in_reply_to: Some(in_reply_to),
                text: Some(text.to_owned()),
                topology: None,
                message: None,
                messages: None,
            },
        }
    }
}
