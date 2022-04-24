use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[derive(Deserialize, Serialize, Debug)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug)]
pub struct MessageBody {
    #[serde(rename = "type")]
    pub message_type: MessageType,

    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,

    // init fields
    pub node_id: Option<String>,
    pub node_ids: Option<Vec<String>>,

    // echo fields
    pub echo: Option<String>,

    // error fields
    pub code: Option<u16>,
    pub text: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Hash, Eq, PartialEq, Copy, Clone)]
#[allow(non_camel_case_types)]
pub enum MessageType {
    init,
    init_ok,
    error,
    echo,
    echo_ok,
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
            },
        }
    }

    pub fn echo(
        source: &str,
        destination: &str,
        message_id: usize,
        in_reply_to: usize,
        echo: &str,
    ) -> Self {
        Self {
            src: source.to_owned(),
            dest: destination.to_owned(),
            body: MessageBody {
                message_type: MessageType::echo_ok,
                msg_id: Some(message_id),
                in_reply_to: Some(in_reply_to),
                node_id: None,
                node_ids: None,
                echo: Some(echo.to_owned()),
                code: None,
                text: None,
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
            },
        }
    }
}
