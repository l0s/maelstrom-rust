extern crate serde;
extern crate serde_with;

use std::io;
use std::io::Write;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[derive(Deserialize, Serialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug)]
struct MessageBody {
    #[serde(rename = "type")]
    message_type: MessageType,

    msg_id: Option<usize>,
    in_reply_to: Option<usize>,

    // init fields
    node_id: Option<String>,
    node_ids: Option<Vec<String>>,

    // echo fields
    echo: Option<String>,

    // error fields
    code: Option<u16>,
    text: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
#[allow(non_camel_case_types)]
enum MessageType {
    init,
    init_ok,
    error,
    echo,
    echo_ok,
}

impl Message {
    fn init_ok(source: &str, destination: &str, message_id: usize, in_reply_to: usize) -> Self {
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

    fn echo(
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

    fn error(source: &str, destination: &str, in_reply_to: usize, code: u16, text: &str) -> Self {
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

fn main() {
    // Application State
    let mut node_id = String::new();
    let mut next_message_id = 0usize;

    // Communication Channels

    // channel for inbound network messages
    let input = io::stdin();
    // channel for outbound network messages
    let output = io::stdout();

    let mut buffer = String::new();
    while let Ok(bytes_read) = input.read_line(&mut buffer) {
        if bytes_read == 0 {
            break;
        }
        let request = serde_json::from_str::<Message>(&buffer).unwrap();
        eprintln!("Received: {:?}", request);
        buffer.clear();

        let request_id = request.body.msg_id.unwrap(); // TODO return error

        match request.body.message_type {
            MessageType::init => {
                node_id = request.body.node_id.unwrap(); // TODO return error
                let response =
                    Message::init_ok(&node_id, &request.src, next_message_id, request_id);
                next_message_id += 1;
                eprintln!("Response: {:?}", response);
                let response = serde_json::to_string(&response).unwrap();
                let mut handle = output.lock();
                handle.write_all(response.as_bytes()).unwrap();
                handle.write_all("\n".as_bytes()).unwrap();
                handle.flush().unwrap();

                eprintln!("Initialized node {}", node_id);
            }
            MessageType::echo => {
                let response = Message::echo(
                    &node_id,
                    &request.src,
                    next_message_id,
                    request_id,
                    &request.body.echo.unwrap(),
                );
                next_message_id += 1;
                eprintln!("Echoing: {:?}", response.body);
                let response = serde_json::to_string(&response).unwrap();
                let mut handle = output.lock();
                handle.write_all(response.as_bytes()).unwrap();
                handle.write_all("\n".as_bytes()).unwrap();
                handle.flush().unwrap();
            }
            _ => {
                let response = Message::error(
                    &node_id,
                    &request.src,
                    request_id,
                    10,
                    "Not yet implemented",
                );
                eprintln!("Response: {:?}", response);
                let response = serde_json::to_string(&response).unwrap();
                let mut handle = output.lock();
                handle.write_all(response.as_bytes()).unwrap();
                handle.write_all("\n".as_bytes()).unwrap();
                handle.flush().unwrap();
                eprintln!("Unhandled error: {}", response);
            }
        }
    }
}
