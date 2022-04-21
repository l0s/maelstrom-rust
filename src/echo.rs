extern crate serde;
extern crate serde_with;

mod lib;

use crate::lib::Message;
use crate::lib::MessageType;
use std::io;
use std::io::Write;

fn main() {
    // Application State
    let mut node_id = String::new();
    let mut next_message_id = 0usize;

    // Communication Channels

    // channel for inbound network messages
    let input = io::stdin();
    // channel for outbound network messages
    let output = io::stdout();

    loop {
        let mut buffer = String::new();
        match input.read_line(&mut buffer) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    break;
                }
                let request = serde_json::from_str::<Message>(&buffer).unwrap();
                eprintln!("Received: {:?}", request);

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
            Err(e) => {
                eprintln!("Input Error: {}", e);
                panic!();
            }
        }
    }
}
