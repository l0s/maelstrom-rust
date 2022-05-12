use std::sync::atomic::{AtomicUsize, Ordering};

use AppError::{AlreadyInitialised, MissingField};

/// Application-specific errors which may occur. Note that these _do not_ correspond one-to-one with
/// the Maelstrom protocol errors.
#[derive(Debug)]
pub enum AppError {
    /// The request message is missing a required field. The parameter contains the dot-notation
    /// path to the missing field.
    MissingField(String),
    /// An initialisation request was received but the application was already initialised.
    AlreadyInitialised,
}

impl AppError {
    /// From the protocol documentation: "Errors are either definite or indefinite. A definite error
    /// means that the requested operation definitely did not (and never will) happen. An indefinite
    /// error means that the operation might have happened, or might never happen, or might happen
    /// at some later time. Maelstrom uses this information to interpret histories correctly, so
    /// it's important that you never return a definite error under indefinite conditions. When in
    /// doubt, indefinite is always safe. Custom error codes are always indefinite."
    fn _is_definite(&self) -> bool {
        match self {
            MissingField(_) | AlreadyInitialised => true,
            // _ => false,
        }
    }

    /// The Maelstrom error code as documented here:
    /// https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors .
    pub(crate) fn code(&self) -> u16 {
        match self {
            MissingField(_) => 12,
            AlreadyInitialised => 22,
        }
    }
}

/// A node in a Maelstrom distributed system
#[derive(Debug)]
pub struct Node {
    /// The node's unique identifier, which won't be available until it has been initialised
    pub node_id: String,
    /// The counter for unique message IDs
    pub(crate) next_message_id: AtomicUsize,
    /// The other node IDs in the cluster
    pub node_ids: Vec<String>,
}

impl Node {
    /// Get the next available message identifier
    pub fn get_and_increment_message_id(&self) -> usize {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }
}
