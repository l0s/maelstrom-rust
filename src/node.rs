use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

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
#[derive(Clone)]
pub struct Node {
    /// The node's unique identifier, which won't be available until it has been initialised
    pub(crate) node_id: Arc<RwLock<String>>,
    /// The counter for unique message IDs
    next_message_id: Arc<AtomicUsize>,
    /// Object to block all operations until the node is initialised
    init_sync: Arc<(Mutex<bool>, Condvar)>,
    /// The other node IDs in the cluster
    node_ids: Arc<RwLock<Vec<String>>>,
}

impl Node {
    /// Set this node's ID and inform it of the other nodes in the system.
    /// Most node operations will block until this method has been called successfully.
    /// Returns:
    /// - `()` if the node was initialised successfully
    /// - `AppError` if the node was already initialised
    pub(crate) fn init(&mut self, node_id: String, node_ids: Vec<String>) -> Result<(), AppError> {
        let &(ref lock, ref condition) = &*self.init_sync;
        let mut init_guard = lock.lock().unwrap();
        if *init_guard.deref() {
            return Err(AlreadyInitialised);
        }
        {
            let mut target_node_id = self.node_id.write().unwrap();
            *target_node_id = node_id;
        }
        {
            let mut target_node_ids = self.node_ids.write().unwrap();
            *target_node_ids = node_ids;
        }

        *init_guard = true;
        condition.notify_all();

        Ok(())
    }

    /// Get the next available message identifier
    pub fn get_and_increment_message_id(&self) -> usize {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Retrieve the node's identifier. This will block until the node has been initialised.
    pub fn read_node_id(&self) -> String {
        let &(ref lock, ref condition) = &*self.init_sync;
        {
            // only hold the lock long enough to verify that the node is initialised
            let _initialised = condition
                .wait_while(lock.lock().unwrap(), |initialised| !*initialised)
                .unwrap();
        }
        self.node_id.read().unwrap().clone()
    }

    /// Retrieve all the node identifiers in the cluster.
    pub fn read_node_ids(&self) -> Vec<String> {
        let &(ref lock, ref condition) = &*self.init_sync;
        {
            // only hold the lock long enough to verify that the node is initialised
            let _initialised = condition
                .wait_while(lock.lock().unwrap(), |initialised| !*initialised)
                .unwrap();
        }
        self.node_ids.read().unwrap().clone()
    }
}

impl Default for Node {
    fn default() -> Self {
        Self {
            node_id: Arc::new(RwLock::new(String::from("Uninitialised Node"))),
            next_message_id: Arc::new(AtomicUsize::new(0)),
            init_sync: Arc::new((Mutex::new(false), Condvar::new())),
            node_ids: Arc::new(RwLock::new(Vec::default())),
        }
    }
}
