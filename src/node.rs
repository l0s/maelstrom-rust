use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use AppError::{AlreadyInitialised, MissingField};

/// Application-specific errors which may occur. Note that these _do not_ correspond one-to-one with
/// the Maelstrom protocol errors.
#[derive(Debug)]
pub enum AppError {
    MissingField(String),
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

    pub(crate) fn code(&self) -> u16 {
        match self {
            MissingField(_) => 12,
            AlreadyInitialised => 22,
        }
    }
}

#[derive(Clone)]
pub struct Node {
    pub(crate) node_id: Arc<RwLock<String>>,
    pub(crate) next_message_id: Arc<AtomicUsize>,
    init_sync: Arc<(Mutex<bool>, Condvar)>,
    /// The other node IDs in the cluster
    node_ids: Arc<RwLock<Vec<String>>>,
}

impl Node {
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

    pub fn get_and_increment_message_id(&self) -> usize {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }

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
