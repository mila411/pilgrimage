//! Module for managing storage nodes and consumer groups.
//!
//! This module provides functions for checking the health of storage nodes and
//! recovering them when they become unavailable. It also provides functions for
//! resetting the assignments of consumer groups when a storage node is recovered.

use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::storage::Storage;
use std::collections::HashMap;
use std::sync::Mutex;

/// A type alias for a collection of consumer groups, keyed by their group names.
///
/// This type is used to store the consumer groups that are managed by the broker.
pub type ConsumerGroups = HashMap<String, ConsumerGroup>;

/// Checks the health of a storage node.
///
/// This function locks the provided storage and checks if it is available.
///
/// # Arguments
/// * `storage` - A reference to a `Mutex<Storage>` that represents the storage node.
///
/// # Returns
/// * `true` - If the storage node is available.
/// * `false` - If the storage node is not available.
///
/// # Examples
/// ```
/// use std::sync::Mutex;
/// use std::path::PathBuf;
/// use pilgrimage::broker::node_management::check_node_health;
/// use pilgrimage::broker::storage::Storage;
///
/// // Create a new storage node
/// let storage = Mutex::new(Storage::new(PathBuf::from("test_check_node_health")).unwrap());
///
/// // Check the health of the storage node
/// let is_available = check_node_health(&storage);
/// // Assert that the storage node is available
/// assert!(is_available);
/// ```
pub fn check_node_health(storage: &Mutex<Storage>) -> bool {
    let storage_guard = storage.lock().unwrap();
    storage_guard.is_available()
}

/// Recovers a storage node and resets the assignments of consumer groups.
///
/// This function attempts to reinitialize the provided storage and resets the
/// assignments of all consumer groups.
///
/// # Arguments
/// * `storage` - A reference to a `Mutex<Storage>` that represents the storage node.
/// * `consumer_groups` - A reference to a `Mutex<ConsumerGroups>` that represents
///   the collection of consumer groups.
///
/// # Examples
/// ```
/// use std::sync::Mutex;
/// use std::path::PathBuf;
/// use pilgrimage::broker::node_management::recover_node;
/// use pilgrimage::broker::consumer::group::ConsumerGroup;
/// use pilgrimage::broker::storage::Storage;
/// use std::collections::HashMap;
///
/// // Create a new storage node
/// let storage = Mutex::new(Storage::new(PathBuf::from("test_recover_node")).unwrap());
///
/// // Create a collection of consumer groups
/// let consumer_groups = Mutex::new(HashMap::new());
///
/// // Create a consumer group
/// let group = ConsumerGroup::new("test_group");
///
/// // Insert the consumer group into the collection
/// consumer_groups.lock().unwrap().insert("test_group".to_string(), group);
///
/// // Recover the storage node
/// recover_node(&storage, &consumer_groups);
/// // Assert that the storage node is available
/// assert!(storage.lock().unwrap().is_available());
/// ```
pub fn recover_node(storage: &Mutex<Storage>, consumer_groups: &Mutex<ConsumerGroups>) {
    let mut storage_guard = storage.lock().unwrap();
    if let Err(e) = storage_guard.reinitialize() {
        eprintln!("Storage initialization failed.: {}", e);
    }

    let mut groups_guard = consumer_groups.lock().unwrap();
    for group in groups_guard.values_mut() {
        group.reset_assignments();
    }
}
