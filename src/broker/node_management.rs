use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::storage::Storage;
use std::collections::HashMap;
use std::sync::Mutex;

pub type ConsumerGroups = HashMap<String, ConsumerGroup>;

pub fn check_node_health(storage: &Mutex<Storage>) -> bool {
    let storage_guard = storage.lock().unwrap();
    storage_guard.is_available()
}

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
