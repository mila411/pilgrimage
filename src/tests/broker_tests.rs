use super::*;
use pilgrimage::broker::{Broker, Node};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use tempfile::tempdir;

fn create_test_broker() -> Broker {
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("test_storage")
        .to_str()
        .unwrap()
        .to_owned();

    Broker {
        consumer_groups: Arc::new(Mutex::new(HashMap::new())),
        id: String::from("test_broker"),
        leader: Arc::new(Mutex::new(Some(String::from("leader1")))),
        storage: Arc::new(Mutex::new(Storage::new(&storage_path).unwrap())),
        topics: HashMap::new(),
        num_partitions: 1,
        replication_factor: 1,
        term: AtomicU64::new(1),
        leader_election: LeaderElection::new("test_broker", HashMap::new()),
        nodes: Arc::new(Mutex::new(HashMap::new())),
        partitions: Arc::new(Mutex::new(HashMap::new())),
        replicas: Arc::new(Mutex::new(HashMap::new())),
        message_queue: Arc::new(MessageQueue::new(1, 10, Duration::from_secs(1))),
        log_path: "logs/broker.log".to_string(),
    }
}

fn create_test_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        address: "127.0.0.1:8080".to_string(),
        is_active: true,
        data: Arc::new(Mutex::new(Vec::new())),
    }
}

fn create_test_broker_with_path(log_path: &str) -> Broker {
    Broker {
        id: "test_broker".to_string(),
        topics: HashMap::new(),
        num_partitions: 3,
        replication_factor: 2,
        term: AtomicU64::new(0),
        leader_election: LeaderElection::new("test_broker", HashMap::new()),
        storage: Arc::new(Mutex::new(Storage::new("test_storage").unwrap())),
        consumer_groups: Arc::new(Mutex::new(HashMap::new())),
        nodes: Arc::new(Mutex::new(HashMap::new())),
        partitions: Arc::new(Mutex::new(HashMap::new())),
        replicas: Arc::new(Mutex::new(HashMap::new())),
        leader: Arc::new(Mutex::new(None)),
        message_queue: Arc::new(MessageQueue::new(1, 10, Duration::from_secs(30))),
        log_path: log_path.to_string(),
    }
}

fn setup_test_logs(path: &str) {
    cleanup_test_logs(path);
}

fn cleanup_test_logs(path: &str) {
    if Path::new(path).exists() {
        let _ = fs::remove_file(path);
    }
    let old_path = format!("{}.old", path);
    if Path::new(&old_path).exists() {
        let _ = fs::remove_file(old_path);
    }
}

#[test]
fn test_node_creation() {
    let node = Node::new("node1", "127.0.0.1:8080", true);
    assert_eq!(node.id, "node1");
    assert_eq!(node.address, "127.0.0.1:8080");
    assert!(node.is_active);
}

#[test]
fn test_message_persistence() {
    let test_log = "test_logs_persistence";
    setup_test_logs(test_log);

    let mut broker = Broker::new("broker1", 3, 2, test_log);
    broker.create_topic("test_topic", None).unwrap();

    broker
        .publish_with_ack("test_topic", "test_message_1".to_string(), None)
        .unwrap();
    broker
        .publish_with_ack("test_topic", "test_message_2".to_string(), None)
        .unwrap();
    broker
        .publish_with_ack("test_topic", "test_message_3".to_string(), None)
        .unwrap();

    let storage = Storage::new(test_log).unwrap();
    let messages = storage.read_messages().unwrap();
    assert_eq!(
        messages.len(),
        3,
        "The number of expected messages differs from the actual number of messages."
    );
    assert_eq!(messages[0], "test_message_1");
    assert_eq!(messages[1], "test_message_2");
    assert_eq!(messages[2], "test_message_3");

    cleanup_test_logs(test_log);
}

#[test]
fn test_log_rotation_and_cleanup() {
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("test_storage")
        .to_str()
        .unwrap()
        .to_owned();
    let storage = Storage::new(&storage_path).unwrap();

    let result = storage.rotate_logs();
    assert!(result.is_err(), "Old log file does not exist");

    let old_log_path = format!("{}.old", storage_path);
    File::create(&old_log_path).unwrap();
    let result = storage.rotate_logs();
    assert!(
        result.is_ok(),
        "Log rotation should succeed when old log file exists"
    );
}

#[test]
fn test_broker_creation() {
    let test_log = "test_logs_creation";
    setup_test_logs(test_log);

    let broker = Broker::new("broker1", 3, 2, test_log);
    assert_eq!(broker.id, "broker1");
    assert_eq!(broker.num_partitions, 3);
    assert_eq!(broker.replication_factor, 2);

    cleanup_test_logs(test_log);
}

#[test]
fn test_create_topic() {
    let test_log = "test_logs_topic";
    setup_test_logs(test_log);

    let mut broker = Broker::new("broker1", 3, 2, test_log);
    broker.create_topic("test_topic", None).unwrap();
    assert!(broker.topics.contains_key("test_topic"));

    cleanup_test_logs(test_log);
}

#[test]
fn test_subscribe_and_publish() {
    let test_log = "test_logs_sub_pub";
    setup_test_logs(test_log);

    let mut broker = Broker::new("broker1", 3, 2, test_log);
    broker.create_topic("test_topic", None).unwrap();

    let subscriber = Subscriber::new(
        "sub1",
        Box::new(|msg: String| {
            println!("Received message: {}", msg);
        }),
    );
    broker.subscribe("test_topic", subscriber, None).unwrap();

    let ack = broker
        .publish_with_ack("test_topic", "test_message".to_string(), None)
        .unwrap();
    assert_eq!(ack.topic, "test_topic");

    cleanup_test_logs(test_log);
}

#[test]
fn test_consumer_group_message_distribution() {
    println!("Test Start");
    let mut broker = Broker::new("broker1", 3, 2, "logs");
    broker.create_topic("test_topic", None).unwrap();

    let received_count = Arc::new((Mutex::new(0), Condvar::default()));
    let total_messages = 10;

    let received_messages = Arc::new(Mutex::new(Vec::new()));

    for i in 0..2 {
        let received_messages = Arc::clone(&received_messages);
        let received_count = Arc::clone(&received_count);

        let subscriber = Subscriber::new(
            &format!("consumer_{}", i),
            Box::new(move |msg: String| {
                received_messages.lock().unwrap().push(msg);

                let (lock, cvar) = &*received_count;
                let mut count = lock.lock().unwrap();
                *count += 1;
                println!("Received count: {}", *count);
                if *count == total_messages {
                    cvar.notify_all();
                }
            }),
        );

        broker
            .subscribe("test_topic", subscriber, Some("group1"))
            .unwrap();
        thread::sleep(Duration::from_millis(100));
    }

    println!("Start sending message");
    for i in 0..total_messages {
        let message = format!("message_{}", i);
        println!("Sending: {}", message);
        broker
            .publish_with_ack("test_topic", message, None)
            .unwrap();
        thread::sleep(Duration::from_millis(10));
    }

    let (lock, cvar) = &*received_count;
    let timeout = Duration::from_secs(5);
    let result = cvar
        .wait_timeout_while(lock.lock().unwrap(), timeout, |&mut count| {
            count < total_messages
        })
        .unwrap();

    if result.1.timed_out() {
        panic!(
            "The test timed out: Number of messages received {}",
            *result.0
        );
    }

    let received = received_messages.lock().unwrap();
    println!("Received messages: {:?}", *received);
    assert_eq!(received.len(), total_messages);
}

#[test]
fn test_data_replication() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let node1 = create_test_node("node1");
    let node2 = create_test_node("node2");

    broker.add_node("node1".to_string(), node1);
    broker.add_node("node2".to_string(), node2);

    {
        let mut replicas = broker.replicas.lock().unwrap();
        replicas.insert("1".to_string(), vec![
            "node1".to_string(),
            "node2".to_string(),
        ]);
    }

    let data = b"test data";
    broker.replicate_data(1, data);

    let nodes = broker.nodes.lock().unwrap();
    let node1_data = nodes.get("node1").unwrap().data.lock().unwrap().clone();
    let node2_data = nodes.get("node2").unwrap().data.lock().unwrap().clone();

    assert_eq!(node1_data, data);
    assert_eq!(node2_data, data);
}

#[test]
fn test_failover() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let node1 = create_test_node("node1");
    let node2 = create_test_node("node2");

    broker.add_node("node1".to_string(), node1);
    broker.add_node("node2".to_string(), node2);

    {
        let mut leader = broker.leader.lock().unwrap();
        *leader = Some("node1".to_string());
    }

    broker.detect_failure("node1");

    let nodes = broker.nodes.lock().unwrap();
    assert!(!nodes.contains_key("node1"));
    assert!(nodes.contains_key("node2"));

    let leader = broker.leader.lock().unwrap();
    assert_eq!(leader.as_deref(), Some("node2"));
}

#[test]
fn test_rebalance() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let node1 = create_test_node("node1");
    let node2 = create_test_node("node2");

    broker.add_node("node1".to_string(), node1);
    broker.add_node("node2".to_string(), node2);

    {
        let mut partitions = broker.partitions.lock().unwrap();
        for i in 0..3 {
            partitions.insert(i, Partition {
                node_id: String::default(),
            });
        }
    }

    broker.rebalance_partitions();
    let partitions = broker.partitions.lock().unwrap();
    let nodes = broker.nodes.lock().unwrap();
    for (partition_id, partition) in partitions.iter() {
        let node_index = partition_id % nodes.len();
        let expected_node_id = nodes.keys().nth(node_index).unwrap();
        assert_eq!(&partition.node_id, expected_node_id);
    }
}

#[test]
fn test_add_existing_node() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let node = create_test_node("node1");

    broker.add_node("node1".to_string(), node.clone());
    broker.add_node("node1".to_string(), node);

    let nodes = broker.nodes.lock().unwrap();
    assert_eq!(nodes.len(), 1);
}

#[test]
fn test_remove_nonexistent_node() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    broker.remove_node("node1");

    let nodes = broker.nodes.lock().unwrap();
    assert_eq!(nodes.len(), 0);
}

#[test]
fn test_replicate_to_nonexistent_node() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let data = b"test data";

    broker.replicate_data(1, data);

    let nodes = broker.nodes.lock().unwrap();
    assert!(nodes.is_empty());
}

#[test]
fn test_election_with_no_nodes() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let elected = broker.start_election();

    assert!(!elected);
}

#[test]
fn test_add_invalid_node() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let node = create_test_node("node1");

    broker.add_node("node1".to_string(), node.clone());
    broker.add_node("node1".to_string(), node);

    let nodes = broker.nodes.lock().unwrap();
    assert_eq!(nodes.len(), 1, "Duplicate nodes should not be added.");
}

#[test]
fn test_elect_leader_with_no_nodes() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let elected = broker.start_election();
    assert!(
        !elected,
        "If there are no nodes, the leader election should fail."
    );
}

#[test]
fn test_replicate_to_nonexistent_nodes() {
    let broker = Broker::new("broker1", 3, 2, "logs");
    let data = b"test data";

    broker.replicate_data(1, data);

    let nodes = broker.nodes.lock().unwrap();
    assert!(
        nodes.is_empty(),
        "If the node does not exist, replication should fail."
    );
}

#[test]
fn test_rotate_logs() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    {
        let mut file = File::create(&log_path).unwrap();
        writeln!(file, "This is a test log entry.").unwrap();
    }

    let broker = create_test_broker_with_path(log_path.to_str().unwrap());

    broker.rotate_logs();
    let rotated_log_path = log_path.with_extension("old");

    assert!(rotated_log_path.exists());
    assert!(log_path.exists());

    let new_log_content = fs::read_to_string(&log_path).unwrap();
    assert!(new_log_content.is_empty());
}

#[test]
fn test_rotate_logs_invalid_path() {
    let broker = create_test_broker_with_path("/invalid/path/test.log");
    broker.rotate_logs();
}

#[test]
fn test_rotate_logs_no_permissions() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    {
        let mut file = File::create(&log_path).unwrap();
        writeln!(file, "test data").unwrap();
    }

    fs::set_permissions(
        &log_path,
        <fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o444),
    )
    .unwrap();

    let broker = create_test_broker_with_path(log_path.to_str().unwrap());
    broker.rotate_logs();
}

#[test]
fn test_rotate_logs_file_not_found() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("nonexistent.log");

    let broker = create_test_broker_with_path(log_path.to_str().unwrap());
    broker.rotate_logs();
}

#[test]
fn test_rotate_logs_file_in_use() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("locked.log");
    let file = OpenOptions::new()
        .append(true)
        .open("path/to/file")
        .unwrap();

    let broker = create_test_broker_with_path(log_path.to_str().unwrap());
    broker.rotate_logs();

    std::mem::drop(file);
}

#[test]
fn test_write_log() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    fs::create_dir_all(log_path.parent().unwrap()).unwrap();

    let broker = create_test_broker_with_path(log_path.to_str().unwrap());
    broker.write_log("Test message 1").unwrap();
    broker.write_log("Test message 2").unwrap();

    let content = fs::read_to_string(&log_path).unwrap();
    assert!(content.contains("Test message 1"));
    assert!(content.contains("Test message 2"));
}

#[test]
fn test_write_log_invalid_path() {
    let dir = tempdir().unwrap();
    let invalid_path = dir.path().join("nonexistent").join("test.log");
    let broker = create_test_broker_with_path(invalid_path.to_str().unwrap());

    // Test error handling - check that it returns an error
    let result = broker.write_log("This should not panic");
    assert!(result.is_err());
}

#[test]
fn test_write_log_no_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    // Create a file and set the permissions to read-only.
    {
        let mut file = File::create(&log_path).unwrap();
        writeln!(file, "Initial content").unwrap();
    }
    fs::set_permissions(&log_path, fs::Permissions::from_mode(0o444)).unwrap();

    let broker = create_test_broker_with_path(log_path.to_str().unwrap());
    let result = broker.write_log("This should handle permission error");
    assert!(result.is_err());
}

#[test]
fn test_rotate_logs_with_invalid_storage() {
    let storage_result = Storage::new("/invalid/path/to/storage");
    assert!(
        storage_result.is_err(),
        "Operations on invalid storage paths should fail."
    );
}

#[test]
fn test_multithreaded_message_processing() {
    let broker = Arc::new(Broker::new("broker1", 3, 2, "logs"));

    let broker_sender = Arc::clone(&broker);
    let sender_handle = thread::spawn(move || {
        for i in 0..10 {
            let message = format!("Message {}", i);
            broker_sender.send_message(message);
        }
    });

    let broker_receiver = Arc::clone(&broker);
    let receiver_handle = thread::spawn(move || {
        for _ in 0..10 {
            if let Some(_message) = broker_receiver.receive_message() {}
        }
    });

    sender_handle.join().unwrap();
    receiver_handle.join().unwrap();
}

#[test]
fn test_log_rotation_error_message() {
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("test_storage")
        .to_str()
        .unwrap()
        .to_owned();
    let storage = Storage::new(&storage_path).unwrap();

    let result = storage.rotate_logs();
    if let Err(e) = result {
        assert_eq!(
            e.to_string(),
            format!("Old log file {}.old does not exist", storage_path)
        );
    } else {
        panic!("Expected an error but got success");
    }
}

#[test]
fn test_write_and_read_messages() {
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir
        .path()
        .join("test_storage")
        .to_str()
        .unwrap()
        .to_owned();
    let mut storage = Storage::new(&storage_path).unwrap();

    storage.write_message("test_message_1").unwrap();
    storage.write_message("test_message_2").unwrap();

    let messages = storage.read_messages().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0], "test_message_1");
    assert_eq!(messages[1], "test_message_2");
}

#[test]
fn test_perform_operation_with_retry_success_on_first_try() {
    let broker = create_test_broker();

    let result = broker.perform_operation_with_retry(
        || Ok::<&str, &str>("Success"),
        3,
        Duration::from_millis(10),
    );

    assert_eq!(result.unwrap(), "Success");
}

#[test]
fn test_perform_operation_with_retry_success_after_retries() {
    let broker = create_test_broker();

    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let result = broker.perform_operation_with_retry(
        || {
            let mut num = counter_clone.lock().unwrap();
            *num += 1;
            if *num < 3 {
                Err("Temporary Error")
            } else {
                Ok("Success after retries")
            }
        },
        5,
        Duration::from_millis(10),
    );

    assert_eq!(result.unwrap(), "Success after retries");
    assert_eq!(*counter.lock().unwrap(), 3);
}

#[test]
fn test_perform_operation_with_retry_failure_after_max_retries() {
    let broker = create_test_broker();

    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let result = broker.perform_operation_with_retry(
        || {
            let mut num = counter_clone.lock().unwrap();
            *num += 1;
            Err::<(), &str>("Persistent Error")
        },
        3,
        Duration::from_millis(10),
    );

    assert!(result.is_err());
    assert_eq!(*counter.lock().unwrap(), 4);
}
