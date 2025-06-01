use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use pilgrimage::broker::Broker;
use pilgrimage::schema::message_schema::MessageSchema;
use std::fs;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Common setup for benchmarks
struct BenchmarkSetup {
    broker: Arc<Mutex<Broker>>,
    topic_name: String,
}

impl BenchmarkSetup {
    fn new(broker_id: &str, storage_path: &str) -> Self {
        // Clean up any existing storage directory
        let storage_dir = std::path::Path::new(storage_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let _ = fs::remove_dir_all(storage_dir);

        // Create storage directory
        fs::create_dir_all(storage_dir).expect("Failed to create storage directory");

        let broker = Arc::new(Mutex::new(Broker::new(broker_id, 3, 2, storage_path)));

        let topic_name = format!("bench-topic-{}", Uuid::new_v4());

        // Create test topic
        {
            let mut broker_instance = broker.lock().unwrap();
            broker_instance
                .create_topic(&topic_name, None)
                .expect("Failed to create topic");
        }

        BenchmarkSetup { broker, topic_name }
    }

    fn create_test_message(&self, content: &str, partition: usize) -> MessageSchema {
        MessageSchema::new()
            .with_content(content.to_string())
            .with_topic(self.topic_name.clone())
            .with_partition(partition)
    }
}

impl Drop for BenchmarkSetup {
    fn drop(&mut self) {
        // Clean up storage after benchmark - just remove the storage directory
        let _ = fs::remove_dir_all("storage");
    }
}

/// Benchmark message sending operations
fn benchmark_message_sending(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_sending");

    // Test different message sizes
    let medium_content = "x".repeat(1024);
    let large_content = "x".repeat(10240);
    let message_sizes = vec![
        ("small", "Hello World!"),
        ("medium", medium_content.as_str()),
        ("large", large_content.as_str()),
    ];

    for (size_name, content) in message_sizes {
        let setup = BenchmarkSetup::new(
            &format!("send-bench-{}", size_name),
            &format!("storage/bench-send-{}.log", size_name),
        );

        group.bench_with_input(
            BenchmarkId::new("send_message", size_name),
            &(setup, content),
            |b, (setup, content)| {
                b.iter(|| {
                    let mut broker_instance = setup.broker.lock().unwrap();
                    let msg = setup.create_test_message(content, 0);
                    let _ = black_box(broker_instance.send_message(msg));
                })
            },
        );
    }

    group.finish();
}

/// Benchmark message receiving operations
fn benchmark_message_receiving(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_receiving");

    let setup = BenchmarkSetup::new("receive-bench", "storage/bench-receive.log");

    // Pre-populate with messages
    {
        let mut broker_instance = setup.broker.lock().unwrap();
        for i in 0..100 {
            let msg = setup.create_test_message(&format!("Message {}", i), 0);
            broker_instance.send_message(msg).unwrap();
        }
    }

    group.bench_function("receive_message", |b| {
        b.iter(|| {
            let broker_instance = setup.broker.lock().unwrap();
            let _ = black_box(broker_instance.receive_message(&setup.topic_name, 0));
        })
    });

    group.finish();
}

/// Benchmark topic operations
fn benchmark_topic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic_operations");

    let setup = BenchmarkSetup::new("topic-bench", "storage/bench-topic.log");

    group.bench_function("create_topic", |b| {
        b.iter(|| {
            let mut broker_instance = setup.broker.lock().unwrap();
            let topic_name = format!("temp-topic-{}", Uuid::new_v4());
            let _ = black_box(broker_instance.create_topic(&topic_name, None));
        })
    });

    group.bench_function("list_topics", |b| {
        b.iter(|| {
            let broker_instance = setup.broker.lock().unwrap();
            let _ = black_box(broker_instance.list_topics());
        })
    });

    group.finish();
}

/// Benchmark partition operations
fn benchmark_partition_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_operations");

    // Test different partition counts
    for partition_count in [1usize, 2, 4, 8].iter() {
        let setup = BenchmarkSetup::new(
            &format!("partition-bench-{}", partition_count),
            &format!("storage/bench-partition-{}.log", partition_count),
        );

        group.bench_with_input(
            BenchmarkId::new("send_to_partitions", partition_count),
            &(setup, *partition_count),
            |b, (setup, partition_count)| {
                b.iter(|| {
                    let mut broker_instance = setup.broker.lock().unwrap();
                    let partition = (0..*partition_count).collect::<Vec<usize>>();
                    for &p in &partition {
                        let msg = setup.create_test_message("Partition test", p);
                        let _ = black_box(broker_instance.send_message(msg));
                    }
                })
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent operations
fn benchmark_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");

    let setup = BenchmarkSetup::new("concurrent-bench", "storage/bench-concurrent.log");

    group.bench_function("concurrent_send_receive", |b| {
        b.iter(|| {
            // Send a message
            {
                let mut broker_instance = setup.broker.lock().unwrap();
                let msg = setup.create_test_message("Concurrent test", 0);
                broker_instance.send_message(msg).unwrap();
            }

            // Immediately try to receive
            {
                let broker_instance = setup.broker.lock().unwrap();
                let _ = black_box(broker_instance.receive_message(&setup.topic_name, 0));
            }
        })
    });

    group.finish();
}

/// Benchmark throughput with batch operations
fn benchmark_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.sample_size(10); // Reduce sample size for batch operations

    let batch_sizes = vec![10, 100, 1000];

    for batch_size in batch_sizes {
        let setup = BenchmarkSetup::new(
            &format!("throughput-bench-{}", batch_size),
            &format!("storage/bench-throughput-{}.log", batch_size),
        );

        group.bench_with_input(
            BenchmarkId::new("batch_send", batch_size),
            &(setup, batch_size),
            |b, (setup, batch_size)| {
                b.iter(|| {
                    let mut broker_instance = setup.broker.lock().unwrap();
                    for i in 0..*batch_size {
                        let msg = setup.create_test_message(&format!("Batch message {}", i), 0);
                        broker_instance.send_message(msg).unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_message_sending,
    benchmark_message_receiving,
    benchmark_topic_operations,
    benchmark_partition_operations,
    benchmark_concurrent_operations,
    benchmark_throughput
);
criterion_main!(benches);
