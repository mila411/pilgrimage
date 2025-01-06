use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use pilgrimage::broker::{Broker, Message};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

// Benchmark for sending a single message
async fn send_single_message(broker: Arc<Mutex<Broker>>, message_content: String) {
    let broker_lock = broker.lock().await;
    let mut transaction = broker_lock.begin_transaction();
    let message = Message::new(message_content);

    if let Ok(_) = broker_lock
        .send_message_transaction(&mut transaction, message)
        .await
    {
        transaction.commit().expect("Transaction commit failed.");
    }
}

// Benchmark function
fn benchmark_message_processing(c: &mut Criterion) {
    // Creating Tokio Runtime
    let rt = Runtime::new().unwrap();

    // Broker initialization
    let broker = Arc::new(Mutex::new(rt.block_on(async {
        Broker::new("broker1", 3, 2, "data/broker1/logs/messages.log").await
    })));

    let mut group = c.benchmark_group("message_processing");

    // メッセージ数を設定
    for &num_messages in &[10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("batch_messages", num_messages),
            &num_messages,
            |b, &num_messages| {
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..num_messages {
                            let message_content = format!("Message {}", i);
                            send_single_message(Arc::clone(&broker), message_content).await;
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10); // Reduce the number of samples
    targets = benchmark_message_processing
}
criterion_main!(benches);
