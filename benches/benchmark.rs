use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pilgrimage::broker::Broker;
use pilgrimage::message::message::Message;
use std::sync::{Arc, Mutex};

fn benchmark_broker(c: &mut Criterion) {
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs")));

    c.bench_function("send_message", |b| {
        b.iter(|| {
            let broker = broker.lock().unwrap();
            let _ = black_box(broker.send_message(Message::from("test message".to_string())));
        })
    });

    c.bench_function("send_benchmark_message", |b| {
        b.iter(|| {
            let broker = broker.lock().unwrap();
            broker
                .send_message(Message::from("benchmark message".to_string()))
                .unwrap();
        })
    });
}

criterion_group!(benches, benchmark_broker);
criterion_main!(benches);
