/// Comprehensive Performance Benchmark Tests for Pilgrimage Message Broker
///
/// This module contains extensive benchmark tests covering all critical
/// performance aspects of the distributed message broker system.
use bytes::Bytes;
use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use pilgrimage::broker::performance_optimizer::{
    MemoryPool, PerformanceConfig, PerformanceOptimizer, ZeroCopyBuffer,
};
use pilgrimage::message::message::Message;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Create a benchmark test message with specified size
fn create_benchmark_message(size: usize) -> Message {
    Message {
        id: Uuid::new_v4(),
        content: "x".repeat(size),
        topic_id: "benchmark_topic".to_string(),
        partition_id: 0,
        timestamp: Utc::now(),
        schema: None,
    }
}

/// Helper function to create performance configuration for benchmarks
fn _create_performance_config() -> PerformanceConfig {
    PerformanceConfig {
        zero_copy_enabled: true,
        batch_processing_enabled: true,
        compression_enabled: true,
        batch_size: 100,
        batch_timeout: std::time::Duration::from_millis(10),
        compression_threshold: 1024,
        compression_level: flate2::Compression::fast(),
        parallel_workers: 4,
        memory_pool_size: 1024 * 1024, // 1MB
        prefetch_size: 16,
    }
}

/// Benchmark zero-copy buffer operations
/// Tests buffer creation, slicing, and data access operations
fn benchmark_zero_copy_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("zero_copy_operations");

    println!("Running zero-copy buffer operation benchmarks...");

    for &buffer_size in [1024, 4096, 16384, 65536].iter() {
        let data = vec![0u8; buffer_size];
        let bytes = Bytes::from(data);

        group.throughput(Throughput::Bytes(buffer_size as u64));

        // Buffer creation benchmark
        group.bench_with_input(
            BenchmarkId::new("buffer_creation", buffer_size),
            &buffer_size,
            |b, _| {
                b.iter(|| {
                    let test_bytes = black_box(bytes.clone());
                    ZeroCopyBuffer::new(test_bytes)
                });
            },
        );

        // Buffer slicing benchmark
        let buffer = ZeroCopyBuffer::new(bytes.clone());
        group.bench_with_input(
            BenchmarkId::new("buffer_slicing", buffer_size),
            &buffer_size,
            |b, _| {
                b.iter(|| {
                    let slice_size = buffer_size / 4;
                    black_box(buffer.slice(0, slice_size))
                });
            },
        );

        // Buffer data access benchmark
        group.bench_with_input(
            BenchmarkId::new("buffer_data_access", buffer_size),
            &buffer_size,
            |b, _| {
                b.iter(|| black_box(buffer.as_bytes()));
            },
        );
    }

    group.finish();
    println!("Zero-copy buffer operation benchmarks completed.");
}

/// Benchmark memory pool operations
/// Tests memory allocation, deallocation, and pool efficiency
fn benchmark_memory_pool_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool_operations");

    println!("Running memory pool operation benchmarks...");

    let rt = tokio::runtime::Runtime::new().unwrap();

    for &pool_size in [16, 64, 256, 1024].iter() {
        let memory_pool = Arc::new(MemoryPool::new(pool_size, 1024));

        group.throughput(Throughput::Elements(pool_size as u64));

        // Memory allocation benchmark
        group.bench_with_input(
            BenchmarkId::new("memory_allocation", pool_size),
            &pool_size,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let buffer = memory_pool.acquire_buffer(1024).await.unwrap();
                        black_box(buffer);
                    });
                });
            },
        );

        // Memory allocation and deallocation cycle
        group.bench_with_input(
            BenchmarkId::new("allocation_deallocation_cycle", pool_size),
            &pool_size,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let buffer = memory_pool.acquire_buffer(1024).await.unwrap();
                        memory_pool.release_buffer(buffer).await;
                    });
                });
            },
        );
    }

    group.finish();
    println!("Memory pool operation benchmarks completed.");
}

/// Benchmark message optimization
/// Tests message compression, serialization, and optimization strategies
fn benchmark_message_optimization(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_optimization");

    println!("Running message optimization benchmarks...");

    for &msg_size in [100, 1000, 10000, 100000].iter() {
        let message = create_benchmark_message(msg_size);

        group.throughput(Throughput::Bytes(msg_size as u64));
        group.bench_with_input(
            BenchmarkId::new("message_optimization", msg_size),
            &msg_size,
            |b, _| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let optimizer = PerformanceOptimizer::new(1024 * 50, 100, true);

                b.iter(|| {
                    rt.block_on(async {
                        let test_message = black_box(message.clone());
                        optimizer.optimize_message(test_message).await.unwrap()
                    });
                });
            },
        );

        // Message serialization benchmark
        group.bench_with_input(
            BenchmarkId::new("message_serialization", msg_size),
            &msg_size,
            |b, _| {
                b.iter(|| {
                    let serialized = serde_json::to_vec(&message).unwrap();
                    black_box(serialized);
                });
            },
        );
    }

    group.finish();
    println!("Message optimization benchmarks completed.");
}

/// Benchmark batch processing operations
/// Tests batch creation, processing, and throughput optimization
fn benchmark_batch_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_processing");

    println!("Running batch processing benchmarks...");

    for &batch_size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(batch_size as u64));

        // Batch creation and basic processing
        group.bench_with_input(
            BenchmarkId::new("batch_creation", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    let messages: Vec<_> = (0..batch_size)
                        .map(|i| create_benchmark_message(100 + i * 10))
                        .collect();
                    black_box(messages);
                });
            },
        );

        // Message processing throughput
        group.bench_with_input(
            BenchmarkId::new("message_processing_throughput", batch_size),
            &batch_size,
            |b, _| {
                let messages: Vec<_> = (0..batch_size)
                    .map(|i| create_benchmark_message(100 + i * 10))
                    .collect();

                b.iter(|| {
                    for message in &messages {
                        // Simulate message processing
                        let serialized = serde_json::to_vec(message).unwrap();
                        black_box(serialized);
                    }
                });
            },
        );
    }

    group.finish();
    println!("Batch processing benchmarks completed.");
}

/// Benchmark throughput and latency metrics
/// Tests system performance under various loads and concurrency levels
fn benchmark_throughput_and_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_and_latency");

    println!("Running throughput and latency benchmarks...");

    let rt = tokio::runtime::Runtime::new().unwrap();

    for &msg_count in [100, 1000, 5000, 10000].iter() {
        group.throughput(Throughput::Elements(msg_count as u64));

        // Single-threaded throughput
        group.bench_with_input(
            BenchmarkId::new("single_threaded_throughput", msg_count),
            &msg_count,
            |b, _| {
                let optimizer = PerformanceOptimizer::new(1024 * 100, 100, true);

                b.iter(|| {
                    rt.block_on(async {
                        for _i in 0..msg_count {
                            let message = create_benchmark_message(500);
                            optimizer
                                .optimize_message(black_box(message))
                                .await
                                .unwrap();
                        }
                    });
                });
            },
        );

        // Multi-threaded throughput (simulated)
        group.bench_with_input(
            BenchmarkId::new("multi_threaded_throughput", msg_count),
            &msg_count,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let optimizer = Arc::new(PerformanceOptimizer::new(1024 * 100, 100, true));
                        let mut handles = vec![];

                        let batch_size = msg_count / 4; // 4 threads
                        for _thread in 0..4 {
                            let optimizer_clone = Arc::clone(&optimizer);
                            let handle = tokio::spawn(async move {
                                for i in 0..batch_size {
                                    let message = create_benchmark_message(500 + i * 10);
                                    optimizer_clone.optimize_message(message).await.unwrap();
                                }
                            });
                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    });
                });
            },
        );

        // Latency measurement
        group.bench_with_input(
            BenchmarkId::new("message_latency", msg_count),
            &msg_count,
            |b, _| {
                let optimizer = PerformanceOptimizer::new(1024 * 100, 100, true);

                b.iter(|| {
                    rt.block_on(async {
                        let message = create_benchmark_message(1000);
                        let start = Instant::now();
                        optimizer
                            .optimize_message(black_box(message))
                            .await
                            .unwrap();
                        black_box(start.elapsed());
                    });
                });
            },
        );
    }

    group.finish();
    println!("Throughput and latency benchmarks completed.");
}

/// Benchmark integration scenarios
/// Tests end-to-end system performance with realistic workloads
fn benchmark_integration_scenarios(c: &mut Criterion) {
    let mut group = c.benchmark_group("integration_scenarios");

    println!("Running integration scenario benchmarks...");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Realistic mixed workload scenario
    group.bench_function("realistic_mixed_workload", |b| {
        b.iter(|| {
            rt.block_on(async {
                let optimizer = PerformanceOptimizer::new(1024 * 200, 50, true);

                // Simulate mixed message sizes and types
                let small_messages = 15;
                let medium_messages = 10;
                let large_messages = 5;

                // Process small messages (< 1KB)
                for i in 0..small_messages {
                    let message = create_benchmark_message(100 + i * 50);
                    optimizer.optimize_message(message).await.unwrap();
                }

                // Process medium messages (1-10KB)
                for i in 0..medium_messages {
                    let message = create_benchmark_message(1000 + i * 500);
                    optimizer.optimize_message(message).await.unwrap();
                }

                // Process large messages (10-100KB)
                for i in 0..large_messages {
                    let message = create_benchmark_message(10000 + i * 5000);
                    optimizer.optimize_message(message).await.unwrap();
                }
            });
        });
    });

    // High-concurrency scenario
    group.bench_function("high_concurrency_scenario", |b| {
        b.iter(|| {
            rt.block_on(async {
                let optimizer = Arc::new(PerformanceOptimizer::new(1024 * 500, 100, true));
                let mut handles = vec![];

                // Simulate 8 concurrent producers
                for producer_id in 0..8 {
                    let optimizer_clone = Arc::clone(&optimizer);
                    let handle = tokio::spawn(async move {
                        for message_id in 0..25 {
                            let message_size = 500 + (producer_id * 100) + (message_id * 20);
                            let message = create_benchmark_message(message_size);
                            optimizer_clone.optimize_message(message).await.unwrap();
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }
            });
        });
    });

    group.finish();
    println!("Integration scenario benchmarks completed.");
}

/// Benchmark memory efficiency
/// Tests memory usage patterns and optimization effectiveness
fn benchmark_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");

    println!("Running memory efficiency benchmarks...");

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Memory pool efficiency test
    group.bench_function("memory_pool_efficiency", |b| {
        b.iter(|| {
            rt.block_on(async {
                let memory_pool = MemoryPool::new(100, 1024);

                // Allocate and deallocate buffers to test pool efficiency
                let mut buffers = Vec::new();
                for _ in 0..50 {
                    let buffer = memory_pool.acquire_buffer(1024).await.unwrap();
                    buffers.push(buffer);
                }

                for buffer in buffers {
                    memory_pool.release_buffer(buffer).await;
                }

                // Check pool statistics
                black_box(memory_pool.get_stats().await);
            });
        });
    });

    // Zero-copy effectiveness test
    group.bench_function("zero_copy_effectiveness", |b| {
        b.iter(|| {
            let large_data = vec![0u8; 64 * 1024]; // 64KB
            let bytes = Bytes::from(large_data);

            // Create multiple zero-copy buffers from the same data
            let buffers: Vec<_> = (0..10)
                .map(|_| ZeroCopyBuffer::new(bytes.clone()))
                .collect();

            black_box(buffers);
        });
    });

    group.finish();
    println!("Memory efficiency benchmarks completed.");
}

// Benchmark group definitions
criterion_group!(
    benches,
    benchmark_zero_copy_operations,
    benchmark_memory_pool_operations,
    benchmark_message_optimization,
    benchmark_batch_processing,
    benchmark_throughput_and_latency,
    benchmark_integration_scenarios,
    benchmark_memory_efficiency,
);

criterion_main!(benches);
