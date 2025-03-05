use std::{
    error::Error,
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

//tokio-debug-console
//use console_subscriber;

use chrono::Local;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use pulsar::{message::proto::command_subscribe::SubType, Consumer, Pulsar, TokioExecutor};
use pulsar_rust_poc::TestData;
use tokio::{
    process::Command,
    runtime::Handle,
    sync::{Mutex, Semaphore},
    time::{sleep as sleep_tokio, timeout},
};

use sysinfo::System;

/// print system info before start benchmarking
async fn print_system_info() {
    // Get system info
    let sys = System::new_all();

    // tokio runtime metrics
    let rt = Handle::current();
    let metrics = rt.metrics();

    // Get Rust version safely
    let rust_version = Command::new("rustc")
        .arg("--version")
        .output()
        .await
        .unwrap();
    let rust_version_str = String::from_utf8_lossy(&rust_version.stdout);

    println!(
        "=== System Info ===:
    OS: {:?}
    Kernel Version: {:?}
    Rust Version: {}
    Total Memory: {} GB
    Total Swap: {} GB
    CPU Brand: {:?}
    CPUs: {:?}
    Tokio Executor Threads: {:?} 
=== System Info ===",
        System::long_os_version().unwrap(),
        System::kernel_version().unwrap(),
        rust_version_str.trim(),
        sys.total_memory() / (1024 * 1024 * 1024),
        sys.total_swap() / (1024 * 1024 * 1024),
        sys.cpus().len(),
        sys.cpus().first().unwrap().brand(),
        metrics.num_workers(),
    );
}

/// This example demonstrates a Pulsar consumer with the following properties:
///
/// **Bounded processing:** The Tokio runtime must control the level of parallelism to prevent unbounded work.
///
/// **Batch processing:** The consumer should process N messages at a time with a timeout configuration, allowing for batch acknowledgment (ack) or negative acknowledgment (nack).
///
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_system_info().await;

    // tokio-debug-console
    // console_subscriber::init();

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;

    let consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic("test")
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::KeyShared)
        .with_subscription("test_subscription")
        .with_unacked_message_resend_delay(Some(Duration::from_secs(60)))
        .build()
        .await?;

    let consumer = Arc::new(Mutex::new(consumer));
    
    const BATCH_SIZE: usize = 10000;
    const TIMEOUT_MS: Duration = Duration::from_millis(2000);
    const MAX_CONCURRENT_THREADS: usize = 100;

    let mut batch = Vec::with_capacity(BATCH_SIZE);

    loop {
        let consumer = consumer.clone();
        let before = Instant::now();

        let _ = timeout(TIMEOUT_MS, async {
            while batch.len() < BATCH_SIZE {
                match consumer.lock().await.try_next().await {
                    Ok(Some(msg)) => {
                        batch.push(msg);
                    }
                    Ok(None) => {
                        println!("nothing to poll");
                    }
                    Err(e) => {
                        eprintln!("Error consuming message: {:?}, retrying later...", e);
                        break;
                    }
                }
            }
        })
        .await;

        println!("processing batch len: {}", batch.len());

        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_THREADS));
        //let mut tasks = Vec::with_capacity(BATCH_SIZE);
        let mut tasks = FuturesUnordered::new();

        let mut tasks_processed = 0;

        for msg in batch.drain(..) {
            tasks_processed += 1;

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let consumer = consumer.clone();

            tasks.push(tokio::spawn(async move {
                let data = match msg.deserialize() {
                    Ok(data) => data,
                    Err(e) => {
                        consumer.lock().await.nack(&msg).await?;
                        return Err(Box::<dyn Error + Send + Sync>::from(format!("Deserialization failed: {}", e)));
                    }
                };
                println!(
                    "processing data: {:?}, timestamp: {:?}",
                    data,
                    Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                );

                // All the algorithm needs to do is:
                // 1. Fetch dependent Data (mostly I/O)
                // 2. Calculate Operations (CPU)
                // 3. Calculate KPIs (CPU)
                // 4. Write it into the database (mostly I/O)

                // Simulate some I/O work
                sleep_tokio(Duration::from_millis(100)).await;

                // Simulate some CPU work
                sleep(Duration::from_millis(10));

                let mut consumer = consumer.lock().await;

                consumer
                    .ack(&msg)
                    .await
                    .map_err(|e| format!("ack failed: {}", e))?;
                println!("Consumed successfully");

                drop(permit);
                Ok(())
            }));
        }

        // future::join_all(tasks).await;
        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(())) => {}                                   // Successfully processed message
                Ok(Err(err)) => eprintln!("Task failed: {}", err), // Custom error message
                Err(join_err) => eprintln!("Task panicked: {:?}", join_err), // Panic case
            }
        }

        let elapsed = before.elapsed();
        if tasks_processed == 0 {
            println!(
                "{:?} total, for tasks_processed {:?}",
                elapsed, tasks_processed
            );
        } else {
            println!(
                "{:?} total, {:?} avg per iteration for tasks_processed {:?}",
                elapsed,
                elapsed / tasks_processed as u32,
                tasks_processed
            );
        }
    }
}
