use std::{
    sync::Arc,
    time::{Duration, Instant},
};

//#[cfg "tokio-debug-console")]
//use console_subscriber;

use chrono::Local;
use futures::{future, TryStreamExt, stream::{ FuturesUnordered }};
use pulsar::{message::proto::command_subscribe::SubType, Consumer, Pulsar, TokioExecutor};
use pulsar_rust_poc::TestData;
use tokio::{
    process::Command,
    runtime::Handle,
    sync::{Mutex, Semaphore},
    time::{sleep, timeout},
};

use sysinfo::System; // Import system info

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

/// This example is a Pulsar consumer that should have the following properties:
///
/// **Not unbounded processing work:** Which means we have to be able to control how much work is done in parallel by the Tokio runtime.
///
/// **Batching processing:** Which means that we can pick N messages and processing them at a time including be able to ack or nack the messages.
///
/// Non function requirements are:
///
/// - Acceptable thorughput not only the processing part but also the ack/nack part as well
/// - Efficient memory usage
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_system_info().await;

    // #[cfg(feature = "tokio-debug-console")]
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

    // Wrap the consumer in an Arc<Mutex> to share across tasks
    let consumer = Arc::new(Mutex::new(consumer));

    const BATCH_SIZE: usize = 10000;
    const TIMEOUT_MS: Duration = Duration::from_millis(2000);
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
                        eprintln!("Error consuming message: {:?}", e);
                        break;
                    }
                }
            }
        })
        .await;

        println!("processing batch len: {}", batch.len());

        let semaphore = Arc::new(Semaphore::new(100));
        let mut tasks = Vec::with_capacity(BATCH_SIZE);
        //let mut tasks = FuturesUnordered::new();

        let mut tasks_processed = 0;

        for msg in batch.drain(..) {
            tasks_processed += 1;

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let consumer = consumer.clone();

            tasks.push(tokio::spawn(async move {
                let data = msg.deserialize();
                match data {
                    Ok(m) => println!(
                        "processing data: {:?}, timestamp: {:?}",
                        m,
                        Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                    ),
                    Err(_) => println!("error deserializing"),
                }

                sleep(Duration::from_millis(200)).await;

                let mut consumer = consumer.lock().await;

                match consumer.ack(&msg).await {
                    Ok(_) => println!("Consumed successfully"),
                    Err(ack_err) => println!("Error: {:?}", ack_err),
                }

                drop(permit);
            }));
        }

        future::join_all(tasks).await;

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
