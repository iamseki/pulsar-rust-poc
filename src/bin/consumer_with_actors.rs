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
use pulsar::{
    consumer::Message, message::proto::command_subscribe::SubType, Consumer, Pulsar, TokioExecutor,
};
use pulsar_rust_poc::TestData;
use tokio::{
    process::Command,
    runtime::Handle,
    signal,
    sync::{mpsc, Semaphore},
    time::{sleep as sleep_tokio},
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

    enum ExecutorCommand {
        Process { msg: Message<TestData> },
    }

    enum AckerCommand {
        Ack { msg: Message<TestData> },
        Nack { msg: Message<TestData> },
    }

    // exclusive channel to process every ack/nack
    let (acker_tx, mut acker_rx) = mpsc::channel::<AckerCommand>(100);
    let acker_tx_tx_topic_test01 = acker_tx.clone();

    // ACKER THREAD
    tokio::spawn(async move {
        let addr = "pulsar://127.0.0.1:6650";
        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap(); // to do handle errors

        let mut consumer: Consumer<TestData, _> = pulsar
            .consumer()
            .with_topic("test")
            .with_topic("test-01")
            .with_consumer_name("test_consumer_acker")
            .with_subscription_type(SubType::KeyShared)
            .with_subscription("test_subscription")
            .build()
            .await
            .unwrap(); // to do handle errors

        loop {
            while let Some(cmd) = acker_rx.recv().await {
                match cmd {
                    AckerCommand::Ack { msg } => {
                        println!("ACK TOPIC => {}, message_id => {:?}", &msg.topic, msg.message_id.id);
                        consumer
                        .ack_with_id(&msg.topic, msg.message_id.id)
                        .await
                        .expect("should ack");
                    },
                    AckerCommand::Nack { msg } => {
                        println!("NACK TOPIC => {}, message_id => {:?}", &msg.topic, msg.message_id.id);
                        consumer
                        .nack_with_id(&msg.topic, msg.message_id.id)
                        .await
                        .expect("should nack")
                    },
                }
            }
        }
    });

    // exclusive channel to process topic test
    let (executor_tx, mut executor_rx) = mpsc::channel::<ExecutorCommand>(100);

    // CONSUMER TOPIC TEST
    tokio::spawn(async move {
        let addr = "pulsar://127.0.0.1:6650";
        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap(); // to do handle errors

        let mut consumer_test: Consumer<TestData, _> = pulsar
            .consumer()
            .with_topic("test")
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::KeyShared)
            .with_subscription("test_subscription")
            .with_unacked_message_resend_delay(Some(Duration::from_secs(60)))
            .build()
            .await
            .unwrap(); // to do handle errors

        loop {
            while let pulsar_msg = consumer_test.try_next().await {
                match pulsar_msg {
                    Ok(Some(pulsar_msg)) => executor_tx
                        .send(ExecutorCommand::Process { msg: pulsar_msg })
                        .await
                        .expect("to send"),
                    Ok(None) => {
                        println!("nothing to poll");
                    }
                    Err(e) => {
                        eprintln!("Error consuming message: {:?}, retrying later...", e);
                        break;
                    }
                }
            }
        }
    });

    // EXECUTOR THREAD TOPIC "TEST"
    tokio::spawn(async move {
        const MAX_CONCURRENT_THREADS: usize = 100;
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_THREADS));
        // let mut tasks = FuturesUnordered::new();

        while let Some(msg) = executor_rx.recv().await {
            println!("[EXECUTOR] reading msg");

            let sender = acker_tx.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            tokio::spawn(async move {
                match msg {
                    ExecutorCommand::Process { msg } => {
                        match msg.deserialize() {
                            Ok(data) => {
                                println!(
                                    "[EXECUTOR] processing data: {:?}, timestamp: {:?}",
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

                                sender
                                    .send(AckerCommand::Ack { msg })
                                    .await
                                    .expect("to send ack");
                            }
                            Err(e) => {
                                sender
                                    .send(AckerCommand::Nack { msg })
                                    .await
                                    .expect("to send nack");
                                return Err(Box::<dyn Error + Send + Sync>::from(format!(
                                    "Deserialization failed: {}",
                                    e
                                )));
                            }
                        };
                    }
                }
                drop(permit);
                Ok(())
            });
        }
    });

    // exclusive channel to process topic "test-01"
    let (executor_tx_test01, mut executor_rx_test01) = mpsc::channel::<ExecutorCommand>(100);

    // CONSUMER TOPIC TEST-01
    tokio::spawn(async move {
        let addr = "pulsar://127.0.0.1:6650";
        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap(); // to do handle errors

        let mut consumer_test01: Consumer<TestData, _> = pulsar
            .consumer()
            .with_topic("test-01")
            .with_consumer_name("test_consumer_test01")
            .with_subscription_type(SubType::KeyShared)
            .with_subscription("test_subscription")
            .with_unacked_message_resend_delay(Some(Duration::from_secs(60)))
            .build()
            .await
            .unwrap(); // to do handle errors

        loop {
            while let pulsar_msg = consumer_test01.try_next().await {
                match pulsar_msg {
                    Ok(Some(pulsar_msg)) => executor_tx_test01
                        .send(ExecutorCommand::Process { msg: pulsar_msg })
                        .await
                        .expect("to send"),
                    Ok(None) => {
                        println!("nothing to poll");
                    }
                    Err(e) => {
                        eprintln!("Error consuming message: {:?}, retrying later...", e);
                        break;
                    }
                }
            }
        }
    });

    // EXECUTOR THREAD TOPIC "TEST-01"
    tokio::spawn(async move {
        const MAX_CONCURRENT_THREADS: usize = 100;
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_THREADS));
        // let mut tasks = FuturesUnordered::new();

        while let Some(msg) = executor_rx_test01.recv().await {
            println!("[EXECUTOR] reading msg");

            let sender = acker_tx_tx_topic_test01.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            tokio::spawn(async move {
                match msg {
                    ExecutorCommand::Process { msg } => {
                        match msg.deserialize() {
                            Ok(data) => {
                                println!(
                                    "[EXECUTOR] processing data: {:?}, timestamp: {:?}",
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

                                sender
                                    .send(AckerCommand::Ack { msg })
                                    .await
                                    .expect("to send ack");
                            }
                            Err(e) => {
                                sender
                                    .send(AckerCommand::Nack { msg })
                                    .await
                                    .expect("to send nack");
                                return Err(Box::<dyn Error + Send + Sync>::from(format!(
                                    "Deserialization failed: {}",
                                    e
                                )));
                            }
                        };
                    }
                }
                drop(permit);
                Ok(())
            });
        }
    });


    match signal::ctrl_c().await {
        Ok(()) => {
            println!("shutting down...")
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }
    Ok(())
}
