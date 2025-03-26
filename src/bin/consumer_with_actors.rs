//tokio-debug-console
//use console_subscriber;

use pulsar::{Pulsar, TokioExecutor};
use pulsar_rust_poc::actors;
use tokio::{process::Command, runtime::Handle, signal};

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

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap(); // to do handle errors

    // init acker task
    let acker_handle =
        actors::AckerHandle::new(&pulsar, vec!["test".to_string(), "test-01".to_string()]).await;

    let acker_tx_test = acker_handle.acker_tx.clone();

    // actors to receive and process topic "test"
    // init executor task
    let executor_handle = actors::ExecutorHandle::new(acker_tx_test).await;
    let executor_tx_test = executor_handle.executor_tx.clone();

    let mut receiver_topic_test =
        actors::Receiver::new(&pulsar, "test".to_string(), executor_tx_test).await;
    // since there is no channels initialized in the consumer actor, its unecessary to create a handle so just init receiver task
    tokio::spawn(async move { receiver_topic_test.consume().await });

    let acker_tx_test01 = acker_handle.acker_tx.clone();
    // actors to receive and process topic "test-01"
    // init executor task
    let executor_handle_test01 = actors::ExecutorHandle::new(acker_tx_test01).await;
    let executor_tx_test01 = executor_handle_test01.executor_tx.clone();

    let mut receiver_topic_test01 =
        actors::Receiver::new(&pulsar, "test-01".to_string(), executor_tx_test01).await;
    // since there is no channels initialized in the consumer actor, its unecessary to create a handle so just init receiver task
    tokio::spawn(async move { receiver_topic_test01.consume().await });

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
