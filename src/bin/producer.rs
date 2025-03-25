use futures::future::join_all;
use pulsar::{producer, Pulsar, TokioExecutor};

use pulsar_rust_poc::TestData;

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    let mut producer = pulsar
        .producer()
        .with_topic("test")
        .with_name("my-producer".to_string())
        .with_options(producer::ProducerOptions {
            batch_size: Some(1000),
            ..Default::default()
        })
        .build()
        .await?;

    let mut v = Vec::new();

    for i in 0..2000 {
        let parition_key = if i % 2 == 0 {
            "10".to_string()
        } else {
            "7".to_string()
        };

        let receipt_rx = producer
            .send_non_blocking(TestData {
                data: "data".to_string(),
                partition_key: parition_key,
            })
            .await
            .unwrap();
        v.push(receipt_rx);
    }

    let mut producer = pulsar
        .producer()
        .with_topic("test-01")
        .with_name("my-producer".to_string())
        .with_options(producer::ProducerOptions {
            batch_size: Some(1000),
            ..Default::default()
        })
        .build()
        .await?;

    let mut v = Vec::new();

    for i in 0..2000 {
        let parition_key = if i % 2 == 0 {
            "10".to_string()
        } else {
            "7".to_string()
        };

        let receipt_rx = producer
            .send_non_blocking(TestData {
                data: "data".to_string(),
                partition_key: parition_key,
            })
            .await
            .unwrap();
        v.push(receipt_rx);
    }

    println!(
        "receipts: {:?}",
        join_all(v)
            .await
            .iter()
            .map(|v| match v {
                Ok(command) => command.producer_id.to_string(),
                Err(err) => err.to_string(),
            })
            .collect::<Vec<String>>()
    );

    Ok(())
}
