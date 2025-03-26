use std::time::Duration;

use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};
use tokio::sync::mpsc;

use crate::TestData;

use super::ExecutorCommand;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};

/// Actor responsible to read data from a topic
/// 
/// creating a channel and returning the rx channel to be use by the executor actor
/// 
/// send the data to the transmiter channel
/// 
/// TODO IMPROVE ABSTRACTION
pub struct Receiver {
  pulsar_consumer: Consumer<TestData, TokioExecutor>,
  executor_tx: mpsc::Sender<ExecutorCommand>
}

impl Receiver {
    pub async fn new(pulsar_client: &Pulsar<TokioExecutor>, topic: String, executor_tx: mpsc::Sender<ExecutorCommand>) -> Self {
        let pulsar_consumer: Consumer<TestData, _> = pulsar_client
        .consumer()
        .with_topic(&topic)
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::KeyShared)
        .with_subscription("test_subscription")
        .with_unacked_message_resend_delay(Some(Duration::from_secs(60)))
        .build()
        .await
        .unwrap(); // to do handle errors

        Self {
            pulsar_consumer,
            executor_tx
        }
    }

    /// Consume will consume messages from pulsar indefinetly
    pub async fn consume(&mut self) {
      loop {
        while let pulsar_msg = self.pulsar_consumer.try_next().await {
            match pulsar_msg {
                Ok(Some(pulsar_msg)) => self.executor_tx
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
    }

}