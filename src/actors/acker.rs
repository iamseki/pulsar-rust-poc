use pulsar::{Consumer, Pulsar, SubType, TokioExecutor};
use tokio::sync::mpsc;

use crate::TestData;

use super::AckerCommand;

pub struct Acker {
    pulsar_consumer: Consumer<TestData, TokioExecutor>,
    acker_rx: mpsc::Receiver<AckerCommand>,
}

impl Acker {
    pub async fn new(
        pulsar_client: &Pulsar<TokioExecutor>,
        acker_rx: mpsc::Receiver<AckerCommand>,
        topics: Vec<String>,
    ) -> Self {
        let pulsar_consumer: Consumer<TestData, TokioExecutor> = pulsar_client
            .consumer()
            .with_topics(topics)
            //.with_topic("test")
            //.with_topic("test-01")
            .with_consumer_name("test_consumer_acker")
            .with_subscription_type(SubType::KeyShared)
            .with_subscription("test_subscription")
            .build()
            .await
            .unwrap(); // to do handle errors

        Self {
            pulsar_consumer,
            acker_rx,
        }
    }

    pub async fn handle_msg(&mut self) {
        loop {
            while let Some(cmd) = self.acker_rx.recv().await {
                match cmd {
                    AckerCommand::Ack { msg } => {
                        println!(
                            "ACK TOPIC => {}, message_id => {:?}",
                            &msg.topic, msg.message_id.id
                        );
                        self.pulsar_consumer
                            .ack_with_id(&msg.topic, msg.message_id.id)
                            .await
                            .expect("should ack");
                    }
                    AckerCommand::Nack { msg } => {
                        println!(
                            "NACK TOPIC => {}, message_id => {:?}",
                            &msg.topic, msg.message_id.id
                        );
                        self.pulsar_consumer
                            .nack_with_id(&msg.topic, msg.message_id.id)
                            .await
                            .expect("should nack")
                    }
                }
            }
        }
    }
}

pub struct AckerHandle {
  pub acker_tx: mpsc::Sender<AckerCommand>,
}

impl AckerHandle {
    pub async fn new(pulsar_client: &Pulsar<TokioExecutor>, topics: Vec<String>) -> Self {
        let (sender, receiver) = mpsc::channel(1000);
        let mut actor = Acker::new(pulsar_client, receiver, topics).await;
        tokio::spawn(async move { actor.handle_msg().await });

        Self { acker_tx: sender }
    }
}
