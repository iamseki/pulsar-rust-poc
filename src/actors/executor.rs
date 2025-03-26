use std::error::Error;
use std::time::Duration;
use std::{sync::Arc, thread::sleep};

use chrono::Local;
use tokio::sync::{mpsc, Semaphore};

use tokio::time::{sleep as sleep_tokio};

use super::{Acker, AckerCommand, ExecutorCommand};

pub struct Executor {
    max_concurrency: usize,
    acker_tx: mpsc::Sender<AckerCommand>,
    executor_rx: mpsc::Receiver<ExecutorCommand>,
}

impl Executor {
    pub fn new(
        acker_tx: mpsc::Sender<AckerCommand>,
        executor_rx: mpsc::Receiver<ExecutorCommand>,
    ) -> Self {
        Self {
            acker_tx,
            executor_rx,
            // TODO
            max_concurrency: 100,
        }
    }

    pub async fn process(&mut self) {
        let semaphore = Arc::new(Semaphore::new(self.max_concurrency));

        while let Some(msg) = self.executor_rx.recv().await {
            println!("[EXECUTOR] reading msg");

            let sender = self.acker_tx.clone();
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
    }
}


pub struct ExecutorHandle {
  pub executor_tx: mpsc::Sender<ExecutorCommand>,
}

impl ExecutorHandle {
    pub async fn new(acker_tx: mpsc::Sender<AckerCommand>) -> Self {
        let (sender, receiver) = mpsc::channel(1000);
        let mut actor = Executor::new(acker_tx, receiver);
        tokio::spawn(async move { actor.process().await });

        Self { executor_tx: sender }
    }
}
