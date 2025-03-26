mod receiver;
pub use receiver::*;

mod executor;
pub use executor::*;

mod acker;
pub use acker::*;

use crate::TestData;

use pulsar::consumer::Message;

pub enum ExecutorCommand {
  Process { msg: Message<TestData> },
}

pub enum AckerCommand {
  Ack { msg: Message<TestData> },
  Nack { msg: Message<TestData> },
}


pub use receiver::Receiver;
