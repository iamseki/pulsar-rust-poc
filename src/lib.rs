#[macro_use]
extern crate serde;

use pulsar::{
    message::Payload,
    producer, DeserializeMessage, Error as PulsarError, SerializeMessage
};

#[derive(Debug, Serialize, Deserialize)]
pub struct TestData {
    pub data: String,
    pub partition_key: String
}

impl SerializeMessage for TestData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            partition_key: Some(input.partition_key),
            ..Default::default()
        })
    }
}

impl DeserializeMessage for TestData {
    type Output = Result<TestData, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}
