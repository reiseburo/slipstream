use crate::schemas;
use crate::settings;
use futures::channel::mpsc::{Receiver, Sender};
use futures::sink::SinkExt;
use futures::StreamExt;
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::path::PathBuf;

/// A struct to carry a message over a channel to be published into a destination Kafka topic.
#[derive(Clone, Debug)]
pub struct DispatchMessage {
    destination: String,
    payload: String,
}

pub async fn consume_topic(
    kafka_settings: HashMap<String, String>,
    topic: settings::Topic,
    schema_root: PathBuf,
    mut sender: Sender<DispatchMessage>,
) -> Result<(), std::io::Error> {
    let kafka_config = create_config(&kafka_settings);

    let consumer: StreamConsumer = kafka_config.create().expect("Consumer creation failed");
    consumer.subscribe(&[&topic.name]);

    let mut stream = consumer.start();
    let mut compiled_schemas = schemas::CompiledSchemas::new(schema_root);

    debug!("Consuming from {}", topic.name);

    while let Some(message) = stream.next().await {
        match message {
            Err(e) => {
                error!("Failed to consume: {}", e);
            }
            Ok(message) => {
                // TODO: might as well turn payload into an OwnedMessage since we need to copy it
                // around a couple times anyways
                let payload = match message.payload_view::<str>() {
                    None => "",
                    Some(Ok(buf)) => buf,
                    Some(Err(e)) => {
                        error!("Could not parse message: {:?}", e);
                        ""
                    }
                };
                debug!("Message consumed: {:?}", payload);

                let value: Result<serde_json::Value, serde_json::error::Error> =
                    serde_json::from_str(payload);
                if value.is_err() {
                    debug!("Non-JSON payload received: {:?}", payload);
                    continue;
                }
                let value = value.unwrap();

                let compiled_schema = compiled_schemas.get_compiled(&topic, &value);

                match compiled_schema {
                    Ok(schema) => {
                        let validated = schemas::validate(value, schema);

                        match validated {
                            Err(validation_error) => {
                                if let Some(invalid_topic) = &topic.routing.invalid {
                                    let destination =
                                        format_routed_topic(&topic.name, invalid_topic);

                                    match serde_json::to_string(&validation_error) {
                                        Ok(payload) => {
                                            let message = DispatchMessage {
                                                destination,
                                                payload,
                                            };
                                            sender.send(message).await;
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to serialize a validation error to JSON: {:?}",
                                                e
                                            );
                                        }
                                    }
                                }
                            }

                            Ok(value) => {
                                if let Some(valid_topic) = &topic.routing.valid {
                                    let destination = format_routed_topic(&topic.name, valid_topic);
                                    let message = DispatchMessage {
                                        destination,
                                        payload: value.to_string(),
                                    };

                                    sender.send(message).await;
                                } else {
                                    debug!("No valid topic defined for {}", topic.name);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Error getting compiled schema. Topic: {:?}. Error: {:?}",
                            &topic, e
                        );
                        continue;
                    }
                }
            }
        }
    }
    Ok(())
}

fn create_config(kafka_settings: &HashMap<String, String>) -> ClientConfig {
    let mut kafka_config: ClientConfig = ClientConfig::new();

    for (key, value) in kafka_settings.iter() {
        kafka_config.set(key, value);
    }

    kafka_config
}

/**
 * Format the routed topic name, substituting $name with the original topic where appropriate.
 */
fn format_routed_topic(original: &str, routed: &str) -> String {
    // this is ugly
    routed.replace("$name", original)
}

pub async fn produce(
    kafka_settings: HashMap<String, String>,
    mut receiver: Receiver<DispatchMessage>,
) {
    let kafka_config = create_config(&kafka_settings);
    let producer: FutureProducer = kafka_config.create().expect("Producer creation failed");

    while let Some(message) = receiver.next().await {
        info!("Need to dispatch to Kafka: {:?}", message);
        let record: FutureRecord<String, String> =
            FutureRecord::to(&message.destination).payload(&message.payload);
        // TODO: Make this more robust and not sequential
        producer.send(record, -1 as i64);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /**
     * Test that the format_routed_topic() properly replaces $name characters
     * with the original topic name
     */
    #[test]
    fn test_format_routed_topic() {
        let original = "test";
        let valid = "$name-valid";
        let invalid = "$name-invalid";

        assert_eq!(format_routed_topic(&original, valid), "test-valid");
        assert_eq!(format_routed_topic(&original, invalid), "test-invalid");
    }
}
