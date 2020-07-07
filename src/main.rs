#[macro_use]
extern crate serde_derive;

use async_std::sync::Arc;
use async_std::task;
use futures::channel::mpsc::{channel, Sender};
use futures::StreamExt;
use futures::sink::SinkExt;
use jsonschema::{JSONSchema, Draft};
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json;
use serde_yaml;
use std::collections::HashMap;
use std::fs;

mod settings;

#[derive(Debug)]
struct DispatchMessage {
    // Topic the message should be written to
    destination: String,
    payload: String,
}

#[async_std::main]
async fn main() {
    pretty_env_logger::init();
    let settings = Arc::new(settings::load_settings());

    // Load schemas from directory
    let mut schemas = HashMap::<String, serde_json::Value>::new();
    let schemas_d = fs::read_dir(settings.schemas.clone())
        .expect(&format!("Failed to read directory: {:?}", settings.schemas));

    for file in schemas_d {
        if let Ok(file) = file {
            let file = file.path();
            match file.extension() {
                Some(ext) => {
                    if ext == "yml" {
                        info!("Loading schema: {:?}", file);
                        let buf = fs::read_to_string(&file)
                            .expect(&format!("Failed to read {:?}", file));

                        let file_name = file.file_name().expect("Failed to unpack file_name()");
                        let value: serde_json::Value = serde_yaml::from_str(&buf)
                            .expect(&format!("Failed to parse {:?}", file));

                        let key = file_name.to_str().unwrap().to_string();
                        debug!("Inserting schema for key: {}", key);
                        // This is gross, gotta be a better way to structure this
                        schemas.insert(key, value);
                    }
                },
                _ => {},
            }
        }
    }

    // XXX: fix this magic number and make the channel size configurable
    let (sender, mut receiver) = channel::<DispatchMessage>(1024);
    // Creating an Arc to pass into the consumers
    let schemas = Arc::new(schemas);
    let brokers = settings.kafka.brokers.join(",");
    let mut kafka_config: ClientConfig = ClientConfig::new();
    kafka_config.set("bootstrap.servers", &brokers);

    for (key, value) in settings.kafka.configuration.iter() {
        kafka_config.set(key, value);
    }

    for topic in settings.topics.iter() {
        /*
         * NOTE: I don't like this but for now each topic will get its own consumer
         * connection to Kafka. While this probably isn't a bad thing, I just don't
         * like it
         */

        let consumer: StreamConsumer = kafka_config.clone()
            .create()
            .expect("Consumer creation failed");

        // Launch a consumer task for each topic
        task::spawn(consume_topic(consumer, topic.clone(), schemas.clone(), sender.clone()));
    }

    // Need to block the main task here with something so the topic consumers don't die
    task::block_on(async move {
        let producer: FutureProducer = kafka_config.create()
            .expect("Producer creation failed");

        while let Some(message) = receiver.next().await {
            info!("Need to dispatch to Kafka: {:?}", message);
            let record: FutureRecord<String, String> = FutureRecord::to(&message.destination)
                .payload(&message.payload);
            // TODO: Make this more robust and not sequential
            producer.send(record, -1 as i64).await;
        }
    });
}


/**
 * This function starts a runloop which will consume from the given topic
 * and then send messages along to the sender channel
 */
async fn consume_topic(consumer: StreamConsumer, topic: settings::Topic, schemas: Arc<HashMap<String, serde_json::Value>>, mut sender: Sender<DispatchMessage>) -> Result<(), std::io::Error> {

    consumer.subscribe(&[&topic.name])
        .expect("Could not subscribe consumer");
    let mut stream = consumer.start();
    debug!("Consuming from {}", topic.name);

    while let Some(message) = stream.next().await {
        match message {
            Err(e) => {
                error!("Failed to consume: {}", e);
            },
            Ok(message) => {
                // TODO: might as well turn payload into an OwnedMessage since we need to copy it
                // around a couple times anyways
                let payload = match message.payload_view::<str>() {
                    None => "",
                    Some(Ok(buf)) => buf,
                    Some(Err(e)) => {
                        error!("Could not parse message: {:?}", e);
                        ""
                    },
                };
                debug!("Message consumed: {:?}", payload);

                // Do the schema validation

                // TODO: better error handling for non-JSON
                let value: serde_json::Value = serde_json::from_str(payload)
                    .expect("Failed to deserialize message payload");

                // TODO: properly handle the different types of schema definitions
                // from the configuration file
                let schema = value.get("$schema")
                    .expect("Message had no $schema");
                // TODO: make this safer
                let schema = schemas.get(schema.as_str().unwrap())
                    .expect("Unknown schema defined");
                trace!("Compiling schema: {}", schema);

                // TODO: Make this compilation checking and whatnot happen outside
                // of the message loop
                let compiled = JSONSchema::compile(&schema, Some(Draft::Draft7))
                    .expect("Failed to compile JSONSchema");

                match compiled.validate(&value) {
                    Err(errors) => {
                        for error in errors {
                            warn!("Validation error: {}", error)
                        }
                        // TODO: Include the validation errors in the JSON output message
                        if let Some(invalid_topic) = &topic.routing.invalid {
                            // TODO: this is ugly
                            let destination = invalid_topic.clone().replace("$name", &topic.name);
                            let message = DispatchMessage {
                                destination,
                                payload: payload.to_string(),
                            };

                            sender.send(message).await;
                        }
                    },
                    Ok(_) => {
                        if let Some(valid_topic) = &topic.routing.valid {
                            // TODO: this is ugly
                            let destination = valid_topic.clone().replace("$name", &topic.name);
                            let message = DispatchMessage {
                                destination,
                                payload: payload.to_string(),
                            };

                            sender.send(message).await;
                        }
                        else {
                            debug!("No valid topic defined for {}", topic.name);
                        }
                    },
                };
            },
        }
    }

    Ok(())
}
