#[macro_use]
extern crate serde_derive;

use async_std::sync::Arc;
use async_std::task;
use futures::channel::mpsc::{channel, Sender};
use futures::sink::SinkExt;
use futures::StreamExt;
use glob;
use jsonschema::{Draft, JSONSchema};
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

/**
 * A struct to carry a message over a channel to be published into Kafka
 */
#[derive(Clone, Debug)]
struct DispatchMessage {
    // Topic the message should be written to
    destination: String,
    payload: String,
}

/**
 * Collection of named parsed JSON Schemas
 *
 * Note: these schemas have not yet been compiled!
 */
type NamedSchemas = HashMap<String, serde_json::Value>;

/**
 * TopicContext will carry the necessary context into a topic consumer to enable
 * it to properly parse and route messages
 */
#[derive(Clone, Debug)]
struct TopicContext {
    topic: settings::Topic,
    settings: Arc<settings::Settings>,
    schemas: Arc<HashMap<String, serde_json::Value>>,
}

impl TopicContext {
    /**
     * Return the schema to use for validating this topic
     */
    pub fn schema_to_use(&self, message: &serde_json::Value) -> Option<&serde_json::Value> {
        match &self.topic.schema {
            settings::Schema::KeyType { key } => {
                // Fish out the right sub-value for the key
                if let Some(schema) = message.get(key) {
                    // Use the string value assuming we can get it
                    if let Some(schema) = schema.as_str() {
                        return self.schemas.get(schema);
                    }
                }
            }
            settings::Schema::PathType { path } => {
                if let Some(path_str) = path.as_path().to_str() {
                    return self.schemas.get(path_str);
                }
            }
        }
        None
    }
}

#[async_std::main]
async fn main() {
    pretty_env_logger::init();
    let settings = Arc::new(settings::load_settings());

    // Load schemas from directory
    let schemas = load_schemas_from(settings.schemas.clone()).expect("Failed to load schemas.d");

    let (sender, mut receiver) = channel::<DispatchMessage>(settings.internal.sendbuffer);
    // Creating an Arc to pass into the consumers
    let schemas = Arc::new(schemas);
    let mut kafka_config: ClientConfig = ClientConfig::new();

    for (key, value) in settings.kafka.iter() {
        kafka_config.set(key, value);
    }

    for topic in settings.topics.iter() {
        /*
         * NOTE: I don't like this but for now each topic will get its own consumer
         * connection to Kafka. While this probably isn't a bad thing, I just don't
         * like it
         */

        let consumer: StreamConsumer = kafka_config
            .clone()
            .create()
            .expect("Consumer creation failed");

        let ctx = TopicContext {
            topic: topic.clone(),
            settings: settings.clone(),
            schemas: schemas.clone(),
        };

        // Launch a consumer task for each topic
        task::spawn(consume_topic(consumer, ctx, sender.clone()));
    }

    // Need to block the main task here with something so the topic consumers don't die
    task::block_on(async move {
        let producer: FutureProducer = kafka_config.create().expect("Producer creation failed");

        while let Some(message) = receiver.next().await {
            info!("Need to dispatch to Kafka: {:?}", message);
            let record: FutureRecord<String, String> =
                FutureRecord::to(&message.destination).payload(&message.payload);
            // TODO: Make this more robust and not sequential
            producer.send(record, -1 as i64).await;
        }
    });
}

/**
 * This function starts a runloop which will consume from the given topic
 * and then send messages along to the sender channel
 */
async fn consume_topic(
    consumer: StreamConsumer,
    ctx: TopicContext,
    mut sender: Sender<DispatchMessage>,
) -> Result<(), std::io::Error> {
    let topic = &ctx.topic;

    consumer
        .subscribe(&[&topic.name])
        .expect("Could not subscribe consumer");
    let mut stream = consumer.start();
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

                // Do the schema validation

                // TODO: better error handling for non-JSON
                let value: serde_json::Value =
                    serde_json::from_str(payload).expect("Failed to deserialize message payload");

                let schema = ctx.schema_to_use(&value);

                if schema.is_none() {
                    error!(
                        "Could not load a schema, skipping message on {}",
                        topic.name
                    );
                    continue;
                }
                let schema = schema.unwrap();

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
                    }
                    Ok(_) => {
                        if let Some(valid_topic) = &topic.routing.valid {
                            // TODO: this is ugly
                            let destination = valid_topic.clone().replace("$name", &topic.name);
                            let message = DispatchMessage {
                                destination,
                                payload: payload.to_string(),
                            };

                            sender.send(message).await;
                        } else {
                            debug!("No valid topic defined for {}", topic.name);
                        }
                    }
                };
            }
        }
    }

    Ok(())
}

/**
 * Load all the .yml files which appear to be schemas in the given directory
 */
fn load_schemas_from(directory: std::path::PathBuf) -> Result<NamedSchemas, ()> {
    let mut schemas = HashMap::<String, serde_json::Value>::new();
    let pattern = format!("{}/**/*.yml", directory.display());
    debug!("Loading schemas with pattern: {}", pattern);

    let directory_path = std::path::Path::new(&directory);

    let options = glob::MatchOptions {
        case_sensitive: false,
        require_literal_separator: false,
        require_literal_leading_dot: false,
    };

    for entry in glob::glob_with(&pattern, options).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => {
                println!("{} - {}", directory_path.display(), path.display());
                if let Ok(file) = path.strip_prefix(directory_path) {
                    debug!("Considering {} as a possible schema file", file.display());
                    match file.extension() {
                        Some(ext) => {
                            if ext == "yml" {
                                info!("Loading schema: {:?}", file);
                                let buf = fs::read_to_string(&path)
                                    .expect(&format!("Failed to read {:?}", file));

                                let file_name =
                                    file.file_name().expect("Failed to unpack file_name()");
                                let value: serde_json::Value = serde_yaml::from_str(&buf)
                                    .expect(&format!("Failed to parse {:?}", file));

                                let key = file_name.to_str().unwrap().to_string();
                                debug!("Inserting schema for key: {}", key);
                                // This is gross, gotta be a better way to structure this
                                schemas.insert(key, value);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Failed to glob directory properly! {}", e);
            }
        }
    }
    Ok(schemas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schemas_for_test() -> NamedSchemas {
        load_schemas_from(std::path::PathBuf::from("schemas.d"))
            .expect("Failed to load schemas for test")
    }

    fn settings_for_test() -> settings::Settings {
        settings::load_settings()
    }

    /**
     * This test is pretty primitive, and is coupled to slipstream.yml in the
     * repository
     */
    #[test]
    fn test_load_schemas_from() {
        pretty_env_logger::init();
        let schemas = schemas_for_test();
        assert_eq!(schemas.keys().len(), 3);
    }

    /**
     * Validating that we can get the right schema to validat the topic
     * by querying the topic itself
     *
     * This is complex enough that it's bordering on an integration test, eep!
     */
    #[test]
    fn test_topics_schema() {
        let settings = Arc::new(settings_for_test());
        let schemas = Arc::new(schemas_for_test());

        let ctx = TopicContext {
            schemas: schemas.clone(),
            topic: settings.topics[0].clone(),
            settings,
        };

        let message = json!({"$id" : "hello.yml"});
        let expected_schema = schemas
            .get("hello.yml")
            .expect("Failed to load hello.yml named schema");

        let schema = ctx.schema_to_use(&message);

        assert!(schema.is_some());
        assert_eq!(expected_schema, schema.unwrap());
    }

    /**
     * Validate that a path-based schema reference for a topic can be found
     */
    #[test]
    fn test_topics_schema_with_path() {
        let settings = Arc::new(settings_for_test());
        let schemas = Arc::new(schemas_for_test());

        let topics: Vec<settings::Topic> = settings
            .topics
            .clone()
            .into_iter()
            .filter(|t| t.name == "other")
            .collect();

        let ctx = TopicContext {
            schemas: schemas.clone(),
            topic: topics[0].clone(),
            settings,
        };

        let message = json!({});
        let expected_schema = schemas
            .get("other.yml")
            .expect("Failed to load other.yml named schema");

        let schema = ctx.schema_to_use(&message);

        assert!(schema.is_some());
        assert_eq!(expected_schema, schema.unwrap());
    }
}
