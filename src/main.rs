#[macro_use]
extern crate serde_derive;

use async_std::task;
use futures::channel::mpsc::{channel, Sender};
use futures::sink::SinkExt;
use futures::StreamExt;
use jsonschema::{Draft, JSONSchema};
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
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
 * Simple struct to carry some data back to callers on validation errors
 */
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ValidationError {
    data: serde_json::Value,
    errors: Vec<String>,
}

/**
 * Collection of named parsed JSON Schemas
 *
 * Note: these schemas have not yet been compiled!
 */
type NamedSchemas = HashMap<String, serde_json::Value>;

/// Stores topic metadata including schemas and routing info. Provides helper functions for loading
/// the desired schema and routing info from a topic name.
pub struct TopicMap {
    map: HashMap<String, settings::Topic>,
    schemas: HashMap<String, serde_json::Value>,
}

impl TopicMap {
    /// Creates a new `TopicMap`.
    pub fn new(
        topics: &[settings::Topic],
        schemas: HashMap<String, serde_json::Value>,
    ) -> TopicMap {
        let topics = topics
            .iter()
            .map(|t| (t.name.clone(), t.clone()))
            .into_iter()
            .collect();

        TopicMap {
            map: topics,
            schemas,
        }
    }

    /// Returns the schema for the given topic name and message. If the schema is `KeyType`,
    /// inspects the given message to determine the file to load.
    pub fn schema_for_topic(
        &self,
        topic_name: &str,
        message: &serde_json::Value,
    ) -> Option<&serde_json::Value> {
        let topic = self
            .map
            .get(topic_name)
            .expect(format!("No settings for {}", topic_name).as_str());

        match &topic.schema {
            settings::Schema::PathType { path } => {
                let path = path.as_path().to_str()?;
                self.schemas.get(path)
            }
            settings::Schema::KeyType { key } => {
                let schema_id = message.get(key)?.as_str()?;
                self.schemas.get(schema_id)
            }
        }
    }

    /// Returns the `RoutingInfo` for the given topic name.
    pub fn routing_for_topic(&self, topic_name: &str) -> &settings::RoutingInfo {
        let topic = self
            .map
            .get(topic_name)
            .expect(format!("No settings for {}", topic_name).as_str());

        &topic.routing
    }

    /// Returns the list of topics to subscribe to. This amounts to all topics configured in
    /// settings.
    pub fn subscription(&self) -> Vec<&str> {
        self.map.keys().map(|k| k.as_str()).collect()
    }
}

#[async_std::main]
async fn main() {
    pretty_env_logger::init();
    let settings = settings::load_settings();

    // Load schemas from directory
    let schemas = load_schemas_from(settings.schemas.clone()).expect("Failed to load schemas.d");

    // Setup a channel for the consumer to dispatch messages to
    let (sender, mut receiver) = channel::<DispatchMessage>(settings.internal.sendbuffer);

    // create a topic map to lookup schemas and routing for each consumed message.
    let topic_map = TopicMap::new(settings.topics.as_slice(), schemas);

    // Setup Kafka config to use for the consumer and producer
    let mut kafka_config: ClientConfig = ClientConfig::new();

    for (key, value) in settings.kafka.iter() {
        kafka_config.set(key, value);
    }

    // create a single consumer across all configured topics.
    let consumer: StreamConsumer = kafka_config.create().expect("Consumer creation failed");

    // run the validation stream in a separate thread
    task::spawn(validate_topics(consumer, topic_map, sender.clone()));

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

/// Subscribes the `consumer` to all topics configured in `topic_map`. Validates each received
/// message and sends it to `sender` as appropriate.
async fn validate_topics(
    consumer: StreamConsumer,
    topic_map: TopicMap,
    mut sender: Sender<DispatchMessage>,
) -> Result<(), std::io::Error> {
    // Subscribe one consumer to all topics configured in settings
    let subscription = topic_map.subscription();

    consumer
        .subscribe(subscription.as_slice())
        .expect(format!("Could not subscribe with subscription {:?}", subscription).as_str());

    let mut stream = consumer.start();

    debug!("Consuming from {:?}", subscription);

    // Handle each produced message
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

                let topic_name = message.topic();

                let schema = topic_map.schema_for_topic(topic_name, &value);
                if schema.is_none() {
                    error!(
                        "Could not load a schema, skipping message on {}",
                        message.topic()
                    );
                    continue;
                }
                let schema = schema.unwrap();
                trace!("Compiling schema: {}", schema);
                // TODO: Make this compilation checking and whatnot happen outside
                // of the message loop
                let compiled = JSONSchema::compile(&schema, Some(Draft::Draft7))
                    .expect("Failed to compile JSONSchema");

                let routing = topic_map.routing_for_topic(topic_name);

                match validate_message(value, compiled) {
                    Err(validation_error) => {
                        if let Some(invalid_topic) = &routing.invalid {
                            let destination = format_routed_topic(topic_name, invalid_topic);
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
                        if let Some(valid_topic) = &routing.valid {
                            let destination = format_routed_topic(topic_name, valid_topic);
                            let message = DispatchMessage {
                                destination,
                                payload: value.to_string(),
                            };

                            sender.send(message).await;
                        } else {
                            debug!("No valid topic defined for {}", topic_name);
                        }
                    }
                }
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
                    if let Some(ext) = file.extension() {
                        if ext == "yml" {
                            info!("Loading schema: {:?}", file);
                            let buf = fs::read_to_string(&path)
                                .unwrap_or_else(|_| panic!("Failed to read: {:?}", file));

                            let file_name = file.file_name().expect("Failed to unpack file_name()");
                            let value: serde_json::Value = serde_yaml::from_str(&buf)
                                .unwrap_or_else(|_| panic!("Failed to parse: {:?}", file));

                            let key = file_name.to_str().unwrap().to_string();
                            debug!("Inserting schema for key: {}", key);
                            // This is gross, gotta be a better way to structure this
                            schemas.insert(key, value);
                        }
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

/**
 * Validate the given message against the given schema
 */
fn validate_message(
    payload: serde_json::Value,
    schema: JSONSchema,
) -> Result<serde_json::Value, ValidationError> {
    let mut valid = false;
    let mut errors = vec![];

    match schema.validate(&payload) {
        Err(validation_errs) => {
            for error in validation_errs {
                trace!("Validation error: {}", error);
                errors.push(format!("{}", error));
            }
        }
        Ok(_) => {
            valid = true;
        }
    }

    if valid {
        return Ok(payload);
    }

    Err(ValidationError {
        data: payload,
        errors,
    })
}

/**
 * Format the routed topic name, substituting $name with the original topic where appropriate
 */
fn format_routed_topic(original: &str, routed: &str) -> String {
    // this is ugly
    routed.replace("$name", original)
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
        let settings = settings_for_test();
        let schemas = schemas_for_test();

        let topic_map = TopicMap::new(settings.topics.as_slice(), schemas.clone());

        let message = json!({"$id" : "hello.yml"});
        let expected_schema = schemas
            .get("hello.yml")
            .expect("Failed to load hello.yml named schema");

        let schema = topic_map.schema_for_topic("test", &message);

        assert!(schema.is_some());
        assert_eq!(expected_schema, schema.unwrap());
    }

    /**
     * Validate that a path-based schema reference for a topic can be found
     */
    #[test]
    fn test_topics_schema_with_path() {
        let settings = settings_for_test();
        let schemas = schemas_for_test();

        let topics: Vec<settings::Topic> = settings
            .topics
            .clone()
            .into_iter()
            .filter(|t| t.name == "other")
            .collect();

        let topic_map = TopicMap::new(topics.as_slice(), schemas.clone());
        let message = json!({});
        let expected_schema = schemas
            .get("other.yml")
            .expect("Failed to load other.yml named schema");

        let schema = topic_map.schema_for_topic("other", &message);

        assert!(schema.is_some());
        assert_eq!(expected_schema, schema.unwrap());
    }

    #[test]
    fn test_validate_message() {
        let payload = json!({"$id" : "hello.yml", "hello" : "test"});
        let schemas = schemas_for_test();
        let expected_schema = schemas
            .get("hello.yml")
            .expect("Failed to load hello.yml named schema");

        let schema = JSONSchema::compile(expected_schema, Some(Draft::Draft7))
            .expect("Failed to compile JSONSchema");

        let result = validate_message(payload, schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_message_with_invalid() {
        let payload = json!({"$id" : "hello.yml"});
        // Used for later verification without messing up ownership
        let payload_clone = payload.clone();
        let schemas = schemas_for_test();
        let expected_schema = schemas
            .get("hello.yml")
            .expect("Failed to load hello.yml named schema");

        let schema = JSONSchema::compile(expected_schema, Some(Draft::Draft7))
            .expect("Failed to compile JSONSchema");

        let result = validate_message(payload, schema);
        assert!(result.is_err());
        // Only expecting one validation error "'hello' is a required property"
        let err = result.unwrap_err();
        assert_eq!(err.errors.len(), 1);
        assert_eq!(err.data, payload_clone);
    }

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
