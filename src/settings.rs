use std::collections::HashMap;
use std::path::PathBuf;

/**
* Load the settings defined in the $PWD/slipstream.yml file
*/
pub fn load_settings() -> Settings {
    let mut settings = config::Config::default();
    settings
        .merge(config::File::with_name("slipstream.yml"))
        .expect("Failed to load configuration from `slipstream.yml`");

    settings
        .try_into()
        .expect("Failed to coerce configuration into our internal structures")
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /**
     * "Raw" Kafka configuration to be passed verbatim to the librdkafka
     * ClientConfig
     */
    pub kafka: HashMap<String, String>,
    /**
     * Internal settings that most users shouldn't ever need to tweak
     */
    pub internal: Internal,
    pub schemas: PathBuf,
    pub topics: Vec<Topic>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Internal {
    #[serde(default = "default_buffer_size")]
    pub sendbuffer: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    pub name: String,
    pub schema: Schema,
    pub routing: RoutingInfo,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
pub enum Schema {
    /**
     * A Key-based schema relies on the Kafka message to have a build in JSON key
     * at the _root level_ which defines the path to the JSON Schema
     */
    KeyType { key: String },
    /**
     * A Path-based schema defines a schema that should be applied from outside the
     * message content itself, i.e. the message needn't be self-describing.
     */
    PathType { path: PathBuf },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingInfo {
    pub valid: Option<String>,
    pub invalid: Option<String>,
    pub error: Option<String>,
}

/**
 * Returns the default buffer size for internal messaging within slipstream
 */
fn default_buffer_size() -> usize {
    1024
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_settings() {
        let settings = load_settings();
        let brokers = settings
            .kafka
            .get("bootstrap.servers")
            .expect("Failed to look up the bootstrap.servers");
        assert_eq!(brokers, "localhost:9092");
        let group = settings
            .kafka
            .get("group.id")
            .expect("Failed to look up the group.id");
        assert_eq!(group, "slipstream");
    }
}
