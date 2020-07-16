
use std::collections::HashMap;
use std::path::PathBuf;

/**
* Load the settings defined in the $PWD/slipstream.yml file
*/
pub fn load_settings() -> Settings {
    let mut settings = config::Config::default();
    settings.merge(config::File::with_name("slipstream.yml"))
        .expect("Failed to load configuration from `slipstream.yml`");

    settings.try_into()
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
    pub schemas: PathBuf,
    pub topics: Vec<Topic>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    pub name: String,
    pub schema: SchemaType,
    pub routing: RoutingInfo,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaType {
    pub key: Option<String>,
    pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingInfo {
    pub valid: Option<String>,
    pub invalid: Option<String>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_settings() {
        let settings = load_settings();
        let brokers = settings.kafka.get("bootstrap.servers")
            .expect("Failed to look up the bootstrap.servers");
        assert_eq!(brokers, "localhost:9092");
        let group = settings.kafka.get("group.id")
            .expect("Failed to look up the group.id");
        assert_eq!(group, "slipstream");
    }
}
