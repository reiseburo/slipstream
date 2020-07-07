
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
    kafka: Kafka,
    schemas: PathBuf,
    topics: Vec<Topic>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Kafka {
    brokers: Vec<String>,
    configuration: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Topic {
    name: String,
    schema: SchemaType,
    routing: RoutingInfo, 
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaType {
    key: Option<String>,
    path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutingInfo {
    valid: Option<String>,
    invalid: Option<String>,
    error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_settings() {
        let settings = load_settings();
        assert_eq!(settings.kafka.brokers[0], "localhost:9092");
        let group = settings.kafka.configuration.get("group.id")
            .expect("Failed to look up the group.id");
        assert_eq!(group, "slipstream");
    }
}
