use crate::errors::*;
use crate::settings;
use jsonschema::{Draft, JSONSchema};
use log::*;
use std::boxed::Box;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

/// Struct that returns validation errors along with the original data to the caller.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationError {
    pub data: serde_json::Value,
    pub errors: Vec<String>,
}

/// Struct that compiles and caches schemas.
pub struct CompiledSchemas<'a> {
    schema_root: PathBuf,
    compiled: HashMap<String, JSONSchema<'a>>,
    missing: HashSet<String>,
}

impl<'a> CompiledSchemas<'a> {
    pub fn new(schema_root: PathBuf) -> Self {
        CompiledSchemas {
            schema_root,
            compiled: HashMap::new(),
            missing: HashSet::new(),
        }
    }

    pub fn get_compiled(
        &mut self,
        topic: &settings::Topic,
        message: &serde_json::Value,
    ) -> Result<&JSONSchema> {
        // fail fast if we have encountered and failed on this schema before
        if self.missing.contains(&topic.name) {
            return Err(SlipstreamError::SchemaMiss);
        }

        // compile the schema if this is the first time seeing it
        if !self.is_compiled(&topic.name) {
            let r = self.load_schema(topic, message);

            match r {
                Err(e) => {
                    self.missing.insert(topic.name.clone());
                    return Err(e);
                }
                _ => {}
            }
        }

        Ok(self.compiled.get(&topic.name).unwrap())
    }

    fn is_compiled(&self, topic_name: &str) -> bool {
        self.compiled.contains_key(topic_name)
    }

    fn load_schema(&mut self, topic: &settings::Topic, message: &serde_json::Value) -> Result<()> {
        let schema_id = try_extract_schema_id(&topic.schema, message)?;
        let text = try_read_text(&self.schema_root, schema_id)?;
        let json = try_deserialize_yaml(&text)?;

        let json = Box::new(json);
        // NOTE: leaking the box here implies the heap data for `json`
        // WILL NOT be cleaned up, even if the `CompiledSchemas` struct is dropped.
        // Unfortunately this is the only way I've found to
        // construct and cache the compiled schema because of the lifetime contraint
        // on the passed json value.
        let json: &'a mut serde_json::Value = Box::leak(json);

        let compiled = try_compile_schema(json)?;

        self.compiled.insert(topic.name.clone(), compiled);

        Ok(())
    }
}

/// Validates the given message against the given schema.
/// Returns an object containing the validated payload and errors.
pub fn validate(
    payload: serde_json::Value,
    schema: &JSONSchema,
) -> std::result::Result<serde_json::Value, ValidationError> {
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

fn try_extract_schema_id(
    schema_config: &settings::Schema,
    message: &serde_json::Value,
) -> Result<String> {
    match schema_config {
        settings::Schema::KeyType { key } => {
            let schema_id = message
                .get(key)
                .map(|v| v.as_str())
                .flatten()
                .map(|s| s.to_string());
            match schema_id {
                Some(id) => Ok(id),
                None => Err(SlipstreamError::MessageSchemaNotSet(message.clone())),
            }
        }
        settings::Schema::PathType { path } => match path.as_path().to_str() {
            Some(s) => Ok(s.to_string()),
            None => Err(SlipstreamError::SchemaPathInvalid(path.clone())),
        },
    }
}

fn try_read_text(root: &PathBuf, schema_id: String) -> Result<String> {
    let schema_path = root.join(schema_id);
    let schema_path = schema_path.as_path().to_str().unwrap();
    let schema_content = fs::read_to_string(schema_path);
    match schema_content {
        Ok(v) => Ok(v),
        Err(e) => Err(SlipstreamError::SchemaFileRead(e)),
    }
}

fn try_deserialize_yaml(yaml: &str) -> Result<serde_json::Value> {
    let value = serde_yaml::from_str::<serde_json::Value>(yaml);
    match value {
        Ok(v) => Ok(v),
        Err(e) => Err(SlipstreamError::SchemaDeserialize(e)),
    }
}

fn try_compile_schema(raw_schema: &serde_json::Value) -> Result<JSONSchema> {
    let compiled = JSONSchema::compile(&raw_schema, Some(Draft::Draft7));
    match compiled {
        Ok(c) => Ok(c),
        Err(e) => Err(SlipstreamError::SchemaCompileFailed(e)),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn compiled_schemas_returns_compiled_schema() {
        let mut compiled_schemas = CompiledSchemas::new(PathBuf::from("schemas.d"));

        let topic = settings::Topic {
            name: String::from("test"),
            schema: settings::Schema::KeyType {
                key: String::from("$id"),
            },
            routing: settings::RoutingInfo {
                valid: None,
                invalid: None,
                error: None,
            },
        };
        let message = json!({"$id": "hello.yml"});
        let result = compiled_schemas.get_compiled(&topic, &message);

        assert!(result.is_ok());
    }

    #[test]
    fn compiled_schemas_caches_schema_after_compile() {
        let mut compiled_schemas = CompiledSchemas::new(PathBuf::from("schemas.d"));

        let topic = settings::Topic {
            name: String::from("test"),
            schema: settings::Schema::KeyType {
                key: String::from("$id"),
            },
            routing: settings::RoutingInfo {
                valid: None,
                invalid: None,
                error: None,
            },
        };
        let message = json!({"$id": "hello.yml"});
        let result = compiled_schemas.get_compiled(&topic, &message);

        assert!(compiled_schemas.is_compiled("test"));
    }

    #[test]
    fn compiled_schemas_returns_file_read_error_when_no_file() {
        let mut compiled_schemas = CompiledSchemas::new(PathBuf::from("schemas.d"));

        let topic = settings::Topic {
            name: String::from("test"),
            schema: settings::Schema::PathType {
                path: PathBuf::from("not_a_file"),
            },
            routing: settings::RoutingInfo {
                valid: None,
                invalid: None,
                error: None,
            },
        };
        let message = json!({"$id": "hello.yml"});
        let result = compiled_schemas.get_compiled(&topic, &message);

        match result {
            Err(e) => match e {
                SlipstreamError::SchemaFileRead(io_err) => assert!(true),
                _ => panic!("Test result is not `FileRead` error"),
            },
            _ => panic!("Test result is not `Err`"),
        }
    }

    #[test]
    fn compiled_schemas_returns_schema_miss_error_after_first_fail() {
        let mut compiled_schemas = CompiledSchemas::new(PathBuf::from("schemas.d"));

        let topic = settings::Topic {
            name: String::from("test"),
            schema: settings::Schema::PathType {
                path: PathBuf::from("not_a_file"),
            },
            routing: settings::RoutingInfo {
                valid: None,
                invalid: None,
                error: None,
            },
        };
        let message = json!({"$id": "hello.yml"});
        let result = compiled_schemas.get_compiled(&topic, &message);
        let result = compiled_schemas.get_compiled(&topic, &message);

        match result {
            Err(e) => match e {
                SlipstreamError::SchemaMiss => assert!(true),
                _ => panic!("Test result is not `SchemaMiss` error"),
            },
            _ => panic!("Test result is not `Err`"),
        }
    }

    #[test]
    fn test_validate_message() {
        let payload = json!({"$id" : "hello.yml", "hello" : "test"});
        let schema_json = json!({
            "properties": {
                "$id": {
                    "type": "string"
                },
                "hello": {
                    "type": "string"
                }
            },
            "required": ["$id", "hello"]
        });
        let schema = JSONSchema::compile(&schema_json, Some(Draft::Draft7))
            .expect("Failed to compile JSONSchema");

        let result = validate(payload, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_message_with_invalid() {
        let payload = json!({"$id" : "hello.yml"});
        // Used for later verification without messing up ownership
        let payload_clone = payload.clone();
        let schema_json = json!({
            "properties": {
                "$id": {
                    "type": "string"
                },
                "hello": {
                    "type": "string"
                }
            },
            "required": ["$id", "hello"]

        });
        let schema = JSONSchema::compile(&schema_json, Some(Draft::Draft7))
            .expect("Failed to compile JSONSchema");

        let result = validate(payload, &schema);
        assert!(result.is_err());
        // Only expecting one validation error "'hello' is a required property"
        let err = result.unwrap_err();
        assert_eq!(err.errors.len(), 1);
        assert_eq!(err.data, payload_clone);
    }
}
