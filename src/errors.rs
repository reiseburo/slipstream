use std::fmt;

pub type Result<T> = std::result::Result<T, SlipstreamError>;

#[derive(Debug)]
pub enum SlipstreamError {
    SchemaMiss,
    TopicSchemaNotSet,
    MessageSchemaNotSet(serde_json::Value),
    SchemaPathInvalid(std::path::PathBuf),
    SchemaFileRead(std::io::Error),
    SchemaDeserialize(serde_yaml::Error),
    SchemaCompileFailed(jsonschema::CompilationError),
}
