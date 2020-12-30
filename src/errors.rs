use std::error;
use std::fmt;

pub type Result<T> = std::result::Result<T, SlipstreamError>;

#[derive(Debug)]
pub enum SlipstreamError {
    SchemaMiss,
    MessageSchemaNotSet(serde_json::Value),
    SchemaPathInvalid(std::path::PathBuf),
    SchemaFileRead(std::io::Error),
    SchemaDeserialize(serde_yaml::Error),
    SchemaCompileFailed(jsonschema::CompilationError),
}

impl fmt::Display for SlipstreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SlipstreamError::SchemaMiss => write!(
                f,
                "Schema is not contained in cache due to a previous error."
            ),
            SlipstreamError::MessageSchemaNotSet(json) => {
                write!(f, "Schema not set in message {:?}", json)
            }
            SlipstreamError::SchemaPathInvalid(path_buf) => {
                write!(f, "Schema path is not valid {:?}", path_buf)
            }
            SlipstreamError::SchemaFileRead(io_error) => {
                write!(f, "File read error when reading schema {:?}", io_error)
            }
            SlipstreamError::SchemaDeserialize(yaml_error) => {
                write!(f, "Error deserializing schema {:?}", yaml_error)
            }
            SlipstreamError::SchemaCompileFailed(compilation_error) => {
                write!(f, "Failed to compile schema {:?}", compilation_error)
            }
        }
    }
}

impl error::Error for SlipstreamError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            SlipstreamError::SchemaFileRead(io_error) => Some(io_error),
            SlipstreamError::SchemaDeserialize(yaml_error) => Some(yaml_error),
            SlipstreamError::SchemaCompileFailed(compilation_error) => Some(compilation_error),
            _ => None,
        }
    }
}

impl From<jsonschema::CompilationError> for SlipstreamError {
    fn from(error: jsonschema::CompilationError) -> SlipstreamError {
        SlipstreamError::SchemaCompileFailed(error)
    }
}

impl From<serde_yaml::Error> for SlipstreamError {
    fn from(error: serde_yaml::Error) -> SlipstreamError {
        SlipstreamError::SchemaDeserialize(error)
    }
}

impl From<std::io::Error> for SlipstreamError {
    fn from(error: std::io::Error) -> SlipstreamError {
        SlipstreamError::SchemaFileRead(error)
    }
}
