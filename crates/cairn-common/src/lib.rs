//! Shared SQL values, schema and PostgreSQL error types.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub kind: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: usize,
}

impl Column {
    pub fn oid(&self) -> i32 {
        match self.kind.as_str() {
            "bigint" => 20,
            "integer" => 23,
            "smallint" => 21,
            "boolean" => 16,
            _ => 25,
        }
    }
}

#[derive(Debug)]
pub struct DbError {
    pub code: &'static str,
    pub message: String,
}

impl DbError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new("0A000", message)
    }
}
impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for DbError {}
pub type Result<T> = std::result::Result<T, DbError>;
