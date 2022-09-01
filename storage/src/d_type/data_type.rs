use serde::{Deserialize, Serialize};

/// SQL data types
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Integer i32
    Integer = 1,

    /// Long Integer i64
    Long = 2,

    /// Floating point f32
    Float = 3,

    /// Double e.g. f64
    Double = 4,

    /// Fixed-length character type e.g. CHAR(10)
    Char = 6,

    /// Variable-length character type e.g. VARCHAR(10)
    VarChar = 5,
}
