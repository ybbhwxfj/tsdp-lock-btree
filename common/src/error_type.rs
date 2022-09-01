use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ET {
    TODO,
    TodoInfo(String),
    RaftEmptyLogEntry,
    RaftIndexOutOfRange,
    RaftCannotFindId,
    ChSendError,
    ChRecvError,
    ErrorLength,
    ErrorType,
    ExistingSuchElement,

    NoSuchElement,
    NoneOption,
    ExceedCapacity,
    OutOffIndex,
    CorruptLog,
    SenderError(String),
    TokioRecvError(String),
    EOF,
    IOError(String),
    JSONError(String),
    SerdeError(String),
    CSVError(String),
    ParseError(String),
    FatalError(String),
    TxConflict,
    ErrorCursor,
    Deadlock,
    Timeout,
    SQLParseError(String),
    DebugNonLeafUpsertNone,
    /// if the newly updated key on non leaf key would violate key order,
    /// we would change the update by a deletion and a insertion
    PageUpdateErrorHighKeySlot,
    PageUpdateErrorNoSuchKey,
    PageUpdateErrorExistingSuchKey,
}

unsafe impl Send for ET {}

unsafe impl Sync for ET {}

impl fmt::Display for ET {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
