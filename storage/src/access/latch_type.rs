use serde::Serialize;
use std::fmt::{Display, Formatter};

/// |  |                      | AI | ND | RL | WL | PM |
/// |--|----------------------|----|----|----|----|----|
/// |E | AccessIntent         | Y  | N  | Y  | Y  | Y  |
/// |X | NodeDelete           |N/A |N/A | Y  | Y  | Y  |
/// |I | ReadLock             | Y  | Y  | Y  | N  | Y  |
/// |S | WriteLock            | Y  | Y  | N  | N  | Y  |
/// |T | ParentModification   | Y  | Y  | Y  | Y  | N  |
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Serialize)]
pub enum PLatchType {
    /// Sharable
    /// The node address has been obtained and will be read
    /// Incompatible with NodeDelete
    AccessIntent = 0,

    /// Exclusive
    /// The node no longer has a pointer to it
    /// Incompatible with AccessIntent
    NodeDelete = 1,

    /// Sharable
    /// Read the node
    /// Incompatible with WriteLock
    ReadLock = 2,

    /// Exclusive
    /// Modify the node
    /// Incompatible with ReadLock and other WriteLocks.
    WriteLock = 3,

    /// Exclusive
    /// Change the node's parent keys, held when a node's fence value is posted or removed during
    /// its parent SMO
    /// Incompatible with another ParentModification.
    ParentModification = 4,

    /// Compatible with all other types
    /// only used when debug
    None = 5,
}

impl PLatchType {
    pub fn conflict(l1: &Self, l2: &Self) -> bool {
        match (l1, l2) {
            (Self::WriteLock, Self::WriteLock) => true,
            (Self::WriteLock, Self::ReadLock) => true,
            (Self::ReadLock, Self::WriteLock) => true,
            (Self::NodeDelete, Self::NodeDelete) => true,
            (Self::NodeDelete, Self::AccessIntent) => true,
            (Self::AccessIntent, Self::NodeDelete) => true,
            (Self::ParentModification, Self::ParentModification) => true,
            _ => false,
        }
    }

    pub fn has_flag(l: Self, flag: u32) -> bool {
        (1 << (l as u32)) & flag != 0
    }

    pub fn flag(l: Self) -> u32 {
        Self::set_flag(l, 0)
    }

    pub fn set_flag(l: Self, flag: u32) -> u32 {
        let mut f = flag;
        f |= 1 << (l as u32);
        f
    }

    pub fn clr_flag(l: Self, flag: u32) -> u32 {
        let mut f = flag;
        f &= !(1 << (l as u32));
        f
    }
}

impl Display for PLatchType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
