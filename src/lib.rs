//! Codex rollout observations and the pure plan for a durable event batch.
//! Runtime composition and atomic persistence are still being declared.

extern crate self as memory;
extern crate self as operation;

mod generated {
    pub mod folding;
    pub mod memory;
    pub mod operation;
    pub mod rollout;
}
mod folding;
mod rollout;

pub use generated::folding::*;
pub use generated::memory::*;
pub use generated::operation::*;
pub use generated::rollout::*;
