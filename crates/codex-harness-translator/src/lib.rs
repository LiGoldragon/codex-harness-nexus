//! The Codex Harness Nexus's translator: the process in front of the Nexus
//! that speaks text for it on the serving every translator shares
//! (`translator-serving`), and carries each emission of the Nexus into the
//! world, reading Codex's own text, the rollout rows, its first session row
//! and the identity producer's answer. Its CLIs are on the client end
//! (`translator-client`).

mod generated {
    pub mod rollout;
}
mod emitting;
mod rollout;
mod translating;

pub use codex_harness_nexus::CodexHarness;
pub use generated::rollout::*;
pub use translating::{CodexHarnessTranslator, TranslatorSettings};
