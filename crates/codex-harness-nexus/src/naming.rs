//! The Codex Harness Nexus as the shared serving and the shared translator
//! name it: codex-harness.sock, codex-harness-meta.sock and its translator's
//! two, each in its place's runtime directory.

use nexus_exchange::{ComponentName, ComponentTitle, Named};

/// The Codex Harness Nexus, as its sockets are named.
pub struct CodexHarness;

impl Named for CodexHarness {
    fn name() -> ComponentName {
        ComponentName("codex-harness".into())
    }

    fn title() -> ComponentTitle {
        ComponentTitle("Codex Harness Nexus".into())
    }
}
