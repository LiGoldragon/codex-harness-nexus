//! A failure as the Nexus is told it: its text.

use signal_codex_harness_nexus::Diagnostic;

use crate::generated::actor::ListenerFailure;

pub use crate::generated::runtime::Diagnosing;

impl<Failure: std::fmt::Display> Diagnosing for Failure {
    fn diagnostic(&self) -> Diagnostic {
        Diagnostic(self.to_string())
    }
}

impl std::fmt::Display for ListenerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Meta(message) | Self::Public(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ListenerFailure {}
