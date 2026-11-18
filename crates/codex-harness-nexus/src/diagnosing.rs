//! A failure as the Nexus is told it: its text.

use signal_codex_harness_nexus::Diagnostic;

pub use crate::generated::runtime::Diagnosing;

impl<Failure: std::fmt::Display> Diagnosing for Failure {
    fn diagnostic(&self) -> Diagnostic {
        Diagnostic(self.to_string())
    }
}
