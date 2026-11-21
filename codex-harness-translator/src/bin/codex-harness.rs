//! The CLI of the Codex Harness Nexus.

fn main() -> std::process::ExitCode {
    codex_harness_translator::call(transport::Placed::TextSocket)
}
