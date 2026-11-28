//! The subscription CLI: signal-harness, as the Codex Harness Nexus serves it.

fn main() -> std::process::ExitCode {
    codex_harness_translator::call(transport::Placed::SubscriptionTextSocket)
}
