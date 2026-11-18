use codex_harness_translator::CodexHarness;
use translator_client::{TextCommand, TextInvocation, TextLane};

fn main() -> std::process::ExitCode {
    TextInvocation::run::<CodexHarness>(TextLane::PublicText, std::env::args().skip(1))
}
