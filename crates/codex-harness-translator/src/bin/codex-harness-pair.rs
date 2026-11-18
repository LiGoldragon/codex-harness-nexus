use codex_harness_translator::CodexHarnessTranslator;
use translator_serving::{PairReading, PairRunning, Pairing};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pairing = Pairing::from_arguments(std::env::args().skip(1))?;
    match pairing.launch::<CodexHarnessTranslator>()? {}
}
