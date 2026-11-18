use codex_harness_translator::CodexHarnessTranslator;
use translator_serving::{BootstrapReading, TranslatorBootstrap, TranslatorServing};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = TranslatorBootstrap::read_standard_input()?;
    CodexHarnessTranslator::try_from(&bootstrap)?.serve(&bootstrap)?;
    Ok(())
}
