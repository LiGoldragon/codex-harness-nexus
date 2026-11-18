use codex_harness_translator::CodexHarness;
use translator_client::{AnswerPrinting, Invoking, TextInvocation, TextLane};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    TextInvocation::from_arguments::<CodexHarness>(TextLane::PublicText, std::env::args().skip(1))?
        .print_answers()?;
    Ok(())
}
