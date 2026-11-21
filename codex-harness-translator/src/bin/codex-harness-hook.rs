//! The hook client: Codex runs it at each event with the payload on its
//! input. It writes the payload, as it came, to the translator's hook socket,
//! waits until the translator has reported it, and ends with 0 whatever happened.

use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::process::ExitCode;

use codex_harness_translator::CodexHarness;
use transport::{Place, Placed};

fn main() -> ExitCode {
    let mut payload = Vec::new();
    if std::io::stdin().read_to_end(&mut payload).is_ok()
        && let Ok(place) = Place::from_configuration::<CodexHarness>()
        && let Ok(mut stream) = UnixStream::connect(place.path::<CodexHarness>(Placed::HookSocket))
        && stream.write_all(&payload).is_ok()
        && stream.shutdown(Shutdown::Write).is_ok()
    {
        let _ = stream.read_to_end(&mut Vec::new());
    }
    ExitCode::SUCCESS
}
