use codex_harness_nexus_daemon::{BootstrapReading, Daemon, NexusBootstrap};
use nexus_exchange::ValidAdmission;
use shared_translator::placement::{Preparable, validate_place_name};

/// The bootstrap is checked before anything is made: its admission, then its
/// place's name; only then is the place prepared and the meta socket bound.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = NexusBootstrap::read_standard_input()?;
    let admission = ValidAdmission::try_from(&bootstrap.admission)
        .map_err(|message| format!("invalid Nexus bootstrap admission: {message}"))?;
    validate_place_name(&bootstrap.place.place_name.0)?;
    bootstrap
        .place
        .prepare_for_start()
        .map_err(|error| format!("place preparation failed: {error}"))?;
    let daemon = Daemon::bind(&bootstrap.place, admission, bootstrap.peer_identity)?;
    // The public socket's connections run on this runtime; the meta socket,
    // the accepting and each watch of the sessions directories stay on
    // threads of their own.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    daemon.serve(runtime.handle().clone())
}
