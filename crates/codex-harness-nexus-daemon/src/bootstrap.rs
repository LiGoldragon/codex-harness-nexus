//! The frame the Nexus is started with.

use codex_harness_nexus::NexusBootstrap;
use signal::{FrameCapacity, FrameReading, Restorable, Signal};

pub trait BootstrapReading: Sized {
    /// Reads the one bootstrap frame the pair launcher writes on standard
    /// input, within Signal's largest frame, then closes standard input.
    fn read_standard_input() -> Result<Self, Box<dyn std::error::Error>>;
}

impl BootstrapReading for NexusBootstrap {
    fn read_standard_input() -> Result<Self, Box<dyn std::error::Error>> {
        let mut input = std::io::stdin().lock();
        let body = input.read_frame(FrameCapacity::from(signal::MAXIMUM_SIGNAL_BYTES))?;
        let bootstrap = Signal::<Self>::from(Vec::<u8>::from(body)).restore()?;
        drop(input);
        nix::unistd::close(0)?;
        Ok(bootstrap)
    }
}
