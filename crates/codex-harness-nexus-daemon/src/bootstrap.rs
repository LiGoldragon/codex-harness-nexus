//! The frame the Nexus is started with: the shared Nexus bootstrap, which
//! the pair launcher writes on standard input.

use nexus_serving::{BootstrapReading as _, NexusBootstrap};

pub trait StandardInputReading: Sized {
    /// Reads the one bootstrap frame from standard input, within Signal's
    /// largest frame, then closes standard input.
    fn read_standard_input() -> Result<Self, Box<dyn std::error::Error>>;
}

impl StandardInputReading for NexusBootstrap {
    fn read_standard_input() -> Result<Self, Box<dyn std::error::Error>> {
        let mut input = std::io::stdin().lock();
        let bootstrap = Self::read_bootstrap(&mut input)?;
        drop(input);
        nix::unistd::close(0)?;
        Ok(bootstrap)
    }
}
