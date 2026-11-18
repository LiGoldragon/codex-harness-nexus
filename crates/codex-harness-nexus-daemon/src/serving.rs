//! This Nexus's bodies for the shared serving: the bounds its public socket
//! is served within, its meta answer and its public answer. Its name is the
//! core's, which its translator names it by too.

use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use codex_harness_nexus::{CodexHarness, HarnessNexus, Opening as _, Remembering as _, Store};
use meta_signal_codex_harness_nexus as meta;
use nexus_exchange::ValidAdmission;
use nexus_serving::{
    ConnectionServing, EmissionLane, Listeners, Listening, MetaAnswering, PeerAdmitting,
    PublicConnection, PublicServing, Reply, SubscriptionClosing, TransportBounded,
};
use shared_translator::placement::Place;
use shared_translator::{Caller, PeerIdentity};
use signal::{Frame, FrameCapacity};
use signal_codex_harness_nexus::{Answer, Answerable as _, Changed, Input, Observations, Output};

use crate::emitting::PairedTranslator;
use crate::waking::ReadyOpening as _;

/// The file in the place's state directory that holds the Nexus's Memory.
const MEMORY_FILE: &str = "codex-harness.memory";

/// The bounds the public socket is served within: the admission the pair
/// launcher handed the Nexus.
pub struct Bounds {
    pub admission: ValidAdmission,
}

impl TransportBounded for Bounds {
    fn transport_timeout(&self) -> Duration {
        self.admission.deadline
    }

    fn frame_capacity(&self) -> FrameCapacity {
        FrameCapacity::from(self.admission.frame_capacity)
    }
}

/// Everything one served Nexus holds: the Nexus itself, its emissions carried
/// to its paired translator, its listeners, its bounds, whether its public
/// socket is open, and which watch of its sessions directories is current.
pub struct Serving {
    pub nexus: HarnessNexus<PairedTranslator>,
    pub listeners: Listeners<CodexHarness>,
    pub bounds: Arc<Bounds>,
    pub opened: OnceLock<()>,
    pub watch_generation: Mutex<u64>,
}

/// The served Nexus, shared by the meta thread, the public accepting and
/// every connection's task.
#[derive(Clone)]
pub struct Daemon {
    pub serving: Arc<Serving>,
}

impl Daemon {
    /// Binds the meta socket in `place` and opens the Memory in its state
    /// directory; the public socket waits for a ready configuration. Every
    /// emission goes to the translator's public socket in the same place.
    pub fn bind(
        place: &Place,
        admission: ValidAdmission,
        translator: PeerIdentity,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let listeners = Listeners::<CodexHarness>::bind(place, admission, translator)?;
        let lane = EmissionLane::from(listeners.paths().translator.clone());
        let store = Store::open(&Path::new(&place.state.0).join(MEMORY_FILE))
            .map_err(|diagnostic| diagnostic.0)?;
        Ok(Self {
            serving: Arc::new(Serving {
                nexus: HarnessNexus {
                    store,
                    translator: PairedTranslator { lane, admission },
                    serial: Mutex::new(()),
                    observations: Mutex::new(None),
                    configurations: Mutex::new(None),
                },
                listeners,
                bounds: Arc::new(Bounds { admission }),
                opened: OnceLock::new(),
                watch_generation: Mutex::new(0),
            }),
        })
    }

    /// Serves until a listener fails: the meta socket on a thread of its
    /// own, the public socket's connections on `runtime`. A configuration
    /// already ready opens the public socket at once.
    pub fn serve(self, runtime: tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>> {
        self.open()?;
        let meta = self.clone();
        std::thread::spawn(move || {
            loop {
                if let Err(error) = meta.serving.listeners.serve_next_meta(&meta) {
                    eprintln!("Codex Harness Nexus meta listener failed: {error}");
                    std::process::exit(1);
                }
            }
        });
        loop {
            let accepted = self.serving.listeners.accept_public(&self)?;
            runtime.spawn(PublicConnection::<Daemon>::serve(
                accepted.stream,
                accepted.authority,
                self.clone(),
                Arc::clone(&self.serving.bounds),
            ));
        }
    }
}

/// The meta socket: Configure sets one value, and a value that makes the
/// configuration ready, or moves the sessions directories, opens what
/// follows from it; the text budget is the translator's, which answers it
/// itself, so one sent here is refused. Subscribe answers the configuration
/// as the Nexus holds it, one Opened frame, since the meta exchange answers
/// once; its text budget is the translator's and stays unset here.
impl MetaAnswering for Daemon {
    type Query = meta::Input;
    type Response = meta::Output;

    fn answer(&self, input: meta::Input) -> meta::Output {
        let nexus = &self.serving.nexus;
        match input {
            meta::Input::Configure(set) => {
                let output = match meta::Answerable::answer(&meta::Input::Configure(set), nexus) {
                    meta::Answer::Once(output) => output,
                    meta::Answer::Subscribed(_) => unreachable!("Configure answers once"),
                };
                if let meta::Output::Configured(set) = &output
                    && let Err(error) = self.configured(set)
                {
                    eprintln!("Codex Harness Nexus could not open after {set:?}: {error}");
                }
                output
            }
            meta::Input::Subscribe => match nexus.store.configuration() {
                Ok(configuration) => meta::Output::Subscribed(Frame::Opened(configuration)),
                Err(diagnostic) => meta::Output::ConfigureRefused(
                    meta::ConfigurationRefusal::Unrecorded(meta::Diagnostic(diagnostic.0)),
                ),
            },
        }
    }
}

/// The public socket admits its paired translator alone: every caller of
/// this Nexus speaks text through it.
impl PeerAdmitting for Daemon {
    type Authority = ();

    fn translator() {}

    fn peer(&self, _caller: &Caller) -> Option<()> {
        None
    }
}

impl PublicServing for Daemon {
    type Component = CodexHarness;
    type Query = Input;
    type Response = Output;
    type Output = Output;
    type State = Observations;
    type Change = Changed;
    type Configuration = Bounds;
    type Authority = ();
    type Grant = ();
    type Input = Input;

    fn configuration(&self) -> Option<Arc<Bounds>> {
        Some(Arc::clone(&self.serving.bounds))
    }

    fn admitted(&self, _authority: &(), _caller: &Caller, query: Input) -> Option<((), Input)> {
        Some(((), query))
    }

    fn answer(
        &self,
        _grant: (),
        input: Input,
        _configuration: Arc<Bounds>,
    ) -> Reply<Output, Observations, Changed> {
        match input.answer(&self.serving.nexus) {
            Answer::Once(output) => Reply::Once(output),
            Answer::Subscribed(stream) => Reply::Subscribed(stream),
        }
    }

    fn carried(&self, _grant: &(), _closing: &Arc<SubscriptionClosing>) -> bool {
        true
    }

    fn answered(output: Output) -> Output {
        output
    }

    fn subscribed(frame: Frame<Observations, Changed>) -> Output {
        Output::Subscribed(frame)
    }
}
