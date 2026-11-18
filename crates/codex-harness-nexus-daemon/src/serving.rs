//! This Nexus's bodies for the shared serving: the bounds its public socket
//! is served within, its meta answer and its public answer. Its name is the
//! core's, which its translator names it by too.

use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use codex_harness_nexus::{
    Channelled as _, CodexHarness, ConfigurationReading as _, HarnessNexus, MemoryPlacing as _,
    Opening as _, SessionsWatch, Store, WatchWord,
};
use meta_signal_codex_harness_nexus as meta;
use nexus_exchange::{Named as _, ValidAdmission};
use nexus_serving::{
    ConnectionServing, EmissionLane, Listeners, Listening, MetaAnswering, PeerAdmitting,
    PublicConnection, PublicServing, Reply, SubscriptionClosing, TransportBounded,
};
use shared_translator::placement::Place;
use shared_translator::{Caller, PeerIdentity};
use signal::{Frame, FrameCapacity};
use signal_codex_harness_nexus::{Answer, Answerable as _, Changed, Input, Observations, Output};

use crate::emitting::PairedTranslator;
use crate::waking::{ReadyOpening as _, WatchFollowing as _};

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
/// socket is open, and the words of its watch, which one thread follows.
pub struct Serving {
    pub nexus: HarnessNexus<PairedTranslator>,
    pub listeners: Listeners<CodexHarness>,
    pub bounds: Arc<Bounds>,
    pub opened: OnceLock<()>,
    pub words: Mutex<Receiver<WatchWord>>,
}

/// The served Nexus, shared by the meta thread, the public accepting and
/// every connection's task.
#[derive(Clone)]
pub struct Daemon {
    pub serving: Arc<Serving>,
}

/// How the daemon starts: bound, then served.
pub trait DaemonStarting: Sized {
    /// Binds the meta socket in `place` and opens the Memory in its state
    /// directory; the public socket waits for a ready configuration. Every
    /// emission goes to the translator's public socket in the same place.
    fn bind(
        place: &Place,
        admission: ValidAdmission,
        translator: PeerIdentity,
    ) -> Result<Self, Box<dyn std::error::Error>>;

    /// Serves until a listener fails, and answers with that failure: the
    /// meta socket, the public accepting and the watch each on a thread of
    /// its own, the public socket's connections on `runtime`. The sessions
    /// directories already recorded are watched, and a configuration
    /// already ready opens the public socket at once.
    fn serve(self, runtime: tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>>;
}

/// The listener that could no longer accept, which ends the daemon.
#[derive(Debug)]
pub enum ListenerFailure {
    /// The meta socket's listener, its error as it was told.
    Meta(signal_codex_harness_nexus::Diagnostic),
    /// The public socket's listener.
    Public(std::io::Error),
}

impl std::fmt::Display for ListenerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let title = CodexHarness::title().0;
        match self {
            Self::Meta(diagnostic) => {
                write!(formatter, "{title} meta listener failed: {}", diagnostic.0)
            }
            Self::Public(error) => write!(formatter, "{title} public listener failed: {error}"),
        }
    }
}

impl std::error::Error for ListenerFailure {}

impl DaemonStarting for Daemon {
    fn bind(
        place: &Place,
        admission: ValidAdmission,
        translator: PeerIdentity,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let listeners = Listeners::<CodexHarness>::bind(place, admission, translator)?;
        let lane = EmissionLane::from(listeners.paths().translator.clone());
        let store =
            Store::open(&place.memory_path::<CodexHarness>()).map_err(|diagnostic| diagnostic.0)?;
        let (sessions_watch, words) = SessionsWatch::channelled();
        Ok(Self {
            serving: Arc::new(Serving {
                nexus: HarnessNexus {
                    store,
                    translator: PairedTranslator { lane, admission },
                    serial: Mutex::new(()),
                    observations: Mutex::new(None),
                    configurations: Mutex::new(None),
                    sessions_watch: Mutex::new(sessions_watch),
                },
                listeners,
                bounds: Arc::new(Bounds { admission }),
                opened: OnceLock::new(),
                words: Mutex::new(words),
            }),
        })
    }

    fn serve(self, runtime: tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(error) = self.watch_recorded() {
            eprintln!(
                "Codex Harness Nexus cannot watch its recorded sessions directories: {error}"
            );
        }
        self.follow_watch();
        self.open()?;
        let (failed, failure) = mpsc::channel();
        let meta = self.clone();
        let meta_failed = failed.clone();
        std::thread::spawn(move || {
            let error = loop {
                if let Err(error) = meta.serving.listeners.serve_next_meta(&meta) {
                    break error;
                }
            };
            let diagnostic = signal_codex_harness_nexus::Diagnostic(error.to_string());
            let _ = meta_failed.send(ListenerFailure::Meta(diagnostic));
        });
        // The public socket admits the translator alone, whose authority is
        // the unit this Nexus's PeerAdmitting names.
        let public = self;
        std::thread::spawn(move || {
            let error = loop {
                match public.serving.listeners.accept_public(&public) {
                    Ok(accepted) => {
                        runtime.spawn(PublicConnection::<Daemon>::serve(
                            accepted.stream,
                            (),
                            public.clone(),
                            Arc::clone(&public.serving.bounds),
                        ));
                    }
                    Err(error) => break error,
                }
            };
            let _ = failed.send(ListenerFailure::Public(error));
        });
        Err(Box::new(failure.recv()?))
    }
}

/// The meta socket: Configure sets one value, and a value that makes the
/// configuration ready, or moves the sessions directories, opens what
/// follows from it; the text budget is the translator's, which answers it
/// itself, so one sent here is refused. Subscribe answers the readout, the
/// configuration as the Nexus holds it and the sessions directories it
/// awaits, one Opened frame, since the meta exchange answers once; its text
/// budget is the translator's and stays unset here.
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
            meta::Input::Subscribe => match nexus.readout() {
                Ok(readout) => meta::Output::Subscribed(Frame::Opened(readout)),
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
