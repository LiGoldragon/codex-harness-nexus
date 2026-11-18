//! This Nexus's bodies for the shared serving: the bounds its public socket
//! is served within, its meta answer and its public answer. Its name is the
//! core's, which its translator names it by too, and its Memory's path is
//! the shared one that name gives.

use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

pub use codex_harness_nexus::ListenerFailure;
use codex_harness_nexus::{
    CodexHarness, HarnessNexus, Opened, PublicOpening, SessionsWatch, Store, StoreOpener,
    StoreOpening as _, WatchGeneration, WatchWord,
};
use harness::FilePath;
use meta_signal_codex_harness_nexus as meta;
use nexus_exchange::{MemoryPlacing as _, Named as _, ValidAdmission};
use nexus_serving::{
    ConnectionServing, EmissionLane, Listeners, Listening, MetaAnswering, MetaConnection,
    MetaServing, PeerAdmitting, PublicConnection, PublicServing, Reply, SubscriptionClosing,
    TransportBounded,
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
    pub listeners: Arc<Listeners<CodexHarness>>,
    pub bounds: Arc<Bounds>,
    pub opened: OnceLock<()>,
    pub words: Mutex<Receiver<WatchWord>>,
}

/// The native listener capability the core actor invokes when readiness is
/// reached. It holds the same listeners the daemon's acceptors use.
#[derive(Clone)]
pub struct PublicListener {
    pub listeners: Arc<Listeners<CodexHarness>>,
}

impl PublicOpening for PublicListener {
    fn open_public(&self) -> Result<Opened, ListenerFailure> {
        let listener = self
            .listeners
            .stage_public()
            .map_err(|error| ListenerFailure::Public(error.to_string()))?;
        self.listeners.publish_public(listener);
        Ok(Opened(true))
    }
}

/// The served Nexus, shared by the accepting threads and every connection's
/// task.
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
    /// meta accepting, the public accepting and the watch each on a thread
    /// of its own, both sockets' connections on `runtime`. The sessions
    /// directories already recorded are watched, and a configuration
    /// already ready opens the public socket at once.
    fn serve(self, runtime: tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>>;
}

impl std::fmt::Display for ListenerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Meta(message) | Self::Public(message) => formatter.write_str(message),
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
        let listeners = Arc::new(Listeners::<CodexHarness>::bind(
            place, admission, translator,
        )?);
        let lane = EmissionLane::from(listeners.paths().translator.clone());
        let store = StoreOpener {}
            .open(FilePath(
                place
                    .memory_path::<CodexHarness>()
                    .to_string_lossy()
                    .into_owned(),
            ))
            .map_err(|diagnostic| diagnostic.0)?;
        let (sender, words) = mpsc::channel();
        let sessions_watch = SessionsWatch {
            sender,
            drawn: WatchGeneration(0),
            current: None,
        };
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
        // The meta socket admits the paired translator alone; each of its
        // connections is served on the runtime, side by side, so a
        // Configure is answered while a Subscribe stays open.
        let meta = Arc::new(self.clone());
        let meta_runtime = runtime.clone();
        let meta_failed = failed.clone();
        std::thread::spawn(move || {
            let error = loop {
                match meta.serving.listeners.accept_meta() {
                    Ok(stream) => {
                        meta_runtime.spawn(MetaConnection::<Daemon>::serve(
                            stream,
                            Arc::clone(&meta),
                            meta.serving.bounds.admission,
                        ));
                    }
                    Err(error) => break error,
                }
            };
            let message = format!("{} meta listener failed: {error}", CodexHarness::title().0);
            let _ = meta_failed.send(ListenerFailure::Meta(message));
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
            let message = format!(
                "{} public listener failed: {error}",
                CodexHarness::title().0
            );
            let _ = failed.send(ListenerFailure::Public(message));
        });
        Err(Box::new(failure.recv()?))
    }
}

/// The meta socket: Configure sets one value, and a value that makes the
/// configuration ready, or moves the sessions directories, opens what
/// follows from it; the text budget is the translator's, which answers it
/// itself, so one sent here is refused. Subscribe opens on the readout, the
/// configuration as the Nexus holds it and the sessions directories it
/// awaits, and stays open, carrying each value set and each change in what
/// is awaited; it needs the subscriber capacity, and before it is set is
/// refused CapacityUnset. The text budget is the translator's, which writes
/// it into the readout and carries its own changes into the subscription.
impl MetaAnswering for Daemon {
    type Component = CodexHarness;
    type Query = meta::Input;
    type Response = meta::Output;
    type State = meta::ConfigurationReadout;
    type Change = meta::ConfigurationChange;

    fn answer(
        &self,
        input: meta::Input,
    ) -> Reply<meta::Output, meta::ConfigurationReadout, meta::ConfigurationChange> {
        match meta::Answerable::answer(&input, &self.serving.nexus) {
            meta::Answer::Once(output) => {
                if let meta::Output::Configured(set) = &output
                    && let Err(error) = self.configured(set)
                {
                    eprintln!("Codex Harness Nexus could not open after {set:?}: {error}");
                }
                Reply::Once(output)
            }
            meta::Answer::Subscribed(stream) => Reply::Subscribed(stream),
        }
    }

    fn subscribed(
        frame: Frame<meta::ConfigurationReadout, meta::ConfigurationChange>,
    ) -> meta::Output {
        meta::Output::Subscribed(frame)
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
