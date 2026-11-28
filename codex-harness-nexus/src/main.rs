//! The Codex Harness Nexus: the process manager on its queue, and the world it
//! traces into. The signal process holds the sockets, the memory process the
//! disk, and an emission is carried to the translator on a connection of its own.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::ExitCode;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use codex_harness_process::*;
use flow::FlowId;
use harness::{Event, FilePath, Observed, State, Tokens, Transitionable};
use process::{Answer, Arrival, Caller, Ended, Gave, Given, Identity, Living, Moment, Ownable, Owner, ProcessId, ProcessManager, Roots, Trace, Traced, Tracing, Unobserved, Unremembered, UserId};
use transport::{Framed, Named, Place, Placed};

struct CodexHarness;

impl Named for CodexHarness {
    const NAME: &'static str = NAME;
}

/// What the manager's queue carries: an arrival for the manager, or the
/// library's announcement that the translator has begun to listen.
enum Arrived {
    Arrival(Arrival<Process>),
    Listening,
}

type Queue = Sender<Arrived>;

/// What the memory holds, archived whole as its file: each session observed,
/// each Watch, and the configuration its Keeps have come to: the rollout root
/// is the directory Codex keeps its sessions' rollouts in.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Records {
    observed_vector: Vec<Observed>,
    watch_vector: Vec<Watch>,
    row_count_option: Option<RowCount>,
    thread_database_option: Option<ThreadDatabase>,
    hook_name_vector: Vec<HookName>,
    rollout_root_option: Option<FilePath>,
}

impl Records {
    /// The row count a Watch is born with when a rollout root is configured
    /// and no row count is: the one the README configures, so a session
    /// discovered with no hook and no other configuration is still read.
    const DISCOVERED_ROW_COUNT: RowCount = RowCount(256);

    /// The row count a Watch is born with: the one configured, else the
    /// discovery's own once a rollout root is configured. It is derived, not
    /// kept: each Watch born after a row count is configured takes that one,
    /// and with neither configured a Watch waits for its row count.
    fn row_count(&self) -> Option<RowCount> {
        self.row_count_option.or_else(|| self.rollout_root_option.as_ref().map(|_| Records::DISCOVERED_ROW_COUNT))
    }
}

/// The memory process: one change at a time, each kept before it is answered.
struct MemoryProcess {
    records: Records,
    file: std::path::PathBuf,
}

impl MemoryProcess {
    fn open(place: &Place) -> MemoryProcess {
        let file = place.path::<CodexHarness>(Placed::Memory(VERSION));
        let records = File::open(&file)
            .ok()
            .and_then(|file| Records::read_from(&mut BufReader::new(file)).ok().flatten())
            .unwrap_or(Records { observed_vector: Vec::new(), watch_vector: Vec::new(), row_count_option: None, thread_database_option: None, hook_name_vector: Vec::new(), rollout_root_option: None });
        MemoryProcess { records, file }
    }

    fn keep(&self) -> Answer {
        let temporary = self.file.with_extension("next");
        let written = File::create(&temporary)
            .and_then(|mut file| self.records.write_to(&mut file))
            .and_then(|()| std::fs::rename(&temporary, &self.file));
        match written {
            Ok(()) => Answer::Succeeded,
            Err(_) => Answer::Failed,
        }
    }

    fn holds_watch(&self, flow_id: &FlowId) -> bool {
        self.records.watch_vector.iter().any(|watch| watch.flow_id == *flow_id)
    }

    fn unlocated(&self, flow_id: &FlowId) -> bool {
        self.records.watch_vector.iter().any(|watch| watch.flow_id == *flow_id && watch.rollout == Rollout::Unlocated)
    }

    /// A Watch's process, when the row count it is born with is configured
    /// or a rollout root is; the thread database is moved in with it, as it
    /// is configured.
    fn process_of(&self, watch: &Watch) -> Option<Process> {
        let row_count = self.records.row_count()?;
        Some(Process::Watch(WatchProcess {
            flow_id: watch.flow_id.clone(),
            session_id: watch.session_id.clone(),
            rollout: watch.rollout.clone(),
            read_from: watch.read_from.clone(),
            row_count,
            thread_database_option: self.records.thread_database_option.clone(),
        }))
    }

    /// The discovery's process, when the rollout root it is born with is configured.
    fn discover_process(&self) -> Option<Process> {
        let file_path = self.records.rollout_root_option.clone()?;
        Some(Process::Discover(DiscoverProcess { file_path }))
    }

    /// The rollout found for a Watch is kept with it.
    fn locate(&mut self, locate: Locate) -> Answer {
        for watch in self.records.watch_vector.iter_mut().filter(|watch| watch.flow_id == locate.flow_id) {
            watch.rollout = Rollout::Located(locate.file_path.clone());
        }
        self.keep()
    }

    /// A Watch of the same flow and session is kept as it is, with the offset
    /// read so far; a rollout it newly names is kept with it. A session named
    /// by its start and discovered under the root is so one Watch.
    fn watch(&mut self, watch: Watch) -> Answer {
        if let Some(held) = self.records.watch_vector.iter_mut().find(|held| held.flow_id == watch.flow_id && held.session_id == watch.session_id) {
            return match watch.rollout {
                Rollout::Located(file_path) if held.rollout != Rollout::Located(file_path.clone()) => {
                    held.rollout = Rollout::Located(file_path);
                    self.keep()
                }
                _ => Answer::Succeeded,
            };
        }
        self.records.watch_vector.retain(|held| held.flow_id != watch.flow_id);
        self.records.watch_vector.push(watch);
        self.keep()
    }

    fn configure(&mut self, Keep(set): Keep) -> Answer {
        match set {
            Set::Hooks(hook_name_vector) => self.records.hook_name_vector = hook_name_vector,
            Set::RemoveHook(hook_name) => self.records.hook_name_vector.retain(|held| *held != hook_name),
            Set::RowCount(row_count) => self.records.row_count_option = Some(row_count),
            Set::ThreadDatabase(thread_database) => self.records.thread_database_option = Some(thread_database),
            Set::RolloutRoot(file_path) => self.records.rollout_root_option = Some(file_path),
        }
        self.keep()
    }

    /// Applies each event and keeps the offset. What a Watch observes applies
    /// only while its Watch is kept. A session's start heard after its rollout
    /// was discovered and read is no news: the rows observed came after it. A
    /// session that has ended is forgotten, its Observed and its Watch, once
    /// its changes are answered.
    fn observe(&mut self, mut observe: Observe) -> Result<Observation, Unremembered> {
        if observe.offset_option.is_some() && !self.holds_watch(&observe.flow_id) {
            return Ok(Observation(Vec::new()));
        }
        let read = self.records.watch_vector.iter().any(|watch| watch.flow_id == observe.flow_id && matches!(watch.read_from, ReadFrom::Offset(Offset(offset)) if offset > 0));
        if observe.offset_option.is_none() && read {
            observe.event_vector.retain(|event| !matches!(event, Event::Started));
        }
        let position = match self.records.observed_vector.iter().position(|observed| observed.flow_id == observe.flow_id) {
            Some(position) => position,
            None => {
                self.records.observed_vector.push(Observed { flow_id: observe.flow_id.clone(), state: State::Dry, tokens: Tokens(0), background_work_vector: vec![] });
                self.records.observed_vector.len() - 1
            }
        };
        let observed = &mut self.records.observed_vector[position];
        let changed = observe
            .event_vector
            .into_iter()
            .map(|event| {
                observed.after(event.clone());
                Changed { flow_id: observe.flow_id.clone(), event, state: observed.state.clone() }
            })
            .collect();
        if observed.state == State::Ended {
            self.records.observed_vector.remove(position);
            self.records.watch_vector.retain(|watch| watch.flow_id != observe.flow_id);
        } else if let Some(offset) = observe.offset_option {
            for watch in self.records.watch_vector.iter_mut().filter(|watch| watch.flow_id == observe.flow_id) {
                watch.read_from = ReadFrom::Offset(offset);
            }
        }
        match self.keep() {
            Answer::Succeeded => Ok(Observation(changed)),
            Answer::Failed => Err(Unremembered {}),
        }
    }
}

/// A subscriber's connection holds what changed until its Subscribed is written.
struct Subscription {
    pending: Vec<signal_harness::Output>,
    answered: bool,
}

/// The signal process: what enters the program and what leaves it.
struct SignalProcess {
    arrived: Arc<Mutex<BTreeMap<Identity, UnixStream>>>,
    connections: BTreeMap<Identity, UnixStream>,
    subscriptions: BTreeMap<Identity, Subscription>,
    queue: Queue,
}

/// What a call becomes: the process it is born as, or the library's Listening.
enum Called {
    Born(Process),
    Listening,
    Rejected,
}

impl SignalProcess {
    /// Each socket's calls become births: the identity is drawn here, and the
    /// call's connection is held under it until its process is born.
    fn listen<C, F>(place: &Place, placed: Placed, roots: Arc<Roots>, arrived: Arc<Mutex<BTreeMap<Identity, UnixStream>>>, queue: Queue, called: F) -> std::io::Result<()>
    where
        C: Framed + Send + 'static,
        F: Fn(C, &UnixStream) -> Called + Send + 'static,
    {
        let path = place.path::<CodexHarness>(placed);
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let Ok(Some(call)) = C::read_from(&mut stream) else { continue };
                let arrival = match called(call, &stream) {
                    Called::Listening => {
                        let _ = Output::Acknowledged.write_to(&mut stream);
                        Arrived::Listening
                    }
                    Called::Rejected => continue,
                    Called::Born(process) => {
                        let identity = roots.draw();
                        arrived.lock().expect("the arrived connections").insert(identity.clone(), stream);
                        Arrived::Arrival(Arrival::Born(Living { identity, process }))
                    }
                };
                if queue.send(arrival).is_err() {
                    return;
                }
            }
        });
        Ok(())
    }

    fn start(place: &Place, roots: Arc<Roots>, queue: Queue) -> std::io::Result<SignalProcess> {
        let arrived = Arc::new(Mutex::new(BTreeMap::new()));
        SignalProcess::listen(place, Placed::NexusSocket, roots.clone(), arrived.clone(), queue.clone(), |call: Call, _stream| match call.input {
            Input::Report(hook) => Called::Born(Process::Report(ReportProcess { caller: call.caller, hook })),
            Input::Listening => Called::Listening,
        })?;
        SignalProcess::listen(place, Placed::SubscriptionSocket, roots.clone(), arrived.clone(), queue.clone(), |input: signal_harness::Input, stream| match input {
            signal_harness::Input::Subscribe => match peer(stream) {
                Some(caller) => Called::Born(Process::Subscribe(SubscribeProcess { caller })),
                None => Called::Rejected,
            },
        })?;
        SignalProcess::listen(place, Placed::MetaSocket, roots, arrived.clone(), queue.clone(), |call: MetaCall, _stream| match call.meta_input {
            meta::Input::Configure(set) => Called::Born(Process::Configure(ConfigureProcess { caller: call.caller, set })),
        })?;
        Ok(SignalProcess { arrived, connections: BTreeMap::new(), subscriptions: BTreeMap::new(), queue })
    }

    /// A call's process takes the connection held under its identity. A
    /// subscriber's process ends when its connection closes.
    fn bind(&mut self, living: &Living<Process>) {
        if living.process.owner() != Owner::Signal {
            return;
        }
        let Some(stream) = self.arrived.lock().expect("the arrived connections").remove(&living.identity) else { return };
        if let (Process::Subscribe(_), Ok(mut closing)) = (&living.process, stream.try_clone()) {
            let identity = living.identity.clone();
            let queue = self.queue.clone();
            thread::spawn(move || {
                let _ = std::io::copy(&mut closing, &mut std::io::sink());
                let _ = queue.send(Arrived::Arrival(Arrival::Ended(Ended { identity, outcome: Outcome::Subscribe })));
            });
        }
        self.connections.insert(living.identity.clone(), stream);
    }

    fn answer<T: Framed>(&mut self, identity: &Identity, output: &T) {
        if let Some(stream) = self.connections.get_mut(identity) {
            let _ = output.write_to(stream);
        }
    }

    fn answer_subscription(&mut self, identity: &Identity, output: &signal_harness::Output) -> bool {
        self.connections.get_mut(identity).is_some_and(|stream| output.write_to(stream).is_ok())
    }

    /// From the moment its recollection is taken, a subscriber misses nothing.
    fn subscribe(&mut self, identity: &Identity) {
        self.subscriptions.insert(identity.clone(), Subscription { pending: Vec::new(), answered: false });
    }

    fn subscribed(&mut self, identity: &Identity, output: signal_harness::Output) {
        if !self.answer_subscription(identity, &output) {
            self.close(identity);
            return;
        }
        let pending = match self.subscriptions.get_mut(identity) {
            Some(subscription) => {
                subscription.answered = true;
                std::mem::take(&mut subscription.pending)
            }
            None => Vec::new(),
        };
        for output in pending {
            if !self.answer_subscription(identity, &output) {
                self.close(identity);
                break;
            }
        }
    }

    fn publish(&mut self, output: signal_harness::Output) {
        let mut failed = Vec::new();
        for (identity, subscription) in self.subscriptions.iter_mut() {
            match (subscription.answered, self.connections.get_mut(identity)) {
                (true, Some(stream)) => {
                    if output.write_to(stream).is_err() {
                        failed.push(identity.clone());
                    }
                }
                _ => subscription.pending.push(output.clone()),
            }
        }
        for identity in failed {
            self.close(&identity);
        }
    }

    fn close(&mut self, identity: &Identity) {
        self.subscriptions.remove(identity);
        if let Some(stream) = self.connections.remove(identity) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

/// A subscription has no in-band caller: its identity is its Unix peer.
fn peer(stream: &UnixStream) -> Option<Caller> {
    let credentials = rustix::net::sockopt::socket_peercred(stream).ok()?;
    Some(Caller { process_id: ProcessId(credentials.pid.as_raw_nonzero().get().into()), user_id: UserId(credentials.uid.as_raw().into()) })
}

/// The emissions on their way to the translator, each on its own connection.
struct EmissionSocket {
    place: Place,
    lasting: BTreeMap<Identity, UnixStream>,
}

impl EmissionSocket {
    /// Dials the translator and writes the emission: None is Unsubmitted.
    fn submit(&self, emission: &Emission) -> Option<UnixStream> {
        let mut stream = UnixStream::connect(self.place.path::<CodexHarness>(Placed::EmissionSocket)).ok()?;
        emission.write_to(&mut stream).ok()?;
        Some(stream)
    }

    /// One outcome on its own connection, read beside the queue.
    fn find(&self, identity: Identity, find_rollout: FindRollout_Data, queue: Queue) {
        let given = move |identity: Identity, taken| Arrived::Arrival(Arrival::Given(Given { identity, taken: Taken::RolloutFinding(taken) }));
        let Some(mut stream) = self.submit(&Emission::FindRollout(find_rollout)) else {
            let _ = queue.send(given(identity, Err(Unobserved::Unsubmitted)));
            return;
        };
        thread::spawn(move || {
            let finding = match RolloutFinding_Data::read_from(&mut stream) {
                Ok(Some(finding)) => Ok(finding),
                _ => Err(Unobserved::Unanswered),
            };
            let _ = queue.send(given(identity, finding));
        });
    }

    /// A lasting emission: each outcome read is given to its process as it
    /// comes, until the connection ends, which is given as Unanswered.
    fn last<O: Framed + Send + 'static>(&mut self, identity: Identity, emission: Emission, queue: Queue, taken: fn(Result<O, Unobserved>) -> Taken) {
        let given = move |identity: &Identity, outcome| Arrived::Arrival(Arrival::Given(Given { identity: identity.clone(), taken: taken(outcome) }));
        let Some(stream) = self.submit(&emission) else {
            let _ = queue.send(given(&identity, Err(Unobserved::Unsubmitted)));
            return;
        };
        let Ok(mut reading) = stream.try_clone() else { return };
        self.lasting.insert(identity.clone(), stream);
        thread::spawn(move || {
            loop {
                match O::read_from(&mut reading) {
                    Ok(Some(outcome)) => {
                        if queue.send(given(&identity, Ok(outcome))).is_err() {
                            return;
                        }
                    }
                    _ => {
                        let _ = queue.send(given(&identity, Err(Unobserved::Unanswered)));
                        return;
                    }
                }
            }
        });
    }

    fn close(&mut self, identity: &Identity) {
        if let Some(stream) = self.lasting.remove(identity) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}

/// Everything the manager does passes here: it is archived, and carried.
struct World {
    signal: SignalProcess,
    memory: MemoryProcess,
    emission: EmissionSocket,
    archive: File,
    queue: Queue,
    roots: Arc<Roots>,
    /// Each Watch that has a living process, or one on its way to birth, by its flow.
    watching: BTreeMap<FlowId, Identity>,
    /// The one discovery that has a living process, or one on its way to birth, and its root.
    discovering: Option<(FilePath, Identity)>,
}

impl World {
    fn give_back(&self, identity: Identity, taken: Taken) {
        let _ = self.queue.send(Arrived::Arrival(Arrival::Given(Given { identity, taken })));
    }

    fn end(&self, identity: Identity, outcome: Outcome) {
        let _ = self.queue.send(Arrived::Arrival(Arrival::Ended(Ended { identity, outcome })));
    }

    /// Every Watch kept and without a living process is given its process, once
    /// its row count is known (Records::row_count): at the program's start, when
    /// the translator begins to listen, and when a Watch or the configuration is kept.
    fn bear_watches(&mut self) {
        let unborn: Vec<Process> = self
            .memory
            .records
            .watch_vector
            .iter()
            .filter(|watch| !self.watching.contains_key(&watch.flow_id))
            .filter_map(|watch| self.memory.process_of(watch))
            .collect();
        for process in unborn {
            let identity = self.roots.draw();
            if let Process::Watch(watch) = &process {
                self.watching.insert(watch.flow_id.clone(), identity.clone());
            }
            let _ = self.queue.send(Arrived::Arrival(Arrival::Born(Living { identity, process })));
        }
    }

    /// The discovery of the rollout root configured is given its process where
    /// it has none: at the program's start, when the translator begins to
    /// listen, and when the configuration is kept. A root replaced ends the
    /// discovery of the old one.
    fn bear_discovery(&mut self) {
        let configured = self.memory.records.rollout_root_option.clone();
        if let Some((root, identity)) = &self.discovering {
            if Some(root) == configured.as_ref() {
                return;
            }
            self.end(identity.clone(), Outcome::Discover(Discovering::Forgotten));
            self.discovering = None;
        }
        let Some(process) = self.memory.discover_process() else { return };
        let identity = self.roots.draw();
        if let Process::Discover(discover) = &process {
            self.discovering = Some((discover.file_path.clone(), identity.clone()));
        }
        let _ = self.queue.send(Arrived::Arrival(Arrival::Born(Living { identity, process })));
    }

    /// A Watch whose rollout was not found is given its process again when its
    /// session is next observed, since a session that acts has a rollout.
    fn bear_unlocated(&mut self, flow_id: &FlowId) {
        if self.memory.unlocated(flow_id) && !self.watching.contains_key(flow_id) {
            self.bear_watches();
        }
    }

    /// A Watch forgotten ends its process.
    fn forget(&mut self) {
        let forgotten: Vec<Identity> = self.watching.iter().filter(|(flow_id, _)| !self.memory.holds_watch(flow_id)).map(|(_, identity)| identity.clone()).collect();
        for identity in forgotten {
            self.end(identity, Outcome::Watch(Watching::Forgotten));
        }
    }

    fn born(&mut self, living: &Living<Process>) {
        self.signal.bind(living);
        if let Process::Watch(watch) = &living.process
            && !self.memory.holds_watch(&watch.flow_id)
        {
            self.end(living.identity.clone(), Outcome::Watch(Watching::Forgotten));
        }
    }

    fn carry(&mut self, Gave { identity, gift }: Gave<Gift>) {
        match gift {
            Gift::Reported => self.signal.answer(&identity, &Output::Reported),
            Gift::Subscribed(Subscribed(observed_vector)) => self.signal.subscribed(&identity, signal_harness::Output::Subscribed(observed_vector)),
            Gift::Changed(changed) => self.signal.publish(signal_harness::Output::Changed(changed.into())),
            Gift::Configured(Configured(set)) => self.signal.answer(&identity, &meta::Output::Configured(set)),
            Gift::Recall(Recall {}) => {
                self.signal.subscribe(&identity);
                let recollection = Recollection(self.memory.records.observed_vector.clone());
                self.give_back(identity, Taken::Recollection(Ok(recollection)));
            }
            Gift::Watch(watch) => {
                let answer = self.memory.watch(watch);
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
            }
            Gift::Keep(keep) => {
                let answer = self.memory.configure(keep);
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
                self.bear_discovery();
            }
            Gift::Observe(observe) => {
                let flow_id = observe.flow_id.clone();
                let observation = self.memory.observe(observe);
                self.give_back(identity, Taken::Observation(observation));
                self.forget();
                self.bear_unlocated(&flow_id);
            }
            Gift::Locate(locate) => {
                let answer = self.memory.locate(locate);
                self.give_back(identity, Taken::Answer(answer));
            }
            Gift::Follow(follow) => self.emission.last(identity, Emission::Follow(follow), self.queue.clone(), Taken::Following),
            Gift::Discover(discover) => self.emission.last(identity, Emission::Discover(discover), self.queue.clone(), Taken::Discovered),
            Gift::FindRollout(find_rollout) => self.emission.find(identity, find_rollout, self.queue.clone()),
        }
    }
}

impl Tracing<Process> for World {
    fn trace(&mut self, trace: Traced<Process>) {
        let _ = trace.write_to(&mut self.archive);
        match trace {
            Trace::Started(_) => {}
            Trace::Born(living) => self.born(&living),
            Trace::Gave(gave) => self.carry(gave),
            Trace::Took(_) => {}
            Trace::Ended(ended) => {
                let flow_id = self.watching.iter().find(|(_, identity)| **identity == ended.identity).map(|(flow_id, _)| flow_id.clone());
                self.watching.retain(|_, identity| *identity != ended.identity);
                if self.discovering.as_ref().is_some_and(|(_, identity)| *identity == ended.identity) {
                    self.discovering = None;
                }
                // A Watch that ended Unlocated while its rollout was discovered is given its process again.
                if ended.outcome == Outcome::Watch(Watching::Unlocated)
                    && let Some(flow_id) = flow_id
                    && self.memory.holds_watch(&flow_id)
                    && !self.memory.unlocated(&flow_id)
                {
                    self.bear_watches();
                }
                self.signal.close(&ended.identity);
                self.emission.close(&ended.identity);
            }
        }
    }
}

fn main() -> ExitCode {
    let place = match Place::from_arguments() {
        Ok(place) => place,
        Err(unplaced) => {
            eprintln!("{unplaced:?}");
            return ExitCode::from(2);
        }
    };
    let (queue, arrivals) = mpsc::channel();
    let roots = Arc::new(Roots::default());
    let started = place.prepare().and_then(|()| {
        let archive = OpenOptions::new().create(true).append(true).open(place.path::<CodexHarness>(Placed::Trace(VERSION)))?;
        let signal = SignalProcess::start(&place, roots.clone(), queue.clone())?;
        Ok((archive, signal))
    });
    let (archive, signal) = match started {
        Ok(started) => started,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let memory = MemoryProcess::open(&place);
    let emission = EmissionSocket { place: place.clone(), lasting: BTreeMap::new() };
    let mut world = World { signal, memory, emission, archive, queue, roots, watching: BTreeMap::new(), discovering: None };
    world.trace(Trace::Started(now()));
    world.bear_watches();
    world.bear_discovery();
    let mut manager = ProcessManager::default();
    for arrived in arrivals {
        match arrived {
            Arrived::Arrival(arrival) => manager.arrive(arrival, &mut world),
            Arrived::Listening => {
                world.bear_watches();
                world.bear_discovery();
            }
        }
    }
    manager.end_all(&mut world);
    ExitCode::SUCCESS
}

fn now() -> Moment {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    Moment(i64::try_from(since.as_nanos()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(row_count_option: Option<RowCount>, rollout_root_option: Option<FilePath>) -> Records {
        Records { observed_vector: Vec::new(), watch_vector: Vec::new(), row_count_option, thread_database_option: None, hook_name_vector: Vec::new(), rollout_root_option }
    }

    fn root() -> Option<FilePath> {
        Some(FilePath("/home/example/.codex/sessions".into()))
    }

    #[test]
    fn a_watch_waits_for_its_row_count_with_no_rollout_root() {
        assert_eq!(records(None, None).row_count(), None);
    }

    #[test]
    fn a_rollout_root_alone_gives_the_discovery_row_count() {
        assert_eq!(records(None, root()).row_count(), Some(RowCount(256)));
    }

    #[test]
    fn a_configured_row_count_is_kept_with_or_without_a_rollout_root() {
        assert_eq!(records(Some(RowCount(64)), None).row_count(), Some(RowCount(64)));
        assert_eq!(records(Some(RowCount(64)), root()).row_count(), Some(RowCount(64)));
    }
}
