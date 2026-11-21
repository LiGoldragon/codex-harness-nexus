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
use harness::{Observed, State, Tokens, Transitionable};
use process::{Arrival, Ended, Gave, Given, Identity, Living, Moment, Ownable, Owner, ProcessManager, Roots, Trace, Traced, Tracing};
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

/// The memory process: one change at a time, each kept before it is answered.
struct MemoryProcess {
    remembered: Remembered,
    file: std::path::PathBuf,
}

impl MemoryProcess {
    fn open(place: &Place) -> MemoryProcess {
        let file = place.path::<CodexHarness>(Placed::Memory(VERSION));
        let remembered = File::open(&file)
            .ok()
            .and_then(|file| Remembered::read_from(&mut BufReader::new(file)).ok().flatten())
            .unwrap_or(Remembered { observed_vector: Vec::new(), watch_vector: Vec::new(), row_count_option: None, thread_database_option: None, hook_name_vector: Vec::new() });
        MemoryProcess { remembered, file }
    }

    fn keep(&self) -> Answer {
        let temporary = self.file.with_extension("next");
        let written = File::create(&temporary)
            .and_then(|mut file| self.remembered.write_to(&mut file))
            .and_then(|()| std::fs::rename(&temporary, &self.file));
        match written {
            Ok(()) => Answer::Succeeded,
            Err(_) => Answer::Failed,
        }
    }

    fn holds_watch(&self, flow_id: &FlowId) -> bool {
        self.remembered.watch_vector.iter().any(|watch| watch.flow_id == *flow_id)
    }

    fn unlocated(&self, flow_id: &FlowId) -> bool {
        self.remembered.watch_vector.iter().any(|watch| watch.flow_id == *flow_id && watch.rollout == Rollout::Unlocated)
    }

    /// A Watch's process, when the row count it is born with is configured;
    /// the thread database is moved in with it, as it is configured.
    fn process_of(&self, watch: &Watch) -> Option<Process> {
        let row_count = self.remembered.row_count_option.clone()?;
        Some(Process::Watch(WatchProcess {
            flow_id: watch.flow_id.clone(),
            session: watch.session.clone(),
            rollout: watch.rollout.clone(),
            offset: watch.offset.clone(),
            row_count,
            thread_database_option: self.remembered.thread_database_option.clone(),
        }))
    }

    /// The rollout found for a Watch is kept with it.
    fn locate(&mut self, locate: Locate) -> Answer {
        for watch in self.remembered.watch_vector.iter_mut().filter(|watch| watch.flow_id == locate.flow_id) {
            watch.rollout = Rollout::Located(locate.file_path.clone());
        }
        self.keep()
    }

    fn watch(&mut self, watch: Watch) -> Answer {
        if self.remembered.watch_vector.contains(&watch) {
            return Answer::Succeeded;
        }
        self.remembered.watch_vector.retain(|held| held.flow_id != watch.flow_id);
        self.remembered.watch_vector.push(watch);
        self.keep()
    }

    fn configure(&mut self, Configure(set): Configure) -> Answer {
        match set {
            Set::Hooks(hook_name_vector) => self.remembered.hook_name_vector = hook_name_vector,
            Set::RemoveHook(hook_name) => self.remembered.hook_name_vector.retain(|held| *held != hook_name),
            Set::RowCount(row_count) => self.remembered.row_count_option = Some(row_count),
            Set::ThreadDatabase(thread_database) => self.remembered.thread_database_option = Some(thread_database),
        }
        self.keep()
    }

    /// Applies each event and keeps the offset. What a Watch observes applies
    /// only while its Watch is kept. A session that has ended is forgotten, its
    /// Observed and its Watch, once its changes are answered.
    fn observe(&mut self, observe: Observe) -> Observation {
        if observe.offset_option.is_some() && !self.holds_watch(&observe.flow_id) {
            return Observation(Vec::new());
        }
        let position = match self.remembered.observed_vector.iter().position(|observed| observed.flow_id == observe.flow_id) {
            Some(position) => position,
            None => {
                self.remembered.observed_vector.push(Observed { flow_id: observe.flow_id.clone(), state: State::Dry, tokens: Tokens(0), background_work_vector: vec![] });
                self.remembered.observed_vector.len() - 1
            }
        };
        let observed = &mut self.remembered.observed_vector[position];
        let changed = observe
            .event_vector
            .into_iter()
            .map(|event| {
                observed.after(event.clone());
                Changed { flow_id: observe.flow_id.clone(), event, state: observed.state.clone() }
            })
            .collect();
        if observed.state == State::Ended {
            self.remembered.observed_vector.remove(position);
            self.remembered.watch_vector.retain(|watch| watch.flow_id != observe.flow_id);
        } else if let Some(offset) = observe.offset_option {
            for watch in self.remembered.watch_vector.iter_mut().filter(|watch| watch.flow_id == observe.flow_id) {
                watch.offset = offset.clone();
            }
        }
        self.keep();
        Observation(changed)
    }
}

/// A subscriber's connection holds what changed until its Subscribed is written.
struct Subscription {
    pending: Vec<Output>,
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
}

impl SignalProcess {
    /// Each socket's calls become births: the identity is drawn here, and the
    /// call's connection is held under it until its process is born.
    fn listen<C, F>(place: &Place, placed: Placed, roots: Arc<Roots>, arrived: Arc<Mutex<BTreeMap<Identity, UnixStream>>>, queue: Queue, called: F) -> std::io::Result<()>
    where
        C: Framed + Send + 'static,
        F: Fn(C) -> Called + Send + 'static,
    {
        let path = place.path::<CodexHarness>(placed);
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let Ok(Some(call)) = C::read_from(&mut stream) else { continue };
                let arrival = match called(call) {
                    Called::Listening => {
                        let _ = Output::Acknowledged.write_to(&mut stream);
                        Arrived::Listening
                    }
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
        SignalProcess::listen(place, Placed::NexusSocket, roots.clone(), arrived.clone(), queue.clone(), |call: Call| match call.input {
            Input::Report(hook) => Called::Born(Process::Report(ReportProcess { caller: call.caller, hook })),
            Input::Subscribe => Called::Born(Process::Subscribe(SubscribeProcess { caller: call.caller })),
            Input::Listening => Called::Listening,
        })?;
        SignalProcess::listen(place, Placed::MetaSocket, roots, arrived.clone(), queue.clone(), |call: MetaCall| match call.meta_input {
            MetaInput::Configure(set) => Called::Born(Process::Configure(ConfigureProcess { caller: call.caller, set })),
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

    /// From the moment its recollection is taken, a subscriber misses nothing.
    fn subscribe(&mut self, identity: &Identity) {
        self.subscriptions.insert(identity.clone(), Subscription { pending: Vec::new(), answered: false });
    }

    fn subscribed(&mut self, identity: &Identity, output: Output) {
        self.answer(identity, &output);
        let pending = match self.subscriptions.get_mut(identity) {
            Some(subscription) => {
                subscription.answered = true;
                std::mem::take(&mut subscription.pending)
            }
            None => Vec::new(),
        };
        for output in pending {
            self.answer(identity, &output);
        }
    }

    fn publish(&mut self, output: Output) {
        for (identity, subscription) in self.subscriptions.iter_mut() {
            match (subscription.answered, self.connections.get_mut(identity)) {
                (true, Some(stream)) => {
                    let _ = output.write_to(stream);
                }
                _ => subscription.pending.push(output.clone()),
            }
        }
    }

    fn close(&mut self, identity: &Identity) {
        self.subscriptions.remove(identity);
        if let Some(stream) = self.connections.remove(identity) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
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
    fn find(&self, identity: Identity, find_rollout: FindRollout, queue: Queue) {
        let given = move |identity: Identity, taken| Arrived::Arrival(Arrival::Given(Given { identity, taken: Taken::RolloutFinding(taken) }));
        let Some(mut stream) = self.submit(&Emission::FindRollout(find_rollout)) else {
            let _ = queue.send(given(identity, Err(Unobserved::Unsubmitted)));
            return;
        };
        thread::spawn(move || {
            let finding = match RolloutFinding::read_from(&mut stream) {
                Ok(Some(finding)) => Ok(finding),
                _ => Err(Unobserved::Unanswered),
            };
            let _ = queue.send(given(identity, finding));
        });
    }

    fn follow(&mut self, identity: Identity, follow: Follow, queue: Queue) {
        let given = move |identity: &Identity, taken| Arrived::Arrival(Arrival::Given(Given { identity: identity.clone(), taken: Taken::Following(taken) }));
        let Some(stream) = self.submit(&Emission::Follow(follow)) else {
            let _ = queue.send(given(&identity, Err(Unobserved::Unsubmitted)));
            return;
        };
        let Ok(mut reading) = stream.try_clone() else { return };
        self.lasting.insert(identity.clone(), stream);
        thread::spawn(move || {
            loop {
                match Following::read_from(&mut reading) {
                    Ok(Some(following)) => {
                        if queue.send(given(&identity, Ok(following))).is_err() {
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
}

impl World {
    fn give_back(&self, identity: Identity, taken: Taken) {
        let _ = self.queue.send(Arrived::Arrival(Arrival::Given(Given { identity, taken })));
    }

    fn end(&self, identity: Identity, outcome: Outcome) {
        let _ = self.queue.send(Arrived::Arrival(Arrival::Ended(Ended { identity, outcome })));
    }

    /// Every Watch kept and without a living process is given its process, once
    /// its row count is configured: at the program's start, when the translator
    /// begins to listen, and when a Watch or the configuration is kept.
    fn bear_watches(&mut self) {
        let unborn: Vec<Process> = self
            .memory
            .remembered
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
            Gift::Subscribed(subscribed) => self.signal.subscribed(&identity, Output::Subscribed(subscribed)),
            Gift::Changed(changed) => self.signal.publish(Output::Changed(changed)),
            Gift::Configured(Configured(set)) => self.signal.answer(&identity, &MetaOutput::Configured(set)),
            Gift::Recall(Recall) => {
                self.signal.subscribe(&identity);
                let recollection = Recollection(self.memory.remembered.observed_vector.clone());
                self.give_back(identity, Taken::Recollection(recollection));
            }
            Gift::Watch(watch) => {
                let answer = self.memory.watch(watch);
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
            }
            Gift::Configure(configure) => {
                let answer = self.memory.configure(configure);
                self.give_back(identity, Taken::Answer(answer));
                self.bear_watches();
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
            Gift::Follow(follow) => self.emission.follow(identity, follow, self.queue.clone()),
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
                self.watching.retain(|_, identity| *identity != ended.identity);
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
    let mut world = World { signal, memory, emission, archive, queue, roots, watching: BTreeMap::new() };
    world.trace(Trace::Started(now()));
    world.bear_watches();
    let mut manager = ProcessManager::default();
    for arrived in arrivals {
        match arrived {
            Arrived::Arrival(arrival) => manager.arrive(arrival, &mut world),
            Arrived::Listening => world.bear_watches(),
        }
    }
    manager.end_all(&mut world);
    ExitCode::SUCCESS
}

fn now() -> Moment {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    Moment(i64::try_from(since.as_nanos()).unwrap_or(i64::MAX))
}
