//! What the generator would write from the Signal, Memory, Emission and Process
//! ethos of the Codex Harness Nexus. Written by hand while ethos-zero knows no
//! Process root; every path names core or alloc, so it builds with no_std.

use alloc::string::String;
use alloc::vec::Vec;
use core::option::Option;
use core::result::Result;

use flow::FlowId;
use harness::{Event, FilePath, Observed, Response, SessionId, State, Tokens, Trigger};
use process::{Givable, Giving, Ownable, Owner, Processable, Unwindable};

macro_rules! generated {
    ($($item:item)*) => { $(
        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
        #[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
        $item
    )* };
}

generated! {
    // Signal
    pub struct ProcessId(pub i64);
    pub struct UserId(pub i64);
    pub struct Caller { pub process_id: ProcessId, pub user_id: UserId }
    pub enum Source { Startup, Resume, Compact }
    pub struct SessionStart { pub session_id: SessionId, pub file_path_option: Option<FilePath>, pub source: Source }
    pub struct PostCompact { pub session_id: SessionId, pub trigger: Trigger }
    pub enum Hook {
        SessionStart(SessionStart),
        PermissionRequest(SessionId),
        PostToolUse(SessionId),
        PostCompact(PostCompact),
    }
    pub struct Reported;
    pub struct Changed { pub flow_id: FlowId, pub event: Event, pub state: State }
    // Listening is the library's, one input every Nexus has: its translator has begun to listen.
    // It is answered at once by Acknowledged.
    pub enum Input { Report(Hook), Subscribe, Listening }
    pub struct Call { pub caller: Caller, pub input: Input }
    pub struct Subscribed(pub Vec<Observed>);
    pub enum Output { Reported, Subscribed(Subscribed), Changed(Changed), Acknowledged }

    // Meta signal
    pub struct HookName(pub String);
    pub struct ThreadDatabase(pub String);
    pub enum Set { Hooks(Vec<HookName>), RemoveHook(HookName), RowCount(RowCount), ThreadDatabase(ThreadDatabase) }
    pub enum MetaInput { Configure(Set) }
    pub enum MetaOutput { Configured(Set) }
    pub struct MetaCall { pub caller: Caller, pub meta_input: MetaInput }

    // Library: the changes and their types, until the Memory page places them
    pub struct Offset(pub i64);
    pub enum ReadFrom { FileEnd, Offset(Offset) }
    pub struct RowCount(pub i64);

    // Memory
    pub enum Rollout { Located(FilePath), Unlocated }
    pub struct Watch { pub flow_id: FlowId, pub session_id: SessionId, pub rollout: Rollout, pub read_from: ReadFrom }
    pub struct Observe { pub flow_id: FlowId, pub event_vector: Vec<Event>, pub offset_option: Option<Offset> }
    pub struct Observation(pub Vec<Changed>);
    pub enum Answer { Succeeded, Failed }
    pub struct Locate { pub flow_id: FlowId, pub file_path: FilePath }
    pub struct Recall;
    pub struct Recollection(pub Vec<Observed>);
    pub struct Configure(pub Set);
    pub struct Remembered {
        pub observed_vector: Vec<Observed>,
        pub watch_vector: Vec<Watch>,
        pub row_count_option: Option<RowCount>,
        pub thread_database_option: Option<ThreadDatabase>,
        pub hook_name_vector: Vec<HookName>,
    }

    // Emission
    pub struct Follow { pub file_path: FilePath, pub read_from: ReadFrom, pub row_count: RowCount }
    pub struct Following { pub row_vector: Vec<Row>, pub offset: Offset }
    pub struct FindRollout { pub thread_database: ThreadDatabase, pub session_id: SessionId }
    pub enum RolloutFinding { Found(FilePath), Unfound }
    pub enum Row { TaskStarted, TokenCount(Option<Tokens>), TaskComplete(Option<Response>), TurnAborted, Other }
    pub enum Unobserved { Unsubmitted, Unanswered }
    // What the emission socket carries: each emission on a connection of its own.
    pub enum Emission { Follow(Follow), FindRollout(FindRollout) }

    // Process
    pub struct ReportProcess { pub caller: Caller, pub hook: Hook }
    pub struct WatchProcess {
        pub flow_id: FlowId,
        pub session_id: SessionId,
        pub rollout: Rollout,
        pub read_from: ReadFrom,
        pub row_count: RowCount,
        pub thread_database_option: Option<ThreadDatabase>,
    }
    pub enum ReportTaken { Answer(Answer), Observation(Observation) }
    pub enum WatchTaken {
        RolloutFinding(Result<RolloutFinding, Unobserved>),
        Answer(Answer),
        Following(Result<Following, Unobserved>),
        Observation(Observation),
    }
    pub enum Reporting { Observed, Unremembered }
    pub enum Watching { Forgotten, Unfollowed, Unlocated }
    pub struct SubscribeProcess { pub caller: Caller }
    pub enum SubscribeTaken { Recollection(Recollection) }
    pub struct ConfigureProcess { pub caller: Caller, pub set: Set }
    pub enum ConfigureTaken { Answer(Answer) }
    pub struct Configured(pub Set);
    pub enum Configuring { Configured, Unremembered }

    // The enums over the Nexus's processes
    pub enum Process { Report(ReportProcess), Watch(WatchProcess), Subscribe(SubscribeProcess), Configure(ConfigureProcess) }
    pub enum Gift {
        Reported,
        Subscribed(Subscribed),
        Changed(Changed),
        Watch(Watch),
        Observe(Observe),
        Locate(Locate),
        Recall(Recall),
        Configure(Configure),
        Configured(Configured),
        Follow(Follow),
        FindRollout(FindRollout),
    }
    pub enum Taken {
        Answer(Answer),
        Observation(Observation),
        Recollection(Recollection),
        Following(Result<Following, Unobserved>),
        RolloutFinding(Result<RolloutFinding, Unobserved>),
    }
    // One variant for each process, named for it; a process that ends one way carries nothing.
    pub enum Outcome { Report(Reporting), Watch(Watching), Subscribe, Configure(Configuring), Unwound }
}

macro_rules! givable {
    ($($gift:ident => $returned:ty),* $(,)?) => { $(
        impl Givable for $gift { type Returned = $returned; }
        impl From<$gift> for Gift { fn from(gift: $gift) -> Gift { Gift::$gift(gift) } }
    )* };
}

// An output that carries nothing is a variant of the Gift that carries nothing.
impl Givable for Reported {
    type Returned = ();
}
impl From<Reported> for Gift {
    fn from(Reported: Reported) -> Gift {
        Gift::Reported
    }
}

givable! {
    Subscribed => (),
    Recall => Recollection,
    Configure => Answer,
    Configured => (),
    Changed => (),
    Watch => Answer,
    Observe => Observation,
    Locate => Answer,
    Follow => Result<Following, Unobserved>,
    FindRollout => Result<RolloutFinding, Unobserved>,
}

impl Unwindable for Outcome {
    fn unwound() -> Self {
        Outcome::Unwound
    }
}

impl Ownable for Process {
    fn owner(&self) -> Owner {
        match self {
            Process::Watch(_) => Owner::Memory,
            Process::Report(_) | Process::Subscribe(_) | Process::Configure(_) => Owner::Signal,
        }
    }
}

impl Processable for Process {
    type Taken = Taken;
    type Gift = Gift;
    type Outcome = Outcome;

    fn begin<N: Giving<Gift>>(&mut self, giving: N) -> Option<Outcome> {
        match self {
            Process::Report(report) => report.begin(giving).map(Outcome::Report),
            Process::Watch(watch) => watch.begin(giving).map(Outcome::Watch),
            Process::Subscribe(subscribe) => subscribe.begin(giving).map(|()| Outcome::Subscribe),
            Process::Configure(configure) => configure.begin(giving).map(Outcome::Configure),
        }
    }

    fn take<N: Giving<Gift>>(&mut self, taken: Taken, giving: N) -> Option<Outcome> {
        match (self, taken) {
            (Process::Report(report), Taken::Answer(answer)) => report.take(ReportTaken::Answer(answer), giving).map(Outcome::Report),
            (Process::Report(report), Taken::Observation(observation)) => report.take(ReportTaken::Observation(observation), giving).map(Outcome::Report),
            (Process::Watch(watch), Taken::RolloutFinding(finding)) => watch.take(WatchTaken::RolloutFinding(finding), giving).map(Outcome::Watch),
            (Process::Watch(watch), Taken::Answer(answer)) => watch.take(WatchTaken::Answer(answer), giving).map(Outcome::Watch),
            (Process::Watch(watch), Taken::Following(following)) => watch.take(WatchTaken::Following(following), giving).map(Outcome::Watch),
            (Process::Watch(watch), Taken::Observation(observation)) => watch.take(WatchTaken::Observation(observation), giving).map(Outcome::Watch),
            (Process::Subscribe(subscribe), Taken::Recollection(recollection)) => subscribe.take(SubscribeTaken::Recollection(recollection), giving).map(|()| Outcome::Subscribe),
            (Process::Configure(configure), Taken::Answer(answer)) => configure.take(ConfigureTaken::Answer(answer), giving).map(Outcome::Configure),
            _ => None,
        }
    }
}
