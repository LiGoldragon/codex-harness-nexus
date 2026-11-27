#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    unused_extern_crates,
    clippy::type_complexity
)]
#[rustfmt::skip]
extern crate alloc;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Reporting {
    Observed,
    Unremembered,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Watching {
    Forgotten,
    Unfollowed,
    Unlocated,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Configuring {
    Configured,
    Unremembered,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Reported {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Subscribed(pub alloc::vec::Vec<harness::Observed>);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Configured(pub codex_harness::meta::Set);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReportProcess {
    pub caller: process::Caller,
    pub session_id: harness::SessionId,
    pub hook: codex_harness::signal::Hook,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ReportTaken {
    Answer(process::Answer),
    Observation(
        core::result::Result<codex_harness::memory::Observation, process::Unremembered>,
    ),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchProcess {
    pub session_id: harness::SessionId,
    pub rollout: codex_harness::memory::Rollout,
    pub read_from: codex_harness::ReadFrom,
    pub row_count: codex_harness::RowCount,
    pub thread_database_option: core::option::Option<
        codex_harness::meta::ThreadDatabase,
    >,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WatchTaken {
    RolloutFinding(
        core::result::Result<
            codex_harness::emission::RolloutFinding_Data,
            process::Unobserved,
        >,
    ),
    Answer(process::Answer),
    Following(
        core::result::Result<
            codex_harness::emission::Following_Data,
            process::Unobserved,
        >,
    ),
    Observation(
        core::result::Result<codex_harness::memory::Observation, process::Unremembered>,
    ),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SubscribeProcess {
    pub caller: process::Caller,
}
#[rustfmt::skip]
pub type SubscribeTaken = core::result::Result<
    codex_harness::memory::Recollection,
    process::Unremembered,
>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureProcess {
    pub caller: process::Caller,
    pub set: codex_harness::meta::Set,
}
#[rustfmt::skip]
pub type ConfigureTaken = process::Answer;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Process {
    Report(ReportProcess),
    Watch(WatchProcess),
    Subscribe(SubscribeProcess),
    Configure(ConfigureProcess),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    Report(Reporting),
    Watch(Watching),
    Subscribe,
    Configure(Configuring),
    Unwound,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Gift {
    Reported,
    Watch(codex_harness::memory::Watch),
    Observe(codex_harness::memory::Observe),
    Changed(codex_harness::signal::Changed_Data),
    FindRollout(codex_harness::emission::FindRollout_Data),
    Locate(codex_harness::memory::Locate),
    Follow(codex_harness::emission::Follow_Data),
    Recall(codex_harness::memory::Recall),
    Subscribed(Subscribed),
    Keep(codex_harness::memory::Keep),
    Configured(Configured),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Taken {
    Answer(process::Answer),
    Observation(
        core::result::Result<codex_harness::memory::Observation, process::Unremembered>,
    ),
    RolloutFinding(
        core::result::Result<
            codex_harness::emission::RolloutFinding_Data,
            process::Unobserved,
        >,
    ),
    Following(
        core::result::Result<
            codex_harness::emission::Following_Data,
            process::Unobserved,
        >,
    ),
    Recollection(
        core::result::Result<codex_harness::memory::Recollection, process::Unremembered>,
    ),
}
#[rustfmt::skip]
impl process::Givable<ReportProcess> for Reported {
    type Returned = ();
}
#[rustfmt::skip]
impl process::Givable<ReportProcess> for codex_harness::memory::Watch {
    type Returned = process::Answer;
}
#[rustfmt::skip]
impl process::Givable<ReportProcess> for codex_harness::memory::Observe {
    type Returned = core::result::Result<
        codex_harness::memory::Observation,
        process::Unremembered,
    >;
}
#[rustfmt::skip]
impl process::Givable<ReportProcess> for codex_harness::signal::Changed_Data {
    type Returned = ();
}
#[rustfmt::skip]
impl process::Givable<WatchProcess> for codex_harness::emission::FindRollout_Data {
    type Returned = core::result::Result<
        codex_harness::emission::RolloutFinding_Data,
        process::Unobserved,
    >;
}
#[rustfmt::skip]
impl process::Givable<WatchProcess> for codex_harness::memory::Locate {
    type Returned = process::Answer;
}
#[rustfmt::skip]
impl process::Givable<WatchProcess> for codex_harness::emission::Follow_Data {
    type Returned = core::result::Result<
        codex_harness::emission::Following_Data,
        process::Unobserved,
    >;
}
#[rustfmt::skip]
impl process::Givable<WatchProcess> for codex_harness::memory::Observe {
    type Returned = core::result::Result<
        codex_harness::memory::Observation,
        process::Unremembered,
    >;
}
#[rustfmt::skip]
impl process::Givable<WatchProcess> for codex_harness::signal::Changed_Data {
    type Returned = ();
}
#[rustfmt::skip]
impl process::Givable<SubscribeProcess> for codex_harness::memory::Recall {
    type Returned = core::result::Result<
        codex_harness::memory::Recollection,
        process::Unremembered,
    >;
}
#[rustfmt::skip]
impl process::Givable<SubscribeProcess> for Subscribed {
    type Returned = ();
}
#[rustfmt::skip]
impl process::Givable<ConfigureProcess> for codex_harness::memory::Keep {
    type Returned = process::Answer;
}
#[rustfmt::skip]
impl process::Givable<ConfigureProcess> for Configured {
    type Returned = ();
}
#[rustfmt::skip]
impl core::convert::From<Reported> for Gift {
    fn from(_: Reported) -> Gift {
        Gift::Reported
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Watch> for Gift {
    fn from(gift: codex_harness::memory::Watch) -> Gift {
        Gift::Watch(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Observe> for Gift {
    fn from(gift: codex_harness::memory::Observe) -> Gift {
        Gift::Observe(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::signal::Changed_Data> for Gift {
    fn from(gift: codex_harness::signal::Changed_Data) -> Gift {
        Gift::Changed(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::emission::FindRollout_Data> for Gift {
    fn from(gift: codex_harness::emission::FindRollout_Data) -> Gift {
        Gift::FindRollout(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Locate> for Gift {
    fn from(gift: codex_harness::memory::Locate) -> Gift {
        Gift::Locate(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::emission::Follow_Data> for Gift {
    fn from(gift: codex_harness::emission::Follow_Data) -> Gift {
        Gift::Follow(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Recall> for Gift {
    fn from(gift: codex_harness::memory::Recall) -> Gift {
        Gift::Recall(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<Subscribed> for Gift {
    fn from(gift: Subscribed) -> Gift {
        Gift::Subscribed(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Keep> for Gift {
    fn from(gift: codex_harness::memory::Keep) -> Gift {
        Gift::Keep(gift)
    }
}
#[rustfmt::skip]
impl core::convert::From<Configured> for Gift {
    fn from(gift: Configured) -> Gift {
        Gift::Configured(gift)
    }
}
#[rustfmt::skip]
impl process::Unwindable for Outcome {
    fn unwound() -> Self {
        Outcome::Unwound
    }
}
#[rustfmt::skip]
impl process::Ownable for Process {
    fn owner(&self) -> process::Owner {
        match self {
            Process::Watch(_) => process::Owner::Memory,
            Process::Report(_) | Process::Subscribe(_) | Process::Configure(_) => {
                process::Owner::Signal
            }
        }
    }
}
#[rustfmt::skip]
impl process::Dispatchable for Process {
    type Taken = Taken;
    type Gift = Gift;
    type Outcome = Outcome;
    fn dispatch_begin(
        &mut self,
        receive: impl FnMut(Gift),
    ) -> core::option::Option<Outcome> {
        match self {
            Process::Report(report) => {
                process::Processable::begin(
                        report,
                        process::Giver::<ReportProcess, _>::from(receive),
                    )
                    .map(Outcome::Report)
            }
            Process::Watch(watch) => {
                process::Processable::begin(
                        watch,
                        process::Giver::<WatchProcess, _>::from(receive),
                    )
                    .map(Outcome::Watch)
            }
            Process::Subscribe(subscribe) => {
                process::Processable::begin(
                        subscribe,
                        process::Giver::<SubscribeProcess, _>::from(receive),
                    )
                    .map(|()| Outcome::Subscribe)
            }
            Process::Configure(configure) => {
                process::Processable::begin(
                        configure,
                        process::Giver::<ConfigureProcess, _>::from(receive),
                    )
                    .map(Outcome::Configure)
            }
        }
    }
    fn dispatch_take(
        &mut self,
        taken: Taken,
        receive: impl FnMut(Gift),
    ) -> core::option::Option<Outcome> {
        match (self, taken) {
            (Process::Report(report), Taken::Answer(carried)) => {
                process::Processable::take(
                        report,
                        ReportTaken::Answer(carried),
                        process::Giver::<ReportProcess, _>::from(receive),
                    )
                    .map(Outcome::Report)
            }
            (Process::Report(report), Taken::Observation(carried)) => {
                process::Processable::take(
                        report,
                        ReportTaken::Observation(carried),
                        process::Giver::<ReportProcess, _>::from(receive),
                    )
                    .map(Outcome::Report)
            }
            (Process::Watch(watch), Taken::RolloutFinding(carried)) => {
                process::Processable::take(
                        watch,
                        WatchTaken::RolloutFinding(carried),
                        process::Giver::<WatchProcess, _>::from(receive),
                    )
                    .map(Outcome::Watch)
            }
            (Process::Watch(watch), Taken::Answer(carried)) => {
                process::Processable::take(
                        watch,
                        WatchTaken::Answer(carried),
                        process::Giver::<WatchProcess, _>::from(receive),
                    )
                    .map(Outcome::Watch)
            }
            (Process::Watch(watch), Taken::Following(carried)) => {
                process::Processable::take(
                        watch,
                        WatchTaken::Following(carried),
                        process::Giver::<WatchProcess, _>::from(receive),
                    )
                    .map(Outcome::Watch)
            }
            (Process::Watch(watch), Taken::Observation(carried)) => {
                process::Processable::take(
                        watch,
                        WatchTaken::Observation(carried),
                        process::Giver::<WatchProcess, _>::from(receive),
                    )
                    .map(Outcome::Watch)
            }
            (Process::Subscribe(subscribe), Taken::Recollection(carried)) => {
                process::Processable::take(
                        subscribe,
                        carried,
                        process::Giver::<SubscribeProcess, _>::from(receive),
                    )
                    .map(|()| Outcome::Subscribe)
            }
            (Process::Configure(configure), Taken::Answer(carried)) => {
                process::Processable::take(
                        configure,
                        carried,
                        process::Giver::<ConfigureProcess, _>::from(receive),
                    )
                    .map(Outcome::Configure)
            }
            _ => core::option::Option::None,
        }
    }
}
