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
pub struct Changed(pub codex_harness::signal::Changed);
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
    pub flow_id: flow::FlowId,
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
    Changed(Changed),
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
impl process::Givable for Reported {
    type Returned = ();
}
#[rustfmt::skip]
impl core::convert::From<Reported> for Gift {
    fn from(_: Reported) -> Gift {
        Gift::Reported
    }
}
#[rustfmt::skip]
impl process::Givable for codex_harness::memory::Watch {
    type Returned = process::Answer;
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Watch> for Gift {
    fn from(gift: codex_harness::memory::Watch) -> Gift {
        Gift::Watch(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for codex_harness::memory::Observe {
    type Returned = core::result::Result<
        codex_harness::memory::Observation,
        process::Unremembered,
    >;
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Observe> for Gift {
    fn from(gift: codex_harness::memory::Observe) -> Gift {
        Gift::Observe(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for Changed {
    type Returned = ();
}
#[rustfmt::skip]
impl core::convert::From<Changed> for Gift {
    fn from(gift: Changed) -> Gift {
        Gift::Changed(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for codex_harness::emission::FindRollout_Data {
    type Returned = core::result::Result<
        codex_harness::emission::RolloutFinding_Data,
        process::Unobserved,
    >;
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::emission::FindRollout_Data> for Gift {
    fn from(gift: codex_harness::emission::FindRollout_Data) -> Gift {
        Gift::FindRollout(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for codex_harness::memory::Locate {
    type Returned = process::Answer;
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Locate> for Gift {
    fn from(gift: codex_harness::memory::Locate) -> Gift {
        Gift::Locate(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for codex_harness::emission::Follow_Data {
    type Returned = core::result::Result<
        codex_harness::emission::Following_Data,
        process::Unobserved,
    >;
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::emission::Follow_Data> for Gift {
    fn from(gift: codex_harness::emission::Follow_Data) -> Gift {
        Gift::Follow(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for codex_harness::memory::Recall {
    type Returned = core::result::Result<
        codex_harness::memory::Recollection,
        process::Unremembered,
    >;
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Recall> for Gift {
    fn from(gift: codex_harness::memory::Recall) -> Gift {
        Gift::Recall(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for Subscribed {
    type Returned = ();
}
#[rustfmt::skip]
impl core::convert::From<Subscribed> for Gift {
    fn from(gift: Subscribed) -> Gift {
        Gift::Subscribed(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for codex_harness::memory::Keep {
    type Returned = process::Answer;
}
#[rustfmt::skip]
impl core::convert::From<codex_harness::memory::Keep> for Gift {
    fn from(gift: codex_harness::memory::Keep) -> Gift {
        Gift::Keep(gift)
    }
}
#[rustfmt::skip]
impl process::Givable for Configured {
    type Returned = ();
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
impl process::Processable for Process {
    type Taken = Taken;
    type Gift = Gift;
    type Outcome = Outcome;
    fn begin<N: process::Giving<Gift>>(
        &mut self,
        giving: N,
    ) -> core::option::Option<Outcome> {
        match self {
            Process::Report(report) => {
                process::Processable::begin(report, giving).map(Outcome::Report)
            }
            Process::Watch(watch) => {
                process::Processable::begin(watch, giving).map(Outcome::Watch)
            }
            Process::Subscribe(subscribe) => {
                process::Processable::begin(subscribe, giving)
                    .map(|()| Outcome::Subscribe)
            }
            Process::Configure(configure) => {
                process::Processable::begin(configure, giving).map(Outcome::Configure)
            }
        }
    }
    fn take<N: process::Giving<Gift>>(
        &mut self,
        taken: Taken,
        giving: N,
    ) -> core::option::Option<Outcome> {
        match (self, taken) {
            (Process::Report(report), Taken::Answer(carried)) => {
                process::Processable::take(report, ReportTaken::Answer(carried), giving)
                    .map(Outcome::Report)
            }
            (Process::Report(report), Taken::Observation(carried)) => {
                process::Processable::take(
                        report,
                        ReportTaken::Observation(carried),
                        giving,
                    )
                    .map(Outcome::Report)
            }
            (Process::Watch(watch), Taken::RolloutFinding(carried)) => {
                process::Processable::take(
                        watch,
                        WatchTaken::RolloutFinding(carried),
                        giving,
                    )
                    .map(Outcome::Watch)
            }
            (Process::Watch(watch), Taken::Answer(carried)) => {
                process::Processable::take(watch, WatchTaken::Answer(carried), giving)
                    .map(Outcome::Watch)
            }
            (Process::Watch(watch), Taken::Following(carried)) => {
                process::Processable::take(watch, WatchTaken::Following(carried), giving)
                    .map(Outcome::Watch)
            }
            (Process::Watch(watch), Taken::Observation(carried)) => {
                process::Processable::take(
                        watch,
                        WatchTaken::Observation(carried),
                        giving,
                    )
                    .map(Outcome::Watch)
            }
            (Process::Subscribe(subscribe), Taken::Recollection(carried)) => {
                process::Processable::take(subscribe, carried, giving)
                    .map(|()| Outcome::Subscribe)
            }
            (Process::Configure(configure), Taken::Answer(carried)) => {
                process::Processable::take(configure, carried, giving)
                    .map(Outcome::Configure)
            }
            _ => core::option::Option::None,
        }
    }
}
