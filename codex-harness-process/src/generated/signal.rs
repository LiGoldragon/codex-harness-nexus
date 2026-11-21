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
pub enum Source {
    Startup,
    Resume,
    Compact,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SessionStart_Data {
    pub session_id: harness::SessionId,
    pub file_path_option: core::option::Option<harness::FilePath>,
    pub source: Source,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PostCompact_Data {
    pub session_id: harness::SessionId,
    pub trigger: harness::Trigger,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Hook {
    SessionStart(SessionStart_Data),
    PermissionRequest(harness::SessionId),
    PostToolUse(harness::SessionId),
    PostCompact(PostCompact_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Input {
    Report(Hook),
    Subscribe,
    Listening,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Changed_Data {
    pub flow_id: flow::FlowId,
    pub event: harness::Event,
    pub state: harness::State,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Output {
    Reported,
    Subscribed(alloc::vec::Vec<harness::Observed>),
    Acknowledged,
    Changed(Changed_Data),
}
#[rustfmt::skip]
#[allow(clippy::result_large_err, clippy::ptr_arg, clippy::borrowed_box)]
pub trait Answering {
    fn report(&self, hook: &Hook) -> core::result::Result<(), Output>;
    fn subscribe(
        &self,
    ) -> core::result::Result<alloc::vec::Vec<harness::Observed>, Output>;
    fn listening(&self) -> core::result::Result<(), Output>;
}
#[rustfmt::skip]
pub trait Answerable {
    type Answer;
    fn answer<Nexus: Answering>(&self, nexus: &Nexus) -> Self::Answer;
}
#[rustfmt::skip]
impl Answerable for Input {
    type Answer = Output;
    fn answer<Nexus: Answering>(&self, nexus: &Nexus) -> Output {
        match self {
            Input::Report(carried) => {
                nexus
                    .report(carried)
                    .map_or_else(|refusal| refusal, |()| Output::Reported)
            }
            Input::Subscribe => {
                nexus.subscribe().map_or_else(|refusal| refusal, Output::Subscribed)
            }
            Input::Listening => {
                nexus
                    .listening()
                    .map_or_else(|refusal| refusal, |()| Output::Acknowledged)
            }
        }
    }
}
