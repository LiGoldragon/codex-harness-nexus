#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlowsRoot(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct IdentityProducer(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ClaimIdentity_Data {
    pub session: harness::Session,
    pub flows_root: FlowsRoot,
    pub identity_producer: IdentityProducer,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Emission {
    FindRollout(harness::Session),
    ClaimIdentity(ClaimIdentity_Data),
    ReadRollout(memory::Watch),
    IdentifyRollout(harness::FilePath),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RolloutFinding_Data {
    Found(harness::FilePath),
    Unfound,
    LaneFailed(nexus_exchange::LaneFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum IdentityClaim_Data {
    Claimed(flow::FlowId),
    Unclaimed(signal_codex_harness_nexus::IdentityFailure),
    LaneFailed(nexus_exchange::LaneFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RolloutReading_Data_Read_Data {
    pub observation_vector: std::vec::Vec<signal_codex_harness_nexus::Observation>,
    pub byte_offset: signal_codex_harness_nexus::ByteOffset,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RolloutReading_Data {
    Read(RolloutReading_Data_Read_Data),
    Unreadable(signal_codex_harness_nexus::Diagnostic),
    LaneFailed(nexus_exchange::LaneFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RolloutIdentity_Data {
    Identified(harness::Session),
    Unidentified,
    LaneFailed(nexus_exchange::LaneFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    RolloutFinding(RolloutFinding_Data),
    IdentityClaim(IdentityClaim_Data),
    RolloutReading(RolloutReading_Data),
    RolloutIdentity(RolloutIdentity_Data),
    LaneFailed(nexus_exchange::LaneFailure),
}
#[rustfmt::skip]
pub trait Emitted {
    type Outcome;
}
#[rustfmt::skip]
pub trait Emittable<Translator>: Emitted {
    fn emit(&self, translator: &Translator) -> Self::Outcome;
}
#[rustfmt::skip]
impl Emitted for harness::Session {
    type Outcome = RolloutFinding_Data;
}
#[rustfmt::skip]
impl Emitted for ClaimIdentity_Data {
    type Outcome = IdentityClaim_Data;
}
#[rustfmt::skip]
impl Emitted for memory::Watch {
    type Outcome = RolloutReading_Data;
}
#[rustfmt::skip]
impl Emitted for harness::FilePath {
    type Outcome = RolloutIdentity_Data;
}
