#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RolloutByte(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RolloutLength(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RolloutBytes(pub std::vec::Vec<RolloutByte>);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RowText(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Row {
    TaskStarted,
    TokenCount(std::option::Option<harness::Tokens>),
    TaskComplete(std::option::Option<harness::Response>),
    TurnAborted,
    Other,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RolloutShorterThanOffset_Data {
    pub rollout_length: RolloutLength,
    pub byte_offset: signal_codex_harness_nexus::ByteOffset,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RolloutReadFailure {
    InvalidByteValue,
    NegativeOffset(signal_codex_harness_nexus::ByteOffset),
    OffsetOverflow,
    RolloutShorterThanOffset(RolloutShorterThanOffset_Data),
}
#[rustfmt::skip]
pub trait Interpretable {
    fn row(&self) -> std::result::Result<Row, signal_codex_harness_nexus::ParseFailure>;
}
#[rustfmt::skip]
pub trait ByteOffsetInput {
    fn byte_offset(&self) -> signal_codex_harness_nexus::ByteOffset;
}
#[rustfmt::skip]
pub trait ReadingRows {
    fn rows<N: ByteOffsetInput>(
        &self,
        input: N,
    ) -> codex_harness_nexus::emission::RolloutReading_Data;
}
#[rustfmt::skip]
const _: () = {
    fn assert_row_observable<T: harness::Observable>() {}
    let _ = assert_row_observable::<Row>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_byteoffset_byteoffsetinput<T: ByteOffsetInput>() {}
    let _ = assert_byteoffset_byteoffsetinput::<signal_codex_harness_nexus::ByteOffset>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_rowtext_interpretable<T: Interpretable>() {}
    let _ = assert_rowtext_interpretable::<RowText>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_rolloutbytes_readingrows<T: ReadingRows>() {}
    let _ = assert_rolloutbytes_readingrows::<RolloutBytes>;
};
