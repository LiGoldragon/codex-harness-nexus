#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TranslatorSettings {
    pub text_limits: translator_serving::TextLimits,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum TranslatorBootstrapFailure {
    InvalidNodeLimit,
    InvalidDepthLimit,
    SettingsRuntimeUnavailable(signal_codex_harness_nexus::Diagnostic),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum TranslatorActorFailure {
    SettingsActorUnavailable(signal_codex_harness_nexus::Diagnostic),
    UnexpectedSettingsReply,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum MetaBudgetFailure {
    NodeLimitOutOfRange,
    DepthLimitOutOfRange,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SettingsCommand {
    ReadSettings,
    SetTextBudget(meta_signal_codex_harness_nexus::TextBudget),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SettingsReply {
    SettingsRead(TranslatorSettings),
    TextBudgetSet(meta_signal_codex_harness_nexus::Output),
}
#[rustfmt::skip]
pub trait TextLimitsValidating {
    fn valid_budget(
        &self,
    ) -> std::result::Result<
        translator_serving::ValidTextBudget,
        TranslatorBootstrapFailure,
    >;
}
#[rustfmt::skip]
pub trait Limiting {
    fn text_limits(&self) -> translator_serving::TextLimits;
}
#[rustfmt::skip]
pub trait MetaBudgeting {
    fn meta_budget(
        &self,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::TextBudget,
        MetaBudgetFailure,
    >;
}
#[rustfmt::skip]
pub trait TextBudgetInput {
    fn text_budget(&self) -> meta_signal_codex_harness_nexus::TextBudget;
}
#[rustfmt::skip]
pub trait ConfigurationReadoutInput {
    fn readout(&self) -> meta_signal_codex_harness_nexus::ConfigurationReadout;
}
#[rustfmt::skip]
pub trait BudgetMerging {
    fn with_text_budget<N: ConfigurationReadoutInput, O: TextBudgetInput>(
        input_0: N,
        input_1: O,
    ) -> meta_signal_codex_harness_nexus::ConfigurationReadout;
}
#[rustfmt::skip]
pub trait TextBudgetSetting {
    fn set_text_budget<N: TextBudgetInput>(
        &mut self,
        input: N,
    ) -> meta_signal_codex_harness_nexus::Output;
}
#[rustfmt::skip]
pub trait SettingsReading {
    fn current_settings(
        &self,
    ) -> std::result::Result<TranslatorSettings, TranslatorActorFailure>;
}
#[rustfmt::skip]
pub trait SettingsWriting {
    fn request_text_budget<N: TextBudgetInput>(
        &self,
        input: N,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::Output,
        TranslatorActorFailure,
    >;
}
#[rustfmt::skip]
pub trait SettingsCommandHandling {
    fn handle(&mut self, input: SettingsCommand) -> SettingsReply;
}
#[rustfmt::skip]
const _: () = {
    fn assert_textlimits_textlimitsvalidating<T: TextLimitsValidating>() {}
    let _ = assert_textlimits_textlimitsvalidating::<translator_serving::TextLimits>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_textbudget_limiting<T: Limiting>() {}
    let _ = assert_textbudget_limiting::<meta_signal_codex_harness_nexus::TextBudget>;
    fn assert_textbudget_textbudgetinput<T: TextBudgetInput>() {}
    let _ = assert_textbudget_textbudgetinput::<
        meta_signal_codex_harness_nexus::TextBudget,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_configurationreadout_configurationreadoutinput<
        T: ConfigurationReadoutInput,
    >() {}
    let _ = assert_configurationreadout_configurationreadoutinput::<
        meta_signal_codex_harness_nexus::ConfigurationReadout,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_validtextbudget_metabudgeting<T: MetaBudgeting>() {}
    let _ = assert_validtextbudget_metabudgeting::<translator_serving::ValidTextBudget>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_configurationreadout_budgetmerging<T: BudgetMerging>() {}
    let _ = assert_configurationreadout_budgetmerging::<
        meta_signal_codex_harness_nexus::ConfigurationReadout,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_translatorsettings_textbudgetsetting<T: TextBudgetSetting>() {}
    let _ = assert_translatorsettings_textbudgetsetting::<TranslatorSettings>;
    fn assert_translatorsettings_settingscommandhandling<T: SettingsCommandHandling>() {}
    let _ = assert_translatorsettings_settingscommandhandling::<TranslatorSettings>;
};
