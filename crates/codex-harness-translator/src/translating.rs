//! The Codex Harness Nexus's translator as the shared serving runs it: its
//! settings, its public and meta text over the Nexus's two contracts, the
//! text budget it carries into every configuration the Nexus reads out, and
//! its answer to each emission.

use std::sync::{PoisonError, RwLock};

use codex_harness_nexus::CodexHarness;
use codex_harness_nexus::emission::{Emission, Emittable as _, Outcome};
use meta_signal_codex_harness_nexus as meta;
use shared_translator::Caller;
use signal::Frame;
use signal_codex_harness_nexus::{Input, Output};
use translator_serving::{
    DepthLimit, EmissionAnswering, MetaTranslating, NexusFailure, NodeLimit, PublicTranslating,
    Settling, TextBudgeting, TextLimits, Translator, TranslatorBootstrap, ValidTextBudget,
};

use crate::settings_actor::{TextLimitsValidating, TranslatorBootstrapFailure};

/// The translator's settings: the budget every ordinary request's text is
/// read within, the translator's own setting on the meta socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TranslatorSettings {
    pub text_budget: ValidTextBudget,
}

impl TextBudgeting for TranslatorSettings {
    fn text_budget(&self) -> ValidTextBudget {
        self.text_budget
    }
}

/// The Codex Harness Nexus's translator process: it speaks text for the
/// Nexus, and carries every emission of the Nexus into the world. Its
/// settings change when the meta socket sets one.
pub struct CodexHarnessTranslator {
    pub settings: RwLock<TranslatorSettings>,
}

/// The translator a bootstrap starts: ready at once, its text budget starting
/// at the bootstrap's text limits, which the pair launcher takes from its
/// configuration.
impl TryFrom<&TranslatorBootstrap> for CodexHarnessTranslator {
    type Error = TranslatorBootstrapFailure;

    fn try_from(bootstrap: &TranslatorBootstrap) -> Result<Self, Self::Error> {
        let text_budget = bootstrap.text_limits.valid_budget()?;
        Ok(Self {
            settings: RwLock::new(TranslatorSettings { text_budget }),
        })
    }
}

/// The limits a meta text budget names, as the shared serving validates
/// them.
pub trait Limiting {
    fn text_limits(&self) -> TextLimits;
}

impl Limiting for meta::TextBudget {
    fn text_limits(&self) -> TextLimits {
        TextLimits {
            node_limit: NodeLimit(self.node_limit.0),
            depth_limit: DepthLimit(self.depth_limit.0),
        }
    }
}

/// A budget the shared serving validated, as the meta contract names it.
pub trait MetaBudgeting {
    fn meta_budget(&self) -> meta::TextBudget;
}

impl MetaBudgeting for ValidTextBudget {
    fn meta_budget(&self) -> meta::TextBudget {
        meta::TextBudget {
            node_limit: meta::NodeLimit(
                i64::try_from(self.nodes).expect("a valid node limit fits i64"),
            ),
            depth_limit: meta::DepthLimit(
                i64::try_from(self.depth).expect("a valid depth limit fits i64"),
            ),
        }
    }
}

/// A configuration the Nexus read out, with the translator's own setting
/// as it stands: the Nexus holds no text budget, so the readout it sends
/// has none until the translator writes its own in.
pub trait BudgetMerging {
    fn with_text_budget(self, text_budget: meta::TextBudget) -> Self;
}

impl BudgetMerging for meta::ConfigurationReadout {
    fn with_text_budget(self, text_budget: meta::TextBudget) -> Self {
        Self {
            configuration: meta::Configuration {
                text_budget_option: Some(text_budget),
                ..self.configuration
            },
            awaited_directories: self.awaited_directories,
        }
    }
}

/// The translator's own setting, set on the meta socket.
pub trait TextBudgetSetting {
    /// Sets the budget every later ordinary request is read within, or
    /// refuses a budget out of Signal's bounds.
    fn set_text_budget(&self, budget: meta::TextBudget) -> meta::Output;
}

impl TextBudgetSetting for CodexHarnessTranslator {
    fn set_text_budget(&self, budget: meta::TextBudget) -> meta::Output {
        match ValidTextBudget::try_from(&budget.text_limits()) {
            Ok(text_budget) => {
                let mut settings = self
                    .settings
                    .write()
                    .unwrap_or_else(PoisonError::into_inner);
                settings.text_budget = text_budget;
                meta::Output::Configured(meta::Set::TextBudget(budget))
            }
            Err(_) => meta::Output::ConfigureRefused(
                meta::ConfigurationRefusal::InvalidTextBudget(budget),
            ),
        }
    }
}

impl Translator for CodexHarnessTranslator {
    type Component = CodexHarness;
    type Ready = TranslatorSettings;

    fn ready(&self) -> Option<TranslatorSettings> {
        Some(self.current_settings())
    }
}

/// The translator's settings as they stand.
pub trait SettingsReading {
    fn current_settings(&self) -> TranslatorSettings;
}

impl SettingsReading for CodexHarnessTranslator {
    fn current_settings(&self) -> TranslatorSettings {
        *self.settings.read().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The ordinary lane carries the public contract's own query: the Nexus's
/// public socket admits this translator alone and answers every query it
/// carries, so the caller is answered for here and goes no further.
impl PublicTranslating for CodexHarnessTranslator {
    type PublicInput = Input;
    type PublicOutput = Output;
    type PublicCall = Input;
    type PublicResponse = Output;

    fn public_call(_caller: Caller, input: Input) -> Input {
        input
    }

    fn public_answered(response: Output) -> Result<Output, NexusFailure> {
        Ok(response)
    }
}

/// The text budget is the translator's own setting, answered here and
/// carried into every subscription open; every other setting, and the
/// subscription, is the Nexus's, carried to its meta socket as it was read,
/// each answer written back as it came, a configuration it reads out with
/// the translator's text budget written in.
impl MetaTranslating for CodexHarnessTranslator {
    type MetaInput = meta::Input;
    type MetaOutput = meta::Output;
    type NexusMetaInput = meta::Input;
    type MetaCall = meta::Input;
    type MetaResponse = meta::Output;

    fn settle(&self, input: meta::Input) -> Settling<meta::Output, meta::Input> {
        match input {
            meta::Input::Configure(setting) => match setting {
                meta::Set::TextBudget(budget) => Settling::Answered(self.set_text_budget(budget)),
                nexus_setting @ (meta::Set::FlowsRoot(_)
                | meta::Set::IdentityProducer(_)
                | meta::Set::SessionsDirectories(_)
                | meta::Set::ThreadDatabases(_)
                | meta::Set::SubscriberCapacity(_)) => {
                    Settling::Forwarded(meta::Input::Configure(nexus_setting))
                }
            },
            meta::Input::Subscribe => Settling::Subscribed(meta::Input::Subscribe),
        }
    }

    fn meta_call(_caller: Caller, input: meta::Input) -> meta::Input {
        input
    }

    fn meta_answered(&self, response: meta::Output) -> Result<meta::Output, NexusFailure> {
        let text_budget = self.current_settings().text_budget.meta_budget();
        Ok(match response {
            meta::Output::Subscribed(Frame::Opened(readout)) => {
                meta::Output::Subscribed(Frame::Opened(readout.with_text_budget(text_budget)))
            }
            meta::Output::Subscribed(Frame::Lagged(readout)) => {
                meta::Output::Subscribed(Frame::Lagged(readout.with_text_budget(text_budget)))
            }
            response @ (meta::Output::Configured(_)
            | meta::Output::ConfigureRefused(_)
            | meta::Output::Subscribed(Frame::Changed(_))) => response,
        })
    }

    /// A text budget set is a change of the configuration every open
    /// subscription follows; a refused one changed nothing.
    fn announced(&self, answered: &meta::Output) -> Option<meta::Output> {
        match answered {
            meta::Output::Configured(set @ meta::Set::TextBudget(_)) => {
                Some(meta::Output::Subscribed(Frame::Changed(
                    meta::ConfigurationChange::Set(set.clone()),
                )))
            }
            _ => None,
        }
    }
}

/// Each emission is one typed value; this is the one place that turns it
/// into its call, and the call's outcome into the value sent back.
impl EmissionAnswering for CodexHarnessTranslator {
    type Emission = Emission;
    type Outcome = Outcome;

    fn emitted(&self, _ready: &TranslatorSettings, emission: Emission) -> Outcome {
        match emission {
            Emission::FindRollout(session) => Outcome::RolloutFinding(session.emit(self)),
            Emission::ClaimIdentity(claim) => Outcome::IdentityClaim(claim.emit(self)),
            Emission::ReadRollout(watch) => Outcome::RolloutReading(watch.emit(self)),
            Emission::IdentifyRollout(path) => Outcome::RolloutIdentity(path.emit(self)),
        }
    }
}
