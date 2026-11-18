//! The Codex Harness Nexus's translator as the shared serving runs it: its
//! settings, its public and meta text over the Nexus's two contracts, the
//! text budget it carries into every configuration the Nexus reads out, and
//! its answer to each emission.

use codex_harness_nexus::CodexHarness;
use codex_harness_nexus::emission::{Emission, Emittable as _, Outcome};
use meta_signal_codex_harness_nexus as meta;
use shared_translator::Caller;
use signal::Frame;
use signal_codex_harness_nexus::{Input, Output};
use translator_serving::{
    EmissionAnswering, MetaTranslating, NexusFailure, PublicTranslating, Settling, TextBudgeting,
    Translator, TranslatorBootstrap, ValidTextBudget,
};

use crate::settings_actor::{
    BudgetMerging, Limiting, MetaBudgeting, SettingsReading, SettingsWriting, TextLimitsValidating,
    TranslatorBootstrapFailure, TranslatorSettingsActor,
};

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
    pub settings: TranslatorSettingsActor,
}

/// The translator a bootstrap starts: ready at once, its text budget starting
/// at the bootstrap's text limits, which the pair launcher takes from its
/// configuration.
impl TryFrom<&TranslatorBootstrap> for CodexHarnessTranslator {
    type Error = TranslatorBootstrapFailure;

    fn try_from(bootstrap: &TranslatorBootstrap) -> Result<Self, Self::Error> {
        Ok(Self {
            settings: <TranslatorSettingsActor as TryFrom<&TranslatorBootstrap>>::try_from(
                bootstrap,
            )?,
        })
    }
}

impl Translator for CodexHarnessTranslator {
    type Component = CodexHarness;
    type Ready = TranslatorSettings;

    fn ready(&self) -> Option<TranslatorSettings> {
        // The serving contract has only Option for readiness, so actor
        // unavailability remains not-ready here rather than a success or a
        // fabricated text budget.
        let settings = self.settings.current_settings().ok()?;
        let text_budget = settings.text_limits.valid_budget().ok()?;
        Some(TranslatorSettings { text_budget })
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
                meta::Set::TextBudget(budget) => {
                    Settling::Answered(match self.settings.request_text_budget(budget.clone()) {
                        Ok(output) => output,
                        Err(failure) => meta::Output::ConfigureRefused(
                            meta::ConfigurationRefusal::TranslatorUnavailable(meta::Diagnostic(
                                failure.to_string(),
                            )),
                        ),
                    })
                }
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
        let settings = match self.settings.current_settings() {
            Ok(settings) => settings,
            Err(failure) => {
                return Ok(meta::Output::ConfigureRefused(
                    meta::ConfigurationRefusal::TranslatorUnavailable(meta::Diagnostic(
                        failure.to_string(),
                    )),
                ));
            }
        };
        let valid_budget = settings
            .text_limits
            .valid_budget()
            .map_err(|failure| NexusFailure(failure.to_string()))?;
        let text_budget = valid_budget
            .meta_budget()
            .map_err(|failure| NexusFailure(failure.to_string()))?;
        Ok(match response {
            meta::Output::Subscribed(Frame::Opened(readout)) => meta::Output::Subscribed(
                Frame::Opened(BudgetMerging::with_text_budget(readout, text_budget)),
            ),
            meta::Output::Subscribed(Frame::Lagged(readout)) => meta::Output::Subscribed(
                Frame::Lagged(BudgetMerging::with_text_budget(readout, text_budget)),
            ),
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
