//! The Codex Harness Nexus's translator as the shared serving runs it: its
//! settings, its public and meta text over the Nexus's two contracts, its
//! answer to each emission, and the bootstrap its Nexus is started with.

use std::sync::{PoisonError, RwLock};

use codex_harness_nexus::emission::{Emission, Emittable as _, Outcome};
use codex_harness_nexus::{CodexHarness, NexusBootstrap};
use meta_signal_codex_harness_nexus as meta;
use shared_translator::{Caller, PeerIdentity};
use signal_codex_harness_nexus::{Input, Output};
use translator_serving::{
    Answers, DepthLimit, EmissionAnswering, MetaTranslating, NexusFailure, NodeLimit,
    PairLaunching, Pairing, PublicTranslating, Settling, TextBudgeting, TextLimits, Translator,
    TranslatorBootstrap, ValidTextBudget,
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
    pub settings: RwLock<TranslatorSettings>,
}

/// The translator a bootstrap starts: ready at once, its text budget starting
/// at the bootstrap's text limits, which the pair launcher takes from its
/// configuration.
impl TryFrom<&TranslatorBootstrap> for CodexHarnessTranslator {
    type Error = &'static str;

    fn try_from(bootstrap: &TranslatorBootstrap) -> Result<Self, Self::Error> {
        let text_budget = ValidTextBudget::try_from(&bootstrap.text_limits)?;
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
        Some(*self.settings.read().unwrap_or_else(PoisonError::into_inner))
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

    /// A Subscribe is followed, each frame of its stream written back as it
    /// comes; a Report is answered once.
    fn answers(input: &Input) -> Answers {
        match input {
            Input::Subscribe => Answers::Followed,
            Input::Report(_) => Answers::Once,
        }
    }

    fn public_call(_caller: Caller, input: Input) -> Input {
        input
    }

    fn public_answered(response: Output) -> Result<Output, NexusFailure> {
        Ok(response)
    }
}

/// The text budget is the translator's own setting, answered here; every
/// other setting, and the subscription, is the Nexus's, carried to its meta
/// socket as it was read and its answer written back as it came.
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
            meta::Input::Subscribe => Settling::Forwarded(meta::Input::Subscribe),
        }
    }

    fn meta_call(_caller: Caller, input: meta::Input) -> meta::Input {
        input
    }

    fn meta_answered(response: meta::Output) -> Result<meta::Output, NexusFailure> {
        Ok(response)
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

/// The Nexus is started knowing its place, its admission and the translator
/// process it is paired with.
impl PairLaunching for CodexHarnessTranslator {
    type NexusBootstrap = NexusBootstrap;

    fn nexus_bootstrap(
        pairing: &Pairing,
        translator: PeerIdentity,
    ) -> Result<NexusBootstrap, Box<dyn std::error::Error>> {
        Ok(NexusBootstrap {
            place: pairing.place.clone(),
            admission: pairing.admission.clone(),
            peer_identity: translator,
        })
    }
}
