//! The translator's Kameo-backed settings owner, prepared independently of
//! the synchronous production translator adapter.
//!
//! `TranslatorSettingsActor` owns both its actor and the Tokio runtime that
//! runs it. Bootstrap and destruction are synchronous OS-thread operations;
//! callers of `SettingsReading` and `SettingsWriting` must likewise be
//! synchronous serving threads, because Kameo's `blocking_send` is the
//! contact point. This type is not an arbitrary async-context adapter.

use std::convert::Infallible;
use std::fmt;

use kameo::actor::{ActorRef, Spawn};
use kameo::message::{Context, Message};
use meta_signal_codex_harness_nexus as meta;
use signal_codex_harness_nexus::Diagnostic;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;
use translator_serving::{DepthLimit, NodeLimit, TextLimits, TranslatorBootstrap, ValidTextBudget};

pub use crate::generated::translator_settings::*;

/// The Kameo actor and runtime owned together for the translator's settings.
struct SettingsActorRuntime {
    actor: ActorRef<TranslatorSettings>,
    task: JoinHandle<
        Result<(TranslatorSettings, kameo::error::ActorStopReason), kameo::error::PanicError>,
    >,
    runtime: Runtime,
}

impl Drop for SettingsActorRuntime {
    fn drop(&mut self) {
        // This owner is created and dropped at the synchronous process
        // boundary, outside a Tokio runtime. The actor is stopped and joined
        // before its runtime is dropped; no actor task is detached.
        let runtime = &self.runtime;
        let actor = self.actor.clone();
        let task = &mut self.task;
        runtime.block_on(async {
            let _ = actor.stop_gracefully().await;
            let _ = task.await;
        });
    }
}

/// A standalone actor owner, not yet wired into `CodexHarnessTranslator`.
pub struct TranslatorSettingsActor {
    settings: SettingsActorRuntime,
}

impl TryFrom<&TranslatorBootstrap> for TranslatorSettingsActor {
    type Error = TranslatorBootstrapFailure;

    fn try_from(bootstrap: &TranslatorBootstrap) -> Result<Self, Self::Error> {
        let text_limits = bootstrap.text_limits.clone();
        text_limits.valid_budget()?;

        let runtime = Builder::new_multi_thread().build().map_err(|error| {
            TranslatorBootstrapFailure::SettingsRuntimeUnavailable(Diagnostic(error.to_string()))
        })?;
        let (actor, task) = {
            let _entered = runtime.enter();
            let prepared_actor = TranslatorSettings::prepare();
            let actor = prepared_actor.actor_ref().clone();
            let task = prepared_actor.spawn(TranslatorSettings { text_limits });
            (actor, task)
        };
        runtime.block_on(actor.wait_for_startup());

        Ok(Self {
            settings: SettingsActorRuntime {
                actor,
                task,
                runtime,
            },
        })
    }
}

impl fmt::Display for TranslatorBootstrapFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for TranslatorBootstrapFailure {}

impl fmt::Display for TranslatorActorFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for TranslatorActorFailure {}

impl fmt::Display for MetaBudgetFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for MetaBudgetFailure {}

impl kameo::Actor for TranslatorSettings {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(settings: Self::Args, _actor: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(settings)
    }
}

impl Message<SettingsCommand> for TranslatorSettings {
    type Reply = SettingsReply;

    async fn handle(
        &mut self,
        message: SettingsCommand,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SettingsCommandHandling::handle(self, message)
    }
}

impl SettingsCommandHandling for TranslatorSettings {
    fn handle(&mut self, message: SettingsCommand) -> SettingsReply {
        match message {
            SettingsCommand::ReadSettings => SettingsReply::SettingsRead(self.clone()),
            SettingsCommand::SetTextBudget(budget) => {
                SettingsReply::TextBudgetSet(TextBudgetSetting::set_text_budget(self, budget))
            }
        }
    }
}

impl Limiting for meta::TextBudget {
    fn text_limits(&self) -> TextLimits {
        TextLimits {
            node_limit: NodeLimit(self.node_limit.0),
            depth_limit: DepthLimit(self.depth_limit.0),
        }
    }
}

impl TextBudgetInput for meta::TextBudget {
    fn text_budget(&self) -> meta::TextBudget {
        self.clone()
    }
}

impl ConfigurationReadoutInput for meta::ConfigurationReadout {
    fn readout(&self) -> meta::ConfigurationReadout {
        self.clone()
    }
}

impl TextLimitsValidating for TextLimits {
    fn valid_budget(&self) -> Result<ValidTextBudget, TranslatorBootstrapFailure> {
        match ValidTextBudget::try_from(self) {
            Ok(budget) => Ok(budget),
            Err(_) => {
                let maximum = ValidTextBudget::MAXIMUM;
                let node_limit = usize::try_from(self.node_limit.0)
                    .ok()
                    .filter(|nodes| (1..=maximum.nodes).contains(nodes));
                if node_limit.is_none() {
                    Err(TranslatorBootstrapFailure::InvalidNodeLimit)
                } else {
                    Err(TranslatorBootstrapFailure::InvalidDepthLimit)
                }
            }
        }
    }
}

impl MetaBudgeting for ValidTextBudget {
    fn meta_budget(&self) -> Result<meta::TextBudget, MetaBudgetFailure> {
        let node_limit =
            i64::try_from(self.nodes).map_err(|_| MetaBudgetFailure::NodeLimitOutOfRange)?;
        let depth_limit =
            i64::try_from(self.depth).map_err(|_| MetaBudgetFailure::DepthLimitOutOfRange)?;
        Ok(meta::TextBudget {
            node_limit: meta::NodeLimit(node_limit),
            depth_limit: meta::DepthLimit(depth_limit),
        })
    }
}

impl BudgetMerging for meta::ConfigurationReadout {
    fn with_text_budget<N: ConfigurationReadoutInput, O: TextBudgetInput>(
        readout: N,
        text_budget: O,
    ) -> meta::ConfigurationReadout {
        let mut readout = readout.readout();
        readout.configuration.text_budget_option = Some(text_budget.text_budget());
        readout
    }
}

impl TextBudgetSetting for TranslatorSettings {
    fn set_text_budget<N: TextBudgetInput>(&mut self, input: N) -> meta::Output {
        let budget = input.text_budget();
        let text_limits = budget.text_limits();
        match text_limits.valid_budget() {
            Ok(_) => {
                self.text_limits = text_limits;
                meta::Output::Configured(meta::Set::TextBudget(budget))
            }
            Err(_) => meta::Output::ConfigureRefused(
                meta::ConfigurationRefusal::InvalidTextBudget(budget),
            ),
        }
    }
}

impl SettingsReading for TranslatorSettingsActor {
    fn current_settings(&self) -> Result<TranslatorSettings, TranslatorActorFailure> {
        match self
            .settings
            .actor
            .ask(SettingsCommand::ReadSettings)
            .blocking_send()
        {
            Ok(SettingsReply::SettingsRead(settings)) => Ok(settings),
            Ok(SettingsReply::TextBudgetSet(_)) => {
                Err(TranslatorActorFailure::UnexpectedSettingsReply)
            }
            Err(error) => Err(TranslatorActorFailure::SettingsActorUnavailable(
                Diagnostic(error.to_string()),
            )),
        }
    }
}

impl SettingsWriting for TranslatorSettingsActor {
    fn request_text_budget<N: TextBudgetInput>(
        &self,
        input: N,
    ) -> Result<meta::Output, TranslatorActorFailure> {
        match self
            .settings
            .actor
            .ask(SettingsCommand::SetTextBudget(input.text_budget()))
            .blocking_send()
        {
            Ok(SettingsReply::TextBudgetSet(output)) => Ok(output),
            Ok(SettingsReply::SettingsRead(_)) => {
                Err(TranslatorActorFailure::UnexpectedSettingsReply)
            }
            Err(error) => Err(TranslatorActorFailure::SettingsActorUnavailable(
                Diagnostic(error.to_string()),
            )),
        }
    }
}
