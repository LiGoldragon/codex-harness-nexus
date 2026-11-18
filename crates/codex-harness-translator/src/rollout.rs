use harness::{Event, Observable, Response, Tokens};
use signal_codex_harness_nexus::{Diagnostic, FieldPath, ParseFailure};

use crate::generated::rollout::{Interpretable, Row, RowText};

impl Observable for Row {
    fn events(&self) -> Vec<Event> {
        match self {
            Row::TaskStarted => vec![Event::PromptSubmitted],
            Row::TokenCount(Some(tokens)) => vec![Event::ContextSize(*tokens)],
            Row::TaskComplete(Some(response)) => {
                vec![Event::Responded(response.clone()), Event::TurnEnded]
            }
            Row::TaskComplete(None) => vec![Event::TurnEnded],
            Row::TurnAborted => vec![Event::Interrupted],
            Row::TokenCount(None) | Row::Other => vec![],
        }
    }
}

impl Interpretable for RowText {
    fn row(&self) -> Result<Row, ParseFailure> {
        let value: serde_json::Value = serde_json::from_str(&self.0)
            .map_err(|error| ParseFailure::InvalidJson(Diagnostic(error.to_string())))?;
        if !value.is_object() {
            return Err(ParseFailure::InvalidField(FieldPath("$".into())));
        }
        if value.pointer("/type").and_then(serde_json::Value::as_str) != Some("event_msg") {
            return Ok(Row::Other);
        }
        match value
            .pointer("/payload/type")
            .and_then(serde_json::Value::as_str)
        {
            Some("task_started") => Ok(Row::TaskStarted),
            Some("turn_aborted") => Ok(Row::TurnAborted),
            Some("token_count") => {
                let info = value
                    .pointer("/payload/info")
                    .ok_or_else(|| ParseFailure::InvalidField(FieldPath("/payload/info".into())))?;
                if info.is_null() {
                    return Ok(Row::TokenCount(None));
                }
                let tokens = info
                    .pointer("/last_token_usage/input_tokens")
                    .and_then(serde_json::Value::as_i64)
                    .filter(|tokens| *tokens >= 0)
                    .ok_or_else(|| {
                        ParseFailure::InvalidField(FieldPath(
                            "/payload/info/last_token_usage/input_tokens".into(),
                        ))
                    })?;
                Ok(Row::TokenCount(Some(Tokens(tokens))))
            }
            Some("task_complete") => {
                let response = value
                    .pointer("/payload/last_agent_message")
                    .ok_or_else(|| {
                        ParseFailure::InvalidField(FieldPath("/payload/last_agent_message".into()))
                    })?;
                if response.is_null() {
                    return Ok(Row::TaskComplete(None));
                }
                let response = response.as_str().ok_or_else(|| {
                    ParseFailure::InvalidField(FieldPath("/payload/last_agent_message".into()))
                })?;
                Ok(Row::TaskComplete(Some(Response(response.into()))))
            }
            _ => Ok(Row::Other),
        }
    }
}
