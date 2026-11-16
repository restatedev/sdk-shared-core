use crate::service_protocol::messages::{
    propose_run_completion_message, InputCommandMessage, ProposeRunCompletionMessage,
};
use crate::service_protocol::CompletionId;
use crate::TerminalFailure;
use std::collections::BTreeMap;
use tracing::trace;

/// What the attempt takes from the entries the earlier attempts stored.
#[derive(Debug, Default)]
pub(crate) struct EarlierEntries {
    pub(crate) input: Option<InputCommandMessage>,
    /// The earlier attempts' commands, the input included: this attempt's are numbered after.
    pub(crate) commands: u32,
    /// The runs left without a completion, by completion id: the attempt fails them.
    pub(crate) open_runs: BTreeMap<CompletionId, String>,
}

impl EarlierEntries {
    pub fn get_input_and_close_dangling_traces(
        self,
    ) -> (
        Option<InputCommandMessage>,
        impl Iterator<Item = ProposeRunCompletionMessage>,
    ) {
        (self.input, self.open_runs.into_iter().map(|(completion_id, name)| {
            trace!("Failing the run '{name}' (completion id {completion_id}), which an earlier attempt left open");
            let result = propose_run_completion_message::Result::Failure(
                TerminalFailure {
                    code: 500,
                    message: "the attempt ended before the run completed".to_owned(),
                    metadata: vec![],
                }
                    .into(),
            );
            ProposeRunCompletionMessage {
                result_completion_id: completion_id,
                result: Some(result),
            }
        })
        )
    }
}
