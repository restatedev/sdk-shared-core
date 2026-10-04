use super::*;

use crate::service_protocol::messages::{
    propose_run_completion_message, run_completion_notification_message, start_message,
    AwaitingOnMessage, ClearAllStateCommandMessage, ClearStateCommandMessage, EndMessage, Failure,
    OneWayCallCommandMessage, ProposeRunCompletionAckMessage, ProposeRunCompletionMessage,
    RunCommandMessage, RunCompletionNotificationMessage, SetStateCommandMessage,
};
use crate::vm::tx::{tx_commit_record, TxCommitRecord, TxStateMutation, TX_COMMIT_RUN_NAME};
use crate::{NonEmptyValue, PayloadOptions, Target, TerminalFailure, TxBegin};
use googletest::prelude::*;
use prost::Message;
use test_log::test;

fn start_with_state(known_entries: u32, state: &[(&str, &str)]) -> StartMessage {
    StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        known_entries,
        key: "my-key".to_string(),
        state_map: state
            .iter()
            .map(|(k, v)| start_message::StateEntry {
                key: Bytes::copy_from_slice(k.as_bytes()),
                value: Bytes::copy_from_slice(v.as_bytes()),
            })
            .collect(),
        partial_state: false,
        ..Default::default()
    }
}

fn get_str(vm: &mut CoreVM, key: &str) -> Option<String> {
    vm.tx_state_get(key)
        .unwrap()
        .map(|b| String::from_utf8(b.to_vec()).unwrap())
}

fn set_str(vm: &mut CoreVM, key: &str, value: &str) {
    vm.tx_state_set(key.to_owned(), Bytes::copy_from_slice(value.as_bytes()))
        .unwrap()
}

fn greeter_target() -> Target {
    Target {
        service: "Greeter".to_string(),
        handler: "greet".to_string(),
        key: None,
        idempotency_key: None,
        scope: None,
        limit_key: None,
        headers: vec![],
    }
}

/// Handler used in most of the tests below:
/// * reads `a`, writes `a = a * 10`, clears `b`, sets `c`, sets `d` to its current value
/// * sends a one-way call
/// * returns the new value of `a`
fn counter_body(vm: &mut CoreVM) -> NonEmptyValue {
    let a: u32 = get_str(vm, "a").unwrap().parse().unwrap();
    set_str(vm, "a", &(a * 10).to_string());
    vm.tx_state_clear("b".to_owned()).unwrap();
    set_str(vm, "c", "3");
    // Writing the same value is not a mutation
    set_str(vm, "d", "4");
    // Clearing a key that doesn't exist is not a mutation
    vm.tx_state_clear("does-not-exist".to_owned()).unwrap();

    // Reads observe the transaction writes
    assert_eq!(get_str(vm, "a"), Some((a * 10).to_string()));
    assert_eq!(get_str(vm, "b"), None);
    assert_eq!(
        vm.tx_state_get_keys().unwrap(),
        vec!["a".to_owned(), "c".to_owned(), "d".to_owned()]
    );

    vm.tx_send(
        greeter_target(),
        Bytes::from_static(b"Francesco"),
        None,
        None,
    )
    .unwrap();

    NonEmptyValue::Success(Bytes::copy_from_slice((a * 10).to_string().as_bytes()))
}

fn expected_counter_record() -> TxCommitRecord {
    TxCommitRecord {
        version: 1,
        clear_all_state: false,
        state_mutations: vec![
            TxStateMutation {
                key: "a".to_owned(),
                value: Some(Bytes::from_static(b"10")),
            },
            TxStateMutation {
                key: "b".to_owned(),
                value: None,
            },
            TxStateMutation {
                key: "c".to_owned(),
                value: Some(Bytes::from_static(b"3")),
            },
        ],
        sends: vec![OneWayCallCommandMessage {
            service_name: "Greeter".to_string(),
            handler_name: "greet".to_string(),
            parameter: Bytes::from_static(b"Francesco"),
            ..Default::default()
        }],
        output: Some(tx_commit_record::Output::Value(Bytes::from_static(b"10"))),
    }
}

fn assert_counter_record_applied_from_set_a(output: &mut OutputIterator) {
    assert_that!(
        output.next_decoded::<SetStateCommandMessage>().unwrap(),
        eq(SetStateCommandMessage {
            key: Bytes::from_static(b"a"),
            value: Some(Bytes::from_static(b"10").into()),
            ..Default::default()
        })
    );
    assert_counter_record_applied_from_clear_b(output);
}

fn assert_counter_record_applied_from_clear_b(output: &mut OutputIterator) {
    assert_that!(
        output.next_decoded::<ClearStateCommandMessage>().unwrap(),
        eq(ClearStateCommandMessage {
            key: Bytes::from_static(b"b"),
            ..Default::default()
        })
    );
    assert_that!(
        output.next_decoded::<SetStateCommandMessage>().unwrap(),
        eq(SetStateCommandMessage {
            key: Bytes::from_static(b"c"),
            value: Some(Bytes::from_static(b"3").into()),
            ..Default::default()
        })
    );
    assert_that!(
        output.next_decoded::<OneWayCallCommandMessage>().unwrap(),
        eq(OneWayCallCommandMessage {
            service_name: "Greeter".to_string(),
            handler_name: "greet".to_string(),
            parameter: Bytes::from_static(b"Francesco"),
            invocation_id_notification_idx: 2,
            ..Default::default()
        })
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"10")
    );
    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

const INITIAL_STATE: &[(&str, &str)] = &[("a", "1"), ("b", "2"), ("d", "4")];

#[test]
fn commit_then_apply() {
    let mut output = VMTestCase::new()
        .input(start_with_state(1, INITIAL_STATE))
        .input(input_entry_message(b""))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            assert_eq!(vm.sys_tx_begin().unwrap(), TxBegin::Execute);
            let output = counter_body(vm);
            let handle = vm.sys_tx_commit(output).unwrap();

            // Waiting for the commit to be durable
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );
            vm.notify_input(encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }));
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert!(vm.is_completed(handle));

            assert_eq!(
                Value::from(vm.sys_tx_end().unwrap()),
                Value::Success(Bytes::from_static(b"10"))
            );
        });

    // Nothing is written to the journal before the commit.
    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 1,
            name: TX_COMMIT_RUN_NAME.to_owned(),
        })
    );
    let proposal = output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_eq!(proposal.result_completion_id, 1);
    let Some(propose_run_completion_message::Result::Value(record)) = proposal.result else {
        panic!("Expected a value");
    };
    assert_that!(
        TxCommitRecord::decode(record).unwrap(),
        eq(expected_counter_record())
    );
    // Once the transaction started, the cancel signal is not awaited anymore
    assert_that!(
        output.next_decoded::<AwaitingOnMessage>().unwrap(),
        pat!(AwaitingOnMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: eq(vec![1]),
                waiting_signals: empty(),
            }))
        })
    );

    assert_counter_record_applied_from_set_a(&mut output);
}

#[test]
fn replay_committed_record() {
    let record = expected_counter_record().encode_to_vec();
    let mut output = VMTestCase::new()
        .input(start_with_state(3, &[("a", "1"), ("b", "2")]))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: TX_COMMIT_RUN_NAME.to_owned(),
        })
        .input(RunCompletionNotificationMessage {
            completion_id: 1,
            result: Some(run_completion_notification_message::Result::Value(
                Bytes::from(record).into(),
            )),
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            // The handler body must not be executed again
            let TxBegin::Committed(handle) = vm.sys_tx_begin().unwrap() else {
                panic!("Expected the transaction to be already committed");
            };
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            assert_eq!(
                Value::from(vm.sys_tx_end().unwrap()),
                Value::Success(Bytes::from_static(b"10"))
            );
        });

    assert_counter_record_applied_from_set_a(&mut output);
}

#[test]
fn replay_partially_applied_record() {
    let record = expected_counter_record().encode_to_vec();
    let mut output = VMTestCase::new()
        // The previous attempt applied `SetState a` before dying.
        // Note: the state map already reflects `a = 10`, but the handler body is not executed, so it doesn't matter.
        .input(start_with_state(4, &[("a", "10"), ("b", "2")]))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: TX_COMMIT_RUN_NAME.to_owned(),
        })
        .input(RunCompletionNotificationMessage {
            completion_id: 1,
            result: Some(run_completion_notification_message::Result::Value(
                Bytes::from(record).into(),
            )),
        })
        .input(SetStateCommandMessage {
            key: Bytes::from_static(b"a"),
            value: Some(Bytes::from_static(b"10").into()),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let TxBegin::Committed(handle) = vm.sys_tx_begin().unwrap() else {
                panic!("Expected the transaction to be already committed");
            };
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            vm.sys_tx_end().unwrap();
        });

    // Only the rest of the record is written out
    assert_counter_record_applied_from_clear_b(&mut output);
}

#[test]
fn replay_commit_run_without_result() {
    // The previous attempt wrote the commit RunCommand, but died before the proposal was stored.
    let mut output = VMTestCase::new()
        .input(start_with_state(2, INITIAL_STATE))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: TX_COMMIT_RUN_NAME.to_owned(),
        })
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            // The handler body is executed again
            assert_eq!(vm.sys_tx_begin().unwrap(), TxBegin::Execute);
            let output = counter_body(vm);
            let handle = vm.sys_tx_commit(output).unwrap();

            vm.notify_input(encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }));
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            vm.sys_tx_end().unwrap();
        });

    // No new RunCommand, just the proposal for the replayed one
    let proposal = output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_eq!(proposal.result_completion_id, 1);
    assert_counter_record_applied_from_set_a(&mut output);
}

#[test]
fn replay_mismatch_with_non_transactional_journal() {
    let mut output = VMTestCase::new()
        .input(start_with_state(2, INITIAL_STATE))
        .input(input_entry_message(b""))
        .input(SetStateCommandMessage {
            key: Bytes::from_static(b"a"),
            value: Some(Bytes::from_static(b"10").into()),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();
            assert_that!(
                vm.sys_tx_begin(),
                err(pat!(Error {
                    code: eq(error::codes::JOURNAL_MISMATCH.code())
                }))
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        pat!(ErrorMessage {
            code: eq(error::codes::JOURNAL_MISMATCH.code() as u32)
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn terminal_failure_discards_writes() {
    let mut output = VMTestCase::new()
        .input(start_with_state(1, INITIAL_STATE))
        .input(input_entry_message(b""))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            assert_eq!(vm.sys_tx_begin().unwrap(), TxBegin::Execute);
            let _ = counter_body(vm);
            let handle = vm
                .sys_tx_commit(NonEmptyValue::Failure(TerminalFailure {
                    code: 409,
                    message: "insufficient funds".to_string(),
                    metadata: vec![],
                }))
                .unwrap();

            vm.notify_input(encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }));
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            vm.sys_tx_end().unwrap();
        });

    output.next_decoded::<RunCommandMessage>().unwrap();
    let proposal = output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    let Some(propose_run_completion_message::Result::Value(record)) = proposal.result else {
        panic!("Expected a value");
    };
    assert_that!(
        TxCommitRecord::decode(record).unwrap(),
        eq(TxCommitRecord {
            version: 1,
            clear_all_state: false,
            state_mutations: vec![],
            sends: vec![],
            output: Some(tx_commit_record::Output::Failure(Failure {
                code: 409,
                message: "insufficient funds".to_string(),
                metadata: vec![],
            })),
        })
    );

    // Only the output, no state mutation nor one way call
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_failure(409, "insufficient funds")
    );
    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

#[test]
fn clear_all_then_set() {
    let mut output = VMTestCase::new()
        .input(start_with_state(1, INITIAL_STATE))
        .input(input_entry_message(b""))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            assert_eq!(vm.sys_tx_begin().unwrap(), TxBegin::Execute);
            set_str(vm, "a", "100");
            vm.tx_state_clear_all().unwrap();
            assert_eq!(get_str(vm, "a"), None);
            assert_eq!(get_str(vm, "d"), None);
            set_str(vm, "z", "26");
            vm.tx_state_clear("d".to_owned()).unwrap();
            assert_eq!(vm.tx_state_get_keys().unwrap(), vec!["z".to_owned()]);

            let handle = vm
                .sys_tx_commit(NonEmptyValue::Success(Bytes::new()))
                .unwrap();
            vm.notify_input(encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }));
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            vm.sys_tx_end().unwrap();
        });

    output.next_decoded::<RunCommandMessage>().unwrap();
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_that!(
        output
            .next_decoded::<ClearAllStateCommandMessage>()
            .unwrap(),
        eq(ClearAllStateCommandMessage::default())
    );
    assert_that!(
        output.next_decoded::<SetStateCommandMessage>().unwrap(),
        eq(SetStateCommandMessage {
            key: Bytes::from_static(b"z"),
            value: Some(Bytes::from_static(b"26").into()),
            ..Default::default()
        })
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"")
    );
    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

#[test]
fn suspend_while_waiting_commit() {
    // E.g. request/response mode: the input is closed before the commit ack arrives.
    // The invocation suspends waiting on the commit, and must not wait on the cancel signal anymore.
    let mut output = VMTestCase::new()
        .input(start_with_state(1, INITIAL_STATE))
        .input(input_entry_message(b""))
        .run(|vm| {
            vm.sys_input().unwrap();

            assert_eq!(vm.sys_tx_begin().unwrap(), TxBegin::Execute);
            let output = counter_body(vm);
            let handle = vm.sys_tx_commit(output).unwrap();

            assert_that!(
                vm.do_await(UnresolvedFuture::Single(handle)),
                err(is_suspended())
            );
        });

    output.next_decoded::<RunCommandMessage>().unwrap();
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            awaiting_on: some(pat!(messages::Future {
                waiting_completions: eq(vec![1]),
                waiting_signals: empty(),
            }))
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn commit_with_run_completion_notification_on_v6() {
    // Before V7 the runtime answers the proposal with the whole notification instead of an ack.
    let mut output = VMTestCase::with_version(Version::V6)
        .input(start_with_state(1, INITIAL_STATE))
        .input(input_entry_message(b""))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            assert_eq!(vm.sys_tx_begin().unwrap(), TxBegin::Execute);
            let output = counter_body(vm);
            let handle = vm.sys_tx_commit(output).unwrap();

            vm.notify_input(encoder.encode(&RunCompletionNotificationMessage {
                completion_id: 1,
                result: Some(run_completion_notification_message::Result::Value(
                    Bytes::from(expected_counter_record().encode_to_vec()).into(),
                )),
            }));
            assert_eq!(
                vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
                AwaitResponse::AnyCompleted
            );
            vm.sys_tx_end().unwrap();
        });

    output.next_decoded::<RunCommandMessage>().unwrap();
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_counter_record_applied_from_set_a(&mut output);
}

#[test]
fn read_only_handler_reads_snapshot() {
    let mut output = VMTestCase::new()
        .input(start_with_state(1, INITIAL_STATE))
        .input(input_entry_message(b""))
        .run(|vm| {
            vm.sys_input().unwrap();

            // No transaction needed to read
            assert_eq!(get_str(vm, "a"), Some("1".to_owned()));
            assert_eq!(get_str(vm, "unknown"), None);
            assert_eq!(
                vm.tx_state_get_keys().unwrap(),
                vec!["a".to_owned(), "b".to_owned(), "d".to_owned()]
            );

            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"1")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

    // Reads are not journaled
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"1")
    );
    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

#[test]
fn read_with_partial_state_fails() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            partial_state: true,
            ..start_with_state(1, INITIAL_STATE)
        })
        .input(input_entry_message(b""))
        .run(|vm| {
            vm.sys_input().unwrap();
            assert_eq!(vm.sys_tx_begin().unwrap(), TxBegin::Execute);

            // Keys in the snapshot are fine
            assert_eq!(get_str(vm, "a"), Some("1".to_owned()));
            // Keys missing from a partial snapshot are unknown
            assert_that!(
                vm.tx_state_get("unknown"),
                err(eq_error(crate::vm::errors::TX_PARTIAL_STATE))
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        error_message_as_error(crate::vm::errors::TX_PARTIAL_STATE)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn write_outside_transaction_fails() {
    let mut output = VMTestCase::new()
        .input(start_with_state(1, INITIAL_STATE))
        .input(input_entry_message(b""))
        .run(|vm| {
            vm.sys_input().unwrap();
            assert_that!(
                vm.tx_state_set("a".to_owned(), Bytes::from_static(b"2")),
                err(pat!(Error {
                    code: eq(error::codes::INTERNAL.code())
                }))
            );
        });

    assert_that!(
        output.next_decoded::<ErrorMessage>().unwrap(),
        pat!(ErrorMessage {
            code: eq(error::codes::INTERNAL.code() as u32)
        })
    );
    assert_eq!(output.next(), None);
}
