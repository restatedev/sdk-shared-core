use super::*;

use crate::service_protocol::messages::{
    call_completion_notification_message, run_completion_notification_message, start_message,
    CallCommandMessage, CallCompletionNotificationMessage,
    CallInvocationIdCompletionNotificationMessage, ClearStateCommandMessage, EndMessage,
    GetLazyStateCommandMessage, ProposeRunCompletionAckMessage, ProposeRunCompletionMessage,
    RunCommandMessage, RunCompletionNotificationMessage, SetStateCommandMessage,
    SleepCommandMessage,
};
use crate::vm::tx::{tx_commit_record, TxCommitRecord, TxStateMutation};
use crate::{
    JournalMode, NonEmptyValue, PayloadOptions, Restore, StepBegin, Target, TerminalFailure,
};
use googletest::prelude::*;
use prost::Message;
use std::time::Duration;
use test_log::test;

fn storage_mode() -> VMTestCase {
    VMTestCase::with_vm_options(VMOptions {
        journal_mode: JournalMode::Storage,
        ..Default::default()
    })
}

fn start(known_entries: u32, state: &[(&str, &str)]) -> StartMessage {
    StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        known_entries,
        key: "order-1".to_string(),
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

fn shipping_target() -> Target {
    Target {
        service: "Shipping".to_string(),
        handler: "label".to_string(),
        key: None,
        idempotency_key: None,
        scope: None,
        limit_key: None,
        headers: vec![],
    }
}

fn get_str(vm: &mut CoreVM, key: &str) -> Option<String> {
    vm.tx_state_get(key)
        .unwrap()
        .map(|b| String::from_utf8(b.to_vec()).unwrap())
}

fn record(mutations: &[(&str, Option<&str>)], result: &str) -> TxCommitRecord {
    TxCommitRecord {
        version: 1,
        clear_all_state: false,
        state_mutations: mutations
            .iter()
            .map(|(k, v)| TxStateMutation {
                key: k.to_string(),
                value: v.map(|v| Bytes::copy_from_slice(v.as_bytes())),
            })
            .collect(),
        sends: vec![],
        output: Some(tx_commit_record::Output::Value(Bytes::copy_from_slice(
            result.as_bytes(),
        ))),
    }
}

fn run_completion(completion_id: u32, record: &TxCommitRecord) -> RunCompletionNotificationMessage {
    RunCompletionNotificationMessage {
        completion_id,
        result: Some(run_completion_notification_message::Result::Value(
            Bytes::from(record.encode_to_vec()).into(),
        )),
    }
}

/// Step "reserve": stock -= 3, returns the new stock.
fn reserve_step(vm: &mut CoreVM) -> NotificationHandle {
    match vm.sys_step_begin("reserve".to_owned()).unwrap() {
        StepBegin::Execute => {
            let stock: u32 = get_str(vm, "stock").unwrap().parse().unwrap();
            vm.tx_state_set("stock".to_owned(), Bytes::from((stock - 3).to_string()))
                .unwrap();
            vm.sys_step_commit(NonEmptyValue::Success(Bytes::from((stock - 3).to_string())))
                .unwrap()
        }
        StepBegin::Committed(handle) => handle,
    }
}

fn await_handle(vm: &mut CoreVM, handle: NotificationHandle) {
    assert_eq!(
        vm.do_await(UnresolvedFuture::Single(handle)).unwrap(),
        AwaitResponse::AnyCompleted
    );
}

fn take_step_result(vm: &mut CoreVM, handle: NotificationHandle) -> Value {
    Value::from(vm.sys_step_take_result(handle).unwrap().unwrap())
}

#[test]
fn fresh_attempt() {
    let mut output = storage_mode()
        .input(start(1, &[("stock", "10")]))
        .input(input_entry_message(b""))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();
            assert_eq!(vm.sys_restore().unwrap(), Restore::Execute);

            // Commit point 1
            let handle = reserve_step(vm);
            vm.notify_input(encoder.encode(&ProposeRunCompletionAckMessage { completion_id: 1 }));
            await_handle(vm, handle);
            assert_eq!(
                take_step_result(vm, handle),
                Value::Success(Bytes::from_static(b"7"))
            );

            // Reads outside of steps see the committed state, and are not journaled
            assert_eq!(get_str(vm, "stock"), Some("7".to_owned()));
            let h = vm
                .sys_state_get("stock".to_owned(), PayloadOptions::default())
                .unwrap();
            assert_eq!(
                vm.take_notification(h).unwrap(),
                Some(Value::Success(Bytes::from_static(b"7")))
            );

            // A call, keyed by its target since it has no name
            vm.sys_call(
                shipping_target(),
                Bytes::from_static(b"order-1"),
                None,
                PayloadOptions::default(),
            )
            .unwrap();

            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"done")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 1,
            name: "tx:reserve".to_owned(),
        })
    );
    let proposal = output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    let Some(crate::service_protocol::messages::propose_run_completion_message::Result::Value(r)) =
        proposal.result
    else {
        panic!("Expected a value")
    };
    assert_that!(
        TxCommitRecord::decode(r).unwrap(),
        eq(record(&[("stock", Some("7"))], "7"))
    );
    // The apply commands are written together with the commit
    assert_that!(
        output.next_decoded::<SetStateCommandMessage>().unwrap(),
        eq(SetStateCommandMessage {
            key: Bytes::from_static(b"stock"),
            value: Some(Bytes::from_static(b"7").into()),
            ..Default::default()
        })
    );
    assert_that!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        pat!(CallCommandMessage {
            service_name: eq("Shipping"),
            handler_name: eq("label"),
            name: eq("Shipping/label"),
            invocation_id_notification_idx: eq(3),
            result_completion_id: eq(4),
        })
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"done")
    );
    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

#[test]
fn retry_with_different_control_flow() {
    // Previous attempt: committed "reserve", issued the call to Shipping/label, then died.
    let reserve = record(&[("stock", Some("7"))], "7");
    let mut output = storage_mode()
        .input(start(6, &[("stock", "7")]))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "tx:reserve".to_owned(),
        })
        .input(run_completion(1, &reserve))
        .input(SetStateCommandMessage {
            key: Bytes::from_static(b"stock"),
            value: Some(Bytes::from_static(b"7").into()),
            ..Default::default()
        })
        .input(CallCommandMessage {
            service_name: "Shipping".to_owned(),
            handler_name: "label".to_owned(),
            parameter: Bytes::from_static(b"order-1"),
            invocation_id_notification_idx: 2,
            result_completion_id: 3,
            name: "Shipping/label".to_owned(),
            ..Default::default()
        })
        .input(CallInvocationIdCompletionNotificationMessage {
            completion_id: 2,
            invocation_id: "inv_shipping".to_owned(),
        })
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();
            assert_eq!(vm.sys_restore().unwrap(), Restore::Execute);

            // This attempt does something the previous one didn't do, before anything else.
            // There's no determinism check: it's just appended to the journal.
            let audit = vm.sys_run("audit".to_owned()).unwrap();
            assert!(!audit.replayed);
            vm.propose_run_completion(
                audit.handle,
                RunExitResult::Success(Bytes::from_static(b"ok")),
                RetryPolicy::default(),
            )
            .unwrap();

            // The step is found by name: its body is not executed again
            let reserve = reserve_step(vm);
            await_handle(vm, reserve);
            assert_eq!(
                take_step_result(vm, reserve),
                Value::Success(Bytes::from_static(b"7"))
            );

            // The call is found by name, and not issued again: we await the result of the call issued by the previous attempt
            let call = vm
                .sys_call(
                    shipping_target(),
                    Bytes::from_static(b"other input"),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();
            vm.notify_input(encoder.encode(&CallCompletionNotificationMessage {
                completion_id: 3,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"label-1").into(),
                )),
            }));
            await_handle(vm, call.call_notification_handle);
            assert_eq!(
                vm.take_notification(call.call_notification_handle).unwrap(),
                Some(Value::Success(Bytes::from_static(b"label-1")))
            );

            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"label-1")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

    // Only the new run and the output are written: new completion ids follow the ones in the journal
    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 4,
            name: "audit".to_owned(),
        })
    );
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"label-1")
    );
    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

#[test]
fn restore_completes_partially_applied_step() {
    let ship = record(
        &[("a", Some("1")), ("b", None), ("c", Some("3"))],
        "shipped",
    );
    let mut output = storage_mode()
        .input(start(4, &[("a", "1"), ("b", "2")]))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "tx:ship".to_owned(),
        })
        .input(run_completion(1, &ship))
        .input(SetStateCommandMessage {
            key: Bytes::from_static(b"a"),
            value: Some(Bytes::from_static(b"1").into()),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();
            // The rest of the step is applied before the handler runs
            assert_eq!(vm.sys_restore().unwrap(), Restore::Execute);
            assert_eq!(get_str(vm, "b"), None);
            assert_eq!(get_str(vm, "c"), Some("3".to_owned()));

            let StepBegin::Committed(handle) = vm.sys_step_begin("ship".to_owned()).unwrap() else {
                panic!("Expected the step to be committed");
            };
            await_handle(vm, handle);
            assert_eq!(
                take_step_result(vm, handle),
                Value::Success(Bytes::from_static(b"shipped"))
            );
            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"shipped")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

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
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"shipped")
    );
    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

#[test]
fn restore_with_output_ends_the_invocation() {
    let mut output = storage_mode()
        .input(start(2, &[]))
        .input(input_entry_message(b""))
        .input(OutputCommandMessage {
            result: Some(output_command_message::Result::Value(
                Bytes::from_static(b"done").into(),
            )),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();
            assert_eq!(vm.sys_restore().unwrap(), Restore::Completed);
        });

    assert_that!(
        output.next_decoded::<EndMessage>().unwrap(),
        eq(EndMessage {})
    );
    assert_eq!(output.next(), None);
}

#[test]
fn same_name_occurrences() {
    // The previous attempt committed only the first "item" step
    let mut output = storage_mode()
        .input(start(3, &[]))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "tx:item".to_owned(),
        })
        .input(run_completion(1, &record(&[], "first")))
        .run(|vm| {
            vm.sys_input().unwrap();
            vm.sys_restore().unwrap();

            let StepBegin::Committed(_) = vm.sys_step_begin("item".to_owned()).unwrap() else {
                panic!("Expected the first occurrence to be committed");
            };
            assert_eq!(
                vm.sys_step_begin("item".to_owned()).unwrap(),
                StepBegin::Execute
            );
            vm.sys_step_commit(NonEmptyValue::Success(Bytes::from_static(b"second")))
                .unwrap();
        });

    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 2,
            name: "tx:item#2".to_owned(),
        })
    );
}

#[test]
fn step_run_without_result_is_executed_again() {
    // The previous attempt died before the step proposal was stored
    let mut output = storage_mode()
        .input(start(2, &[("stock", "10")]))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "tx:reserve".to_owned(),
        })
        .run(|vm| {
            vm.sys_input().unwrap();
            vm.sys_restore().unwrap();
            reserve_step(vm);
        });

    // A new run command is written, right before the apply commands
    assert_that!(
        output.next_decoded::<RunCommandMessage>().unwrap(),
        eq(RunCommandMessage {
            result_completion_id: 2,
            name: "tx:reserve".to_owned(),
        })
    );
    output
        .next_decoded::<ProposeRunCompletionMessage>()
        .unwrap();
    output.next_decoded::<SetStateCommandMessage>().unwrap();
}

#[test]
fn step_failure_is_memoized() {
    let failed = TxCommitRecord {
        output: Some(tx_commit_record::Output::Failure(
            crate::service_protocol::messages::Failure {
                code: 409,
                message: "out of stock".to_owned(),
                metadata: vec![],
            },
        )),
        ..record(&[], "")
    };
    storage_mode()
        .input(start(3, &[]))
        .input(input_entry_message(b""))
        .input(RunCommandMessage {
            result_completion_id: 1,
            name: "tx:reserve".to_owned(),
        })
        .input(run_completion(1, &failed))
        .run(|vm| {
            vm.sys_input().unwrap();
            vm.sys_restore().unwrap();
            let StepBegin::Committed(handle) = vm.sys_step_begin("reserve".to_owned()).unwrap()
            else {
                panic!("Expected the step to be committed");
            };
            await_handle(vm, handle);
            assert_eq!(
                take_step_result(vm, handle),
                Value::Failure(TerminalFailure {
                    code: 409,
                    message: "out of stock".to_owned(),
                    metadata: vec![],
                })
            );
        });
}

#[test]
fn named_sleep_is_reused() {
    let mut output = storage_mode()
        .input(start(2, &[]))
        .input(input_entry_message(b""))
        .input(SleepCommandMessage {
            wake_up_time: 1000,
            result_completion_id: 1,
            name: "cool-down".to_owned(),
        })
        .run(|vm| {
            vm.sys_input().unwrap();
            vm.sys_restore().unwrap();
            let handle = vm
                .sys_sleep("cool-down".to_owned(), Duration::from_millis(5000), None)
                .unwrap();
            // Waiting on the sleep of the previous attempt, which is still pending
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(handle)),
                err(is_suspended())
            );
        });

    // No new sleep command
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_completion(1)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn lazy_state_reads_are_supported() {
    let mut output = storage_mode()
        .input(StartMessage {
            partial_state: true,
            ..start(1, &[("known", "1")])
        })
        .input(input_entry_message(b""))
        .run(|vm| {
            vm.sys_input().unwrap();
            vm.sys_restore().unwrap();
            // Known key: not journaled
            vm.sys_state_get("known".to_owned(), PayloadOptions::default())
                .unwrap();
            // Unknown key: fetched from the runtime
            vm.sys_state_get("unknown".to_owned(), PayloadOptions::default())
                .unwrap();
        });

    assert_that!(
        output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
        pat!(GetLazyStateCommandMessage {
            key: eq(Bytes::from_static(b"unknown")),
        })
    );
    assert_eq!(output.next(), None);
}

#[test]
fn awakeables_are_not_supported() {
    storage_mode()
        .input(start(1, &[]))
        .input(input_entry_message(b""))
        .run(|vm| {
            vm.sys_input().unwrap();
            vm.sys_restore().unwrap();
            assert_that!(
                vm.sys_awakeable(),
                err(pat!(Error {
                    code: eq(error::codes::UNSUPPORTED_FEATURE.code())
                }))
            );
        });
}

#[test]
fn restore_is_required() {
    storage_mode()
        .input(start(1, &[]))
        .input(input_entry_message(b""))
        .run(|vm| {
            vm.sys_input().unwrap();
            assert_that!(
                vm.sys_run("run".to_owned()),
                err(pat!(Error {
                    code: eq(error::codes::INTERNAL.code())
                }))
            );
        });
}
