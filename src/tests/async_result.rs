use super::*;

use crate::service_protocol::messages::*;
use crate::{PayloadOptions, Value};

use test_log::test;

fn greeter_target() -> Target {
    Target {
        service: "Greeter".to_string(),
        handler: "greeter".to_string(),
        key: None,
        idempotency_key: None,
        headers: Vec::new(),
    }
}

#[test]
fn dont_await_call() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputCommandMessage::default())
        .run(|vm| {
            vm.sys_input().unwrap();

            let _ = vm
                .sys_call(
                    greeter_target(),
                    Bytes::from_static(b"Francesco"),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();
            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"Whatever")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap()
        });

    assert_eq!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        CallCommandMessage {
            service_name: "Greeter".to_owned(),
            handler_name: "greeter".to_owned(),
            parameter: Bytes::from_static(b"Francesco"),
            invocation_id_notification_idx: 1,
            result_completion_id: 2,
            ..Default::default()
        }
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"Whatever")
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn dont_await_call_dont_notify_input_closed() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputCommandMessage::default())
        .run_without_closing_input(|vm, _| {
            vm.sys_input().unwrap();
            let _ = vm
                .sys_call(
                    greeter_target(),
                    Bytes::from_static(b"Francesco"),
                    None,
                    PayloadOptions::default(),
                )
                .unwrap();
            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::from_static(b"Whatever")),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap()
        });

    assert_eq!(
        output.next_decoded::<CallCommandMessage>().unwrap(),
        CallCommandMessage {
            service_name: "Greeter".to_owned(),
            handler_name: "greeter".to_owned(),
            parameter: Bytes::from_static(b"Francesco"),
            invocation_id_notification_idx: 1,
            result_completion_id: 2,
            ..Default::default()
        }
    );
    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"Whatever")
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

mod do_progress {
    use super::*;

    use test_log::test;

    #[test]
    fn await_twice_the_same_handle() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 1,
                ..Default::default()
            })
            .input(input_entry_message(b"my-data"))
            .run_without_closing_input(|vm, _| {
                vm.sys_input().unwrap();

                let (_, h) = vm.sys_awakeable().unwrap();

                assert_eq!(
                    vm.do_progress(vec![h]).unwrap(),
                    DoProgressResponse::ReadFromInput
                );
                assert_eq!(
                    vm.do_progress(vec![h]).unwrap(),
                    DoProgressResponse::ReadFromInput
                );

                vm.notify_input_closed();

                assert_that!(vm.do_progress(vec![h]), err(is_suspended()));
            });

        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_signal(17)
        );
        assert_eq!(output.next(), None);
    }
}

mod reverse_await_order {
    use super::*;

    use test_log::test;

    fn handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm
            .sys_call(
                greeter_target(),
                Bytes::from_static(b"Francesco"),
                None,
                PayloadOptions::default(),
            )
            .unwrap();
        let h2 = vm
            .sys_call(
                greeter_target(),
                Bytes::from_static(b"Till"),
                None,
                PayloadOptions::default(),
            )
            .unwrap();

        if vm
            .do_progress(vec![h2.call_notification_handle])
            .is_err_and(|e| e.is_suspended_error())
        {
            assert_that!(
                vm.take_notification(h2.call_notification_handle),
                err(is_closed())
            );
            return;
        }
        assert2::assert!(
            let Some(Value::Success(h2_value)) =
                vm.take_notification(h2.call_notification_handle).unwrap()
        );

        vm.sys_state_set("A2".to_owned(), h2_value.clone(), PayloadOptions::default())
            .unwrap();

        if vm
            .do_progress(vec![h1.call_notification_handle])
            .is_err_and(|e| e.is_suspended_error())
        {
            assert_that!(
                vm.take_notification(h1.call_notification_handle),
                err(is_closed())
            );
            return;
        }
        assert2::assert!(
            let Some(Value::Success(h1_value)) =
                vm.take_notification(h1.call_notification_handle).unwrap()
        );

        vm.sys_write_output(
            NonEmptyValue::Success(Bytes::from([&h1_value[..], b"-", &h2_value[..]].concat())),
            PayloadOptions::default(),
        )
        .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn none_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(4)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn a1_and_a2_completed_later() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallInvocationIdCompletionNotificationMessage {
                completion_id: 1,
                invocation_id: "a1".to_string(),
            })
            .input(CallInvocationIdCompletionNotificationMessage {
                completion_id: 3,
                invocation_id: "a2".to_string(),
            })
            .input(CallCompletionNotificationMessage {
                completion_id: 2,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"FRANCESCO").into(),
                )),
            })
            .input(CallCompletionNotificationMessage {
                completion_id: 4,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"TILL").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"A2"),
                value: Some(Bytes::from_static(b"TILL").into()),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"FRANCESCO-TILL")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn a2_and_a1_completed_later() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallCompletionNotificationMessage {
                completion_id: 4,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"TILL").into(),
                )),
            })
            .input(CallCompletionNotificationMessage {
                completion_id: 2,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"FRANCESCO").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"A2"),
                value: Some(Bytes::from_static(b"TILL").into()),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"FRANCESCO-TILL")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn only_a2_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallCompletionNotificationMessage {
                completion_id: 4,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"TILL").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"A2"),
                value: Some(Bytes::from_static(b"TILL").into()),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(2)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn only_a1_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(CallCompletionNotificationMessage {
                completion_id: 2,
                result: Some(call_completion_notification_message::Result::Value(
                    Bytes::from_static(b"FRANCESCO").into(),
                )),
            })
            .run(handler);

        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Francesco"),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<CallCommandMessage>().unwrap(),
            CallCommandMessage {
                service_name: "Greeter".to_owned(),
                handler_name: "greeter".to_owned(),
                parameter: Bytes::from_static(b"Till"),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(4)
        );

        assert_eq!(output.next(), None);
    }
}

mod combinators {
    use super::*;

    use test_log::test;

    #[test]
    fn replay_with_combinator_and_entry_afterwards() {
        let mut output = VMTestCase::new()
            .input(start_message(5))
            .input(input_entry_message(b"my-data"))
            // Two sleep are created
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 1,
                ..Default::default()
            })
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 2,
                ..Default::default()
            })
            // Only one of them completes
            .input(SleepCompletionNotificationMessage {
                completion_id: 2,
                void: Some(Default::default()),
            })
            // Another sleep here
            .input(SleepCommandMessage {
                wake_up_time: 0,
                result_completion_id: 3,
                ..Default::default()
            })
            .run(|vm| {
                vm.sys_input().unwrap();

                // Simulating the user code should be:
                //
                // val a = sleep()
                // val b = sleep()
                // await any(a, b)
                // val c = sleep()
                // await c

                let a_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();
                let b_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();

                // Transition should work fine here!
                assert_that!(
                    vm.do_progress(vec![a_handle, b_handle]),
                    ok(eq(DoProgressResponse::AnyCompleted))
                );
                assert!(!vm.is_completed(a_handle));
                assert!(vm.is_completed(b_handle));

                // Code moves on to c = sleep() and suspends
                let c_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();
                assert_that!(vm.do_progress(vec![c_handle]), err(is_suspended()));
            });

        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(3)
        );
        assert_eq!(output.next(), None);
    }
}
