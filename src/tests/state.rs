use crate::service_protocol::messages::{start_message::StateEntry, *};
use crate::tests::VMTestCase;
use crate::{CoreVM, NonEmptyValue, SuspendedOrVMError, Value, VM};
use assert2::let_assert;
use bytes::Bytes;
use googletest::assert_that;
use googletest::matchers::pat;
use googletest::prelude::err;

/// Normal state
fn get_state_handler(vm: &mut CoreVM) {
    vm.sys_input().unwrap();

    let h1 = vm.sys_state_get("STATE".to_owned()).unwrap();

    if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
        assert_that!(
            vm.take_notification(h1),
            err(pat!(SuspendedOrVMError::Suspended(_)))
        );
        return;
    }

    let str_result = match vm.take_notification(h1).unwrap().unwrap() {
        Value::Void => "Unknown".to_owned(),
        Value::Success(s) => String::from_utf8(s.to_vec()).unwrap(),
        _ => panic!("Unexpected variants"),
    };

    vm.sys_write_output(NonEmptyValue::Success(Bytes::copy_from_slice(
        str_result.as_bytes(),
    )))
    .unwrap();
    vm.sys_end().unwrap()
}

mod only_lazy_state {
    use super::*;
    use googletest::assert_that;

    use crate::tests::{input_entry_message, is_output_with_success, suspended_waiting_completion};
    use test_log::test;

    #[test]
    fn entry_already_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 3,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            })
            .input(GetLazyStateCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    get_lazy_state_completion_notification_message::Result::Value(
                        Bytes::from_static(b"Francesco").into(),
                    ),
                ),
            })
            .run(get_state_handler);

        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn entry_already_completed_empty() {
        let mut output =
            VMTestCase::new()
                .input(StartMessage {
                    id: Bytes::from_static(b"abc"),
                    debug_id: "abc".to_owned(),
                    known_entries: 3,
                    partial_state: true,
                    ..Default::default()
                })
                .input(input_entry_message(b"Till"))
                .input(GetLazyStateCommandMessage {
                    key: Bytes::from_static(b"STATE"),
                    result_completion_id: 1,
                    ..Default::default()
                })
                .input(GetLazyStateCompletionNotificationMessage {
                    completion_id: 1,
                    result: Some(
                        get_lazy_state_completion_notification_message::Result::Void(
                            Default::default(),
                        ),
                    ),
                })
                .run(get_state_handler);

        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Unknown")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn new_entry() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .run(get_state_handler);

        assert_eq!(
            output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
            GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(1)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn entry_not_completed_on_replay() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 2,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            })
            .run(get_state_handler);

        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(1)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn entry_on_replay_completed_later() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 2,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            })
            .input(GetLazyStateCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    get_lazy_state_completion_notification_message::Result::Value(
                        Bytes::from_static(b"Francesco").into(),
                    ),
                ),
            })
            .run(get_state_handler);

        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn new_entry_completed_later() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    get_lazy_state_completion_notification_message::Result::Value(
                        Bytes::from_static(b"Francesco").into(),
                    ),
                ),
            })
            .run(get_state_handler);

        assert_eq!(
            output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
            GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }
}

/// Eager state
mod eager {
    use super::*;

    use crate::tests::{input_entry_message, is_output_with_success, suspended_waiting_completion};
    use test_log::test;

    fn get_empty_state_handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_state_get("STATE".to_owned()).unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
            assert_that!(
                vm.take_notification(h1),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }

        let str_result = match vm.take_notification(h1).unwrap().unwrap() {
            Value::Void => "true".to_owned(),
            Value::Success(_) => "false".to_owned(),
            _ => panic!("Unexpected variants"),
        };

        vm.sys_write_output(NonEmptyValue::Success(Bytes::copy_from_slice(
            str_result.as_bytes(),
        )))
        .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn get_empty_with_complete_state() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(get_empty_state_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"true")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_empty_with_partial_state() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(get_empty_state_handler);

        assert_eq!(
            output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
            GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(1)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_empty_resume_with_partial_state() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 2,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default(),
                )),
                ..Default::default()
            })
            .run(get_empty_state_handler);

        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"true")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_with_complete_state() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                state_map: vec![StateEntry {
                    key: Bytes::from_static(b"STATE"),
                    value: Bytes::from_static(b"Francesco"),
                }],
                key: "my-greeter".to_owned(),
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(get_state_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Value(
                    Bytes::from_static(b"Francesco").into()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_with_partial_state() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                state_map: vec![StateEntry {
                    key: Bytes::from_static(b"STATE"),
                    value: Bytes::from_static(b"Francesco"),
                }],
                partial_state: true,
                key: "my-greeter".to_owned(),
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(get_state_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Value(
                    Bytes::from_static(b"Francesco").into()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_with_partial_state_without_the_state_entry() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(get_state_handler);

        assert_eq!(
            output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
            GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(1)
        );
        assert_eq!(output.next(), None);
    }

    fn append_state_handler(vm: &mut CoreVM) {
        let input = vm.sys_input().unwrap().input;

        let h1 = vm.sys_state_get("STATE".to_owned()).unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
            assert_that!(
                vm.take_notification(h1),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }

        let get_result = match vm.take_notification(h1).unwrap().unwrap() {
            Value::Void => {
                panic!("Unexpected empty get state")
            }
            Value::Success(s) => s,
            Value::Failure(f) => {
                vm.sys_write_output(NonEmptyValue::Failure(f)).unwrap();
                vm.sys_end().unwrap();
                return;
            }
            _ => panic!("Unexpected variants"),
        };

        vm.sys_state_set(
            "STATE".to_owned(),
            Bytes::from([get_result.clone(), input.clone()].concat()),
        )
        .unwrap();

        let h2 = vm.sys_state_get("STATE".to_owned()).unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h2]) {
            assert_that!(
                vm.take_notification(h2),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }

        let second_get_result = match vm.take_notification(h2).unwrap().unwrap() {
            Value::Void => {
                panic!("Unexpected empty get state")
            }
            Value::Success(s) => s,
            Value::Failure(f) => {
                vm.sys_write_output(NonEmptyValue::Failure(f)).unwrap();
                vm.sys_end().unwrap();
                return;
            }
            _ => panic!("Unexpected variants"),
        };

        vm.sys_write_output(NonEmptyValue::Success(second_get_result))
            .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn append_with_state_in_the_state_map() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                state_map: vec![StateEntry {
                    key: Bytes::from_static(b"STATE"),
                    value: Bytes::from_static(b"Francesco"),
                }],
                partial_state: true,
                key: "my-greeter".to_owned(),
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .run(append_state_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Value(
                    Bytes::from_static(b"Francesco").into()
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                value: Some(Bytes::from_static(b"FrancescoTill").into()),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Value(
                    Bytes::from_static(b"FrancescoTill").into()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"FrancescoTill")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn append_with_partial_state_on_the_first_get() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    get_lazy_state_completion_notification_message::Result::Value(
                        Bytes::from_static(b"Francesco").into(),
                    ),
                ),
            })
            .run(append_state_handler);

        assert_eq!(
            output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
            GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<SetStateCommandMessage>().unwrap(),
            SetStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                value: Some(Bytes::from_static(b"FrancescoTill").into()),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Value(
                    Bytes::from_static(b"FrancescoTill").into()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"FrancescoTill")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    fn get_and_clear_state_handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_state_get("STATE".to_owned()).unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
            assert_that!(
                vm.take_notification(h1),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }
        let first_get_result = match vm.take_notification(h1).unwrap().unwrap() {
            Value::Void => {
                panic!("Unexpected empty get state")
            }
            Value::Success(s) => s,
            _ => panic!("Unexpected variants"),
        };

        vm.sys_state_clear("STATE".to_owned()).unwrap();

        let h2 = vm.sys_state_get("STATE".to_owned()).unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h2]) {
            assert_that!(
                vm.take_notification(h2),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }
        let_assert!(Ok(Some(Value::Void)) = vm.take_notification(h2));

        vm.sys_write_output(NonEmptyValue::Success(first_get_result))
            .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn get_and_clear_state_with_state_in_the_state_map() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                state_map: vec![StateEntry {
                    key: Bytes::from_static(b"STATE"),
                    value: Bytes::from_static(b"Francesco"),
                }],
                partial_state: true,
                key: "my-greeter".to_owned(),
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .run(get_and_clear_state_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Value(
                    Bytes::from_static(b"Francesco").into()
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<ClearStateCommandMessage>().unwrap(),
            ClearStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_and_clear_state_with_partial_state_on_the_first_get() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    get_lazy_state_completion_notification_message::Result::Value(
                        Bytes::from_static(b"Francesco").into(),
                    ),
                ),
            })
            .run(get_and_clear_state_handler);

        assert_eq!(
            output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
            GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            output.next_decoded::<ClearStateCommandMessage>().unwrap(),
            ClearStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    fn get_and_clear_all_state_handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_state_get("STATE".to_owned()).unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
            assert_that!(
                vm.take_notification(h1),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }
        let first_get_result = match vm.take_notification(h1).unwrap().unwrap() {
            Value::Void => {
                panic!("Unexpected empty get state")
            }
            Value::Success(s) => s,
            _ => panic!("Unexpected variants"),
        };

        vm.sys_state_clear_all().unwrap();

        let h2 = vm.sys_state_get("STATE".to_owned()).unwrap();
        vm.do_progress(vec![h2]).unwrap();
        let_assert!(Ok(Some(Value::Void)) = vm.take_notification(h2));

        let h3 = vm.sys_state_get("ANOTHER_STATE".to_owned()).unwrap();
        vm.do_progress(vec![h3]).unwrap();
        let_assert!(Ok(Some(Value::Void)) = vm.take_notification(h3));

        vm.sys_write_output(NonEmptyValue::Success(first_get_result))
            .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn get_clear_all_with_state_in_the_state_map() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                state_map: vec![
                    StateEntry {
                        key: Bytes::from_static(b"STATE"),
                        value: Bytes::from_static(b"Francesco"),
                    },
                    StateEntry {
                        key: Bytes::from_static(b"ANOTHER_STATE"),
                        value: Bytes::from_static(b"Francesco"),
                    },
                ],
                partial_state: true,
                key: "my-greeter".to_owned(),
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .run(get_and_clear_all_state_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Value(
                    Bytes::from_static(b"Francesco").into()
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<ClearAllStateCommandMessage>()
                .unwrap(),
            ClearAllStateCommandMessage::default()
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"ANOTHER_STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn get_clear_all_with_partial_state_on_the_first_get() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateCompletionNotificationMessage {
                completion_id: 1,
                result: Some(
                    get_lazy_state_completion_notification_message::Result::Value(
                        Bytes::from_static(b"Francesco").into(),
                    ),
                ),
            })
            .run(get_and_clear_all_state_handler);

        assert_eq!(
            output.next_decoded::<GetLazyStateCommandMessage>().unwrap(),
            GetLazyStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<ClearAllStateCommandMessage>()
                .unwrap(),
            ClearAllStateCommandMessage::default()
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"ANOTHER_STATE"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"Francesco")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    fn consecutive_get_with_empty_handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_state_get("key-0".to_owned()).unwrap();
        vm.do_progress(vec![h1]).unwrap();
        let_assert!(Ok(Some(Value::Void)) = vm.take_notification(h1));

        let h2 = vm.sys_state_get("key-0".to_owned()).unwrap();
        vm.do_progress(vec![h2]).unwrap();
        let_assert!(Ok(Some(Value::Void)) = vm.take_notification(h2));

        vm.sys_write_output(NonEmptyValue::Success(Bytes::default()))
            .unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn consecutive_get_with_empty() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .run(consecutive_get_with_empty_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"key-0"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"key-0"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn consecutive_get_with_empty_run_with_replay_of_the_first_get() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 2,
                ..Default::default()
            })
            .input(InputCommandMessage::default())
            .input(GetEagerStateCommandMessage {
                key: Bytes::from_static(b"key-0"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default(),
                )),
                ..Default::default()
            })
            .run(consecutive_get_with_empty_handler);

        assert_eq!(
            output
                .next_decoded::<GetEagerStateCommandMessage>()
                .unwrap(),
            GetEagerStateCommandMessage {
                key: Bytes::from_static(b"key-0"),
                result: Some(get_eager_state_command_message::Result::Void(
                    Default::default()
                )),
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }
}

mod state_keys {
    use super::*;

    use crate::service_protocol::messages::StateKeys;
    use crate::tests::{input_entry_message, is_output_with_success, suspended_waiting_completion};
    use googletest::prelude::*;
    use test_log::test;

    fn get_state_keys_handler(vm: &mut CoreVM) {
        vm.sys_input().unwrap();

        let h1 = vm.sys_state_get_keys().unwrap();

        if let Err(SuspendedOrVMError::Suspended(_)) = vm.do_progress(vec![h1]) {
            assert_that!(
                vm.take_notification(h1),
                err(pat!(SuspendedOrVMError::Suspended(_)))
            );
            return;
        }
        let output = match vm.take_notification(h1).unwrap().unwrap() {
            Value::StateKeys(keys) => NonEmptyValue::Success(Bytes::from(keys.join(","))),
            _ => panic!("Unexpected variants"),
        };

        vm.sys_write_output(output).unwrap();
        vm.sys_end().unwrap()
    }

    #[test]
    fn entry_already_completed() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 2,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetEagerStateKeysCommandMessage {
                value: Some(StateKeys {
                    keys: vec![
                        Bytes::from_static(b"ANOTHER-STATE"),
                        Bytes::from_static(b"MY-STATE"),
                    ],
                }),
                ..Default::default()
            })
            .run(get_state_keys_handler);

        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"ANOTHER-STATE,MY-STATE")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn new_entry() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .run(get_state_keys_handler);

        assert_eq!(
            output
                .next_decoded::<GetLazyStateKeysCommandMessage>()
                .unwrap(),
            GetLazyStateKeysCommandMessage {
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(1)
        );

        assert_eq!(output.next(), None);
    }

    #[test]
    fn new_entry_completed_later() {
        let state_keys = StateKeys {
            keys: vec![
                Bytes::from_static(b"MY-STATE"),
                Bytes::from_static(b"ANOTHER-STATE"),
            ],
        };
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateKeysCompletionNotificationMessage {
                completion_id: 1,
                state_keys: Some(state_keys.clone()),
            })
            .run(get_state_keys_handler);

        assert_eq!(
            output
                .next_decoded::<GetLazyStateKeysCommandMessage>()
                .unwrap(),
            GetLazyStateKeysCommandMessage {
                result_completion_id: 1,
                ..Default::default()
            }
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"MY-STATE,ANOTHER-STATE")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn entry_on_replay_completed_later() {
        let state_keys = StateKeys {
            keys: vec![
                Bytes::from_static(b"MY-STATE"),
                Bytes::from_static(b"ANOTHER-STATE"),
            ],
        };
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 2,
                partial_state: true,
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .input(GetLazyStateKeysCommandMessage {
                result_completion_id: 1,
                ..Default::default()
            })
            .input(GetLazyStateKeysCompletionNotificationMessage {
                completion_id: 1,
                state_keys: Some(state_keys.clone()),
            })
            .run(get_state_keys_handler);

        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"MY-STATE,ANOTHER-STATE")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );
        assert_eq!(output.next(), None);
    }

    #[test]
    fn new_entry_completed_with_eager_state() {
        let mut output = VMTestCase::new()
            .input(StartMessage {
                id: Bytes::from_static(b"abc"),
                debug_id: "abc".to_owned(),
                known_entries: 1,
                partial_state: false,
                state_map: vec![
                    StateEntry {
                        key: Bytes::from_static(b"MY-STATE"),
                        value: Bytes::from_static(b"Francesco"),
                    },
                    StateEntry {
                        key: Bytes::from_static(b"ANOTHER-STATE"),
                        value: Bytes::from_static(b"Till"),
                    },
                ],
                ..Default::default()
            })
            .input(input_entry_message(b"Till"))
            .run(get_state_keys_handler);

        assert_that!(
            output
                .next_decoded::<GetEagerStateKeysCommandMessage>()
                .unwrap(),
            pat!(GetEagerStateKeysCommandMessage {
                value: some(pat!(StateKeys {
                    keys: eq(vec![
                        Bytes::from_static(b"ANOTHER-STATE"),
                        Bytes::from_static(b"MY-STATE")
                    ])
                }))
            })
        );
        assert_that!(
            output.next_decoded::<OutputCommandMessage>().unwrap(),
            is_output_with_success(b"ANOTHER-STATE,MY-STATE")
        );
        assert_eq!(
            output.next_decoded::<EndMessage>().unwrap(),
            EndMessage::default()
        );

        assert_eq!(output.next(), None);
    }
}
