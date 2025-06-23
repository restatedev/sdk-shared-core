use super::*;

use crate::service_protocol::messages::*;

use crate::Value;
use assert2::let_assert;
use test_log::test;

fn sleep_handler(vm: &mut CoreVM) {
    vm.sys_input().unwrap();

    let h1 = vm
        .sys_sleep(String::default(), Duration::from_secs(1), None)
        .unwrap();

    if vm
        .do_progress(vec![h1])
        .is_err_and(|e| e.is_suspended_error())
    {
        assert_that!(vm.take_notification(h1), err(is_closed()));
        return;
    }
    let_assert!(Some(Value::Void) = vm.take_notification(h1).unwrap());

    vm.sys_write_output(NonEmptyValue::Success(Bytes::default()))
        .unwrap();
    vm.sys_end().unwrap();
}

#[test]
fn sleep_suspends() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"abc"),
            debug_id: "abc".to_owned(),
            known_entries: 1,
            partial_state: true,
            ..Default::default()
        })
        .input(input_entry_message(b"Till"))
        .run(sleep_handler);

    assert_that!(
        output.next_decoded::<SleepCommandMessage>().unwrap(),
        pat!(SleepCommandMessage {
            result_completion_id: eq(1)
        })
    );
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_completion(1)
    );
    assert_eq!(output.next(), None);
}

#[test]
fn sleep_completed() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"abc"),
            debug_id: "abc".to_owned(),
            known_entries: 3,
            partial_state: true,
            ..Default::default()
        })
        .input(input_entry_message(b"Till"))
        .input(SleepCommandMessage {
            wake_up_time: 1721123699086,
            result_completion_id: 1,
            ..Default::default()
        })
        .input(SleepCompletionNotificationMessage {
            completion_id: 1,
            void: Some(Default::default()),
        })
        .run(sleep_handler);

    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success("")
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn sleep_still_sleeping() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"abc"),
            debug_id: "abc".to_owned(),
            known_entries: 2,
            partial_state: true,
            ..Default::default()
        })
        .input(input_entry_message(b"Till"))
        .input(SleepCommandMessage {
            wake_up_time: 1721123699086,
            result_completion_id: 1,
            ..Default::default()
        })
        .run(sleep_handler);

    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_completion(1)
    );
    assert_eq!(output.next(), None);
}
