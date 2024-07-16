use super::*;

use crate::service_protocol::messages::*;

use assert2::let_assert;
use test_log::test;

#[test]
fn sleep_suspends() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"abc"),
            debug_id: "abc".to_owned(),
            known_entries: 1,
            partial_state: true,
            ..Default::default()
        })
        .input(InputEntryMessage {
            value: Bytes::from_static(b"Till"),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let h1 = vm.sys_sleep(Duration::from_secs(1)).unwrap();
            vm.notify_await_point(h1);
            let h1_result = vm.take_async_result(h1);
            if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
                return;
            }
            let_assert!(Some(Value::Void) = h1_result.unwrap());

            vm.sys_write_output(NonEmptyValue::Success(vec![])).unwrap();
            vm.sys_end().unwrap();
        });

    let _ = output.next_decoded::<SleepEntryMessage>().unwrap();
    assert_eq!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        SuspensionMessage {
            entry_indexes: vec![1],
        }
    );
    assert_eq!(output.next(), None);
}

#[test]
fn sleep_completed() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"abc"),
            debug_id: "abc".to_owned(),
            known_entries: 2,
            partial_state: true,
            ..Default::default()
        })
        .input(InputEntryMessage {
            value: Bytes::from_static(b"Till"),
            ..Default::default()
        })
        .input(SleepEntryMessage {
            wake_up_time: 1721123699086,
            result: Some(sleep_entry_message::Result::Empty(Empty::default())),
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let h1 = vm.sys_sleep(Duration::from_secs(1)).unwrap();
            vm.notify_await_point(h1);
            let h1_result = vm.take_async_result(h1);
            if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
                return;
            }
            let_assert!(Some(Value::Void) = h1_result.unwrap());

            vm.sys_write_output(NonEmptyValue::Success(vec![])).unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::new())),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn sleep_still_sleeping() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"abc"),
            debug_id: "abc".to_owned(),
            known_entries: 2,
            partial_state: true,
            ..Default::default()
        })
        .input(InputEntryMessage {
            value: Bytes::from_static(b"Till"),
            ..Default::default()
        })
        .input(SleepEntryMessage {
            wake_up_time: 1721123699086,
            ..Default::default()
        })
        .run(|vm| {
            vm.sys_input().unwrap();

            let h1 = vm.sys_sleep(Duration::from_secs(1)).unwrap();
            vm.notify_await_point(h1);
            let h1_result = vm.take_async_result(h1);
            if let Err(SuspendedOrVMError::Suspended(_)) = &h1_result {
                return;
            }
            let_assert!(Some(Value::Void) = h1_result.unwrap());

            vm.sys_write_output(NonEmptyValue::Success(vec![])).unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        SuspensionMessage {
            entry_indexes: vec![1],
        }
    );
    assert_eq!(output.next(), None);
}
