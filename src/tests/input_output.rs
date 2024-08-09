use super::*;

use crate::service_protocol::messages::{
    output_entry_message, EndMessage, InputEntryMessage, OutputEntryMessage, StartMessage,
};
use assert2::let_assert;
use test_log::test;

fn echo_handler(vm: &mut CoreVM) {
    let_assert!(Input { input, .. } = vm.sys_input().unwrap());
    assert_eq!(input, b"my-data".to_vec());

    vm.sys_write_output(NonEmptyValue::Success(input)).unwrap();
    vm.sys_end().unwrap();
}

#[test]
fn echo() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            state_map: vec![],
            partial_state: false,
            key: "".to_string(),
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .run(echo_handler);

    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"my-data"
            ))),
            ..OutputEntryMessage::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn headers() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputEntryMessage {
            headers: vec![service_protocol::messages::Header {
                key: "x-my-header".to_owned(),
                value: "my-value".to_owned(),
            }],
            value: Bytes::from_static(b"other-value"),
            ..InputEntryMessage::default()
        })
        .run(|vm| {
            let_assert!(Input { headers, .. } = vm.sys_input().unwrap());

            assert_that!(
                headers,
                elements_are![eq(Header {
                    key: Cow::Borrowed("x-my-header"),
                    value: Cow::Borrowed("my-value"),
                })]
            );

            vm.sys_write_output(NonEmptyValue::Success(vec![])).unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::default())),
            ..OutputEntryMessage::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn replay_output_too() {
    let mut output = VMTestCase::new(Version::V1)
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 2,
            state_map: vec![],
            partial_state: false,
            key: "".to_string(),
        })
        .input(InputEntryMessage {
            headers: vec![],
            value: Bytes::from_static(b"my-data"),
            ..InputEntryMessage::default()
        })
        .input(OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"my-data",
            ))),
            ..OutputEntryMessage::default()
        })
        .run(echo_handler);

    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
