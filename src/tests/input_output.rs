use super::*;

use crate::service_protocol::messages::{EndMessage, OutputCommandMessage, StartMessage};
use crate::PayloadOptions;
use test_log::test;

fn echo_handler(vm: &mut CoreVM) {
    assert2::assert!(let Input { input, .. } = vm.sys_input().unwrap());
    assert_eq!(input, b"my-data".to_vec());

    vm.sys_write_output(NonEmptyValue::Success(input), PayloadOptions::default())
        .unwrap();
    vm.sys_end().unwrap();
}

#[test]
fn echo() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run(echo_handler);

    assert_that!(
        output.next_decoded::<OutputCommandMessage>().unwrap(),
        is_output_with_success(b"my-data")
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}

#[test]
fn headers() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(InputCommandMessage {
            headers: vec![service_protocol::messages::Header {
                key: "x-my-header".to_owned(),
                value: "my-value".to_owned(),
            }],
            value: Some(Bytes::from_static(b"other-value").into()),
            ..InputCommandMessage::default()
        })
        .run(|vm| {
            assert2::assert!(let Input { headers, .. } = vm.sys_input().unwrap());

            assert_that!(
                headers,
                elements_are![eq(Header {
                    key: Cow::Borrowed("x-my-header"),
                    value: Cow::Borrowed("my-value"),
                })]
            );

            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::default()),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

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
fn start_message_v7_fields_surface_on_input() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            scope: Some("tenant-a".to_string()),
            limit_key: Some("user-42".to_string()),
            idempotency_key: Some("idem-7".to_string()),
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            let input = vm.sys_input().unwrap();
            assert_eq!(input.scope, Some("tenant-a".to_string()));
            assert_eq!(input.limit_key, Some("user-42".to_string()));
            assert_eq!(input.idempotency_key, Some("idem-7".to_string()));

            vm.sys_write_output(
                NonEmptyValue::Success(Bytes::default()),
                PayloadOptions::default(),
            )
            .unwrap();
            vm.sys_end().unwrap();
        });

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
fn start_message_v7_fields_dropped_on_v6() {
    let mut vm = CoreVM::mock_init(Version::V6);
    let encoder = Encoder::new(Version::V6);

    // V6 StartMessage proto doesn't have these fields, but the Rust struct does — set them and
    // confirm that the input transition's V7 guard discards them when negotiated version is V6.
    vm.notify_input(encoder.encode(&StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        known_entries: 1,
        scope: Some("tenant-a".to_string()),
        limit_key: Some("user-42".to_string()),
        idempotency_key: Some("idem-7".to_string()),
        ..Default::default()
    }));
    vm.notify_input(encoder.encode(&input_entry_message(b"my-data")));
    vm.notify_input_closed();

    let input = vm.sys_input().unwrap();
    assert_eq!(input.scope, None);
    assert_eq!(input.limit_key, None);
    assert_eq!(input.idempotency_key, None);
}

#[test]
fn replay_output_too() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 2,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .input(OutputCommandMessage {
            result: Some(output_command_message::Result::Value(
                Bytes::from_static(b"my-data").into(),
            )),
            ..OutputCommandMessage::default()
        })
        .run(echo_handler);

    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
