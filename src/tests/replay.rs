use super::*;

use crate::service_protocol::messages::{
    completion_message, output_entry_message, CompletionMessage, EndMessage, GetStateEntryMessage,
    OutputEntryMessage,
};
use assert2::let_assert;
use test_log::test;

#[test]
fn receive_completion_while_still_getting_entries() {
    let mut output = VMTestCase::new(Version::V1)
        .input(start_message(1))
        .input(CompletionMessage {
            entry_index: 1,
            result: Some(completion_message::Result::Value(Bytes::from_static(
                b"Pippo",
            ))),
        })
        .input(input_entry_message(b"my-data"))
        .run(|vm| {
            vm.sys_input().unwrap();

            let handle = vm.sys_get_state("Personaggio".to_owned()).unwrap();
            let_assert!(Some(Value::Success(b)) = vm.take_async_result(handle).unwrap());
            assert_eq!(b, b"Pippo".to_vec());

            vm.sys_write_output(NonEmptyValue::Success(b)).unwrap();
            vm.sys_end().unwrap();
        });

    assert_eq!(
        output.next_decoded::<GetStateEntryMessage>().unwrap(),
        GetStateEntryMessage {
            key: Bytes::from_static(b"Personaggio"),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<OutputEntryMessage>().unwrap(),
        OutputEntryMessage {
            result: Some(output_entry_message::Result::Value(Bytes::from_static(
                b"Pippo"
            ))),
            ..Default::default()
        }
    );
    assert_eq!(
        output.next_decoded::<EndMessage>().unwrap(),
        EndMessage::default()
    );
    assert_eq!(output.next(), None);
}
