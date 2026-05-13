use super::*;

use crate::service_protocol::messages::*;
use crate::service_protocol::{Decoder, MessageType};
use crate::{PayloadOptions, Value};

use test_log::test;

fn greeter_target() -> Target {
    Target {
        service: "Greeter".to_string(),
        handler: "greeter".to_string(),
        key: None,
        idempotency_key: None,
        scope: None,
        limit_key: None,
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
                vm.do_await(h.into()).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );
            assert_eq!(
                vm.do_await(h.into()).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );

            vm.notify_input_closed();

            assert_that!(vm.do_await(h.into()), err(is_suspended()));
        });

    // Two do_progress calls returned WaitingExternalProgress, each emits AwaitingOnMessage
    // The future is FirstCompleted([Single(signal_17), Single(cancel_signal)])
    for _ in 0..2 {
        assert_that!(
            output.next_decoded::<AwaitingOnMessage>().unwrap(),
            pat!(AwaitingOnMessage {
                awaiting_on: some(pat!(Future {
                    waiting_signals: unordered_elements_are![eq(17), eq(1)],
                    waiting_completions: empty(),
                    waiting_named_signals: empty(),
                    nested_futures: empty(),
                    combinator_type: eq(CombinatorType::FirstCompleted as i32)
                })),
                executing_side_effects: eq(false)
            })
        );
    }
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        suspended_waiting_signal(17)
    );
    assert_eq!(output.next(), None);
}

mod do_await {
    use super::*;

    use test_log::test;

    // Little tool to test do_await
    struct AwaitTest {
        vm: CoreVM,
        encoder: Encoder,
        decoder: Decoder,
        handles: Vec<NotificationHandle>,
        taken: std::collections::HashSet<usize>,
    }

    impl AwaitTest {
        fn given_n_futures(n: usize) -> Self {
            let encoder = Encoder::new(Version::maximum_supported_version());
            let mut vm = CoreVM::mock_init_with_options(
                Version::maximum_supported_version(),
                VMOptions {
                    // We test implicit cancellation elsewhere, let's disable it here to keep these tests simple.
                    implicit_cancellation: ImplicitCancellationOption::Disabled,
                    ..Default::default()
                },
            );

            vm.notify_input(encoder.encode(&StartMessage {
                id: Bytes::from_static(b"123"),
                debug_id: "123".to_string(),
                known_entries: 1,
                ..Default::default()
            }));
            vm.notify_input(encoder.encode(&input_entry_message(b"test")));
            assert!(vm.is_ready_to_execute().unwrap());
            vm.sys_input().unwrap();

            let mut handles = Vec::with_capacity(n);
            for _ in 0..n {
                let (_, h) = vm.sys_awakeable().unwrap();
                handles.push(h);
            }

            Self {
                vm,
                encoder,
                decoder: Decoder::new(Version::maximum_supported_version()),
                handles,
                taken: Default::default(),
            }
        }

        /// Send void notifications for the given 0-based handle indices.
        fn given_notify(&mut self, indices: impl IntoIterator<Item = usize>) -> &mut Self {
            for i in indices {
                let signal_id = 17 + i as u32;
                self.vm
                    .notify_input(self.encoder.encode(&empty_signal_notification(signal_id)));
            }
            self
        }

        /// Send void notifications for the given 0-based handle indices.
        fn given_notify_failure(&mut self, indices: impl IntoIterator<Item = usize>) -> &mut Self {
            for i in indices {
                let signal_id = 17 + i as u32;
                self.vm
                    .notify_input(self.encoder.encode(&SignalNotificationMessage {
                        signal_id: Some(signal_notification_message::SignalId::Idx(signal_id)),
                        result: Some(signal_notification_message::Result::Failure(
                            Default::default(),
                        )),
                    }));
            }
            self
        }

        fn given_input_closed(&mut self) -> &mut Self {
            self.vm.notify_input_closed();
            self
        }

        fn when_await_then_suspended(
            &mut self,
            await_on: UnresolvedFuture,
            reduced: UnresolvedFuture,
        ) {
            let await_on = self.translate_to_handles(await_on);
            let reduced = self.translate_to_handles(reduced);
            let expected_suspension_msg = SuspensionMessage {
                awaiting_on: Some(self.vm.resolve_unresolved_future(reduced)),
            };

            // This should not perform any mutations.
            assert_that!(self.vm.do_await(await_on), err(is_suspended()));
            // Drain all buffered output into our decoder
            while let TakeOutputResult::Buffer(b) = self.vm.take_output() {
                if b.is_empty() {
                    break;
                }
                self.decoder.push(b);
            }

            let msg = self
                .decoder
                .consume_next()
                .expect("decoder consumed")
                .expect("there must be one message");
            assert_that!(msg.ty(), eq(MessageType::Suspension));
            let msg = msg.decode_to::<SuspensionMessage>(0).unwrap();
            assert_that!(msg, eq(expected_suspension_msg));
        }

        /// The `await_on` future is the one we provide to do_await, the `reduced` future is the one we expect to be in AwaitingOnMessage.
        fn when_await_then_awaiting_on(
            &mut self,
            await_on: UnresolvedFuture,
            reduced: UnresolvedFuture,
        ) -> &mut Self {
            let await_on = self.translate_to_handles(await_on);
            let reduced = self.translate_to_handles(reduced);
            let expected_awaiting_on_msg = AwaitingOnMessage {
                awaiting_on: Some(self.vm.resolve_unresolved_future(reduced)),
                executing_side_effects: false,
            };

            // This should not perform any mutations.
            assert_eq!(
                self.vm.do_await(await_on).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false,
                }
            );
            // Drain all buffered output into our decoder
            while let TakeOutputResult::Buffer(b) = self.vm.take_output() {
                if b.is_empty() {
                    break;
                }
                self.decoder.push(b);
            }

            let msg = self
                .decoder
                .consume_next()
                .expect("decoder consumed")
                .expect("there must be one message");
            assert_that!(msg.ty(), eq(MessageType::AwaitingOn));
            let msg = msg.decode_to::<AwaitingOnMessage>(0).unwrap();
            assert_that!(msg, eq(expected_awaiting_on_msg));
            self
        }

        /// Await a future, assert `AnyCompleted` and expect the `completed_indices` are completed. This simulates what `tryComplete()` would do in the SDK.
        /// Future uses 0-based indices that get translated to real handles.
        fn when_await_then_completed(&mut self, fut: UnresolvedFuture) -> &mut Self {
            let fut = self.translate_to_handles(fut);
            assert_eq!(self.vm.do_await(fut).unwrap(), AwaitResponse::AnyCompleted,);
            self
        }

        /// Just checks the completed handles.
        fn expect_completed(
            &mut self,
            completed_indices: impl IntoIterator<Item = usize>,
        ) -> &mut Self {
            let completed = completed_indices.into_iter().collect::<Vec<_>>();
            for (i, h) in self.handles.iter().enumerate() {
                if self.taken.contains(&i) {
                    continue;
                }
                if completed.contains(&i) {
                    assert!(self.vm.is_completed(*h), "expected h{i} to be completed");
                } else {
                    assert!(
                        !self.vm.is_completed(*h),
                        "expected h{i} to NOT be completed"
                    );
                }
            }
            for i in completed {
                let _ = self.vm.take_notification(self.handles[i]);
                self.taken.insert(i);
            }
            self
        }

        /// Translate a future tree built with raw 0-based indices into real handles.
        fn translate_to_handles(&self, fut: UnresolvedFuture) -> UnresolvedFuture {
            match fut {
                UnresolvedFuture::Single(h) => {
                    UnresolvedFuture::Single(self.handles[u32::from(h) as usize])
                }
                UnresolvedFuture::FirstCompleted(c) => UnresolvedFuture::FirstCompleted(
                    c.into_iter()
                        .map(|f| self.translate_to_handles(f))
                        .collect(),
                ),
                UnresolvedFuture::AllCompleted(c) => UnresolvedFuture::AllCompleted(
                    c.into_iter()
                        .map(|f| self.translate_to_handles(f))
                        .collect(),
                ),
                UnresolvedFuture::FirstSucceededOrAllFailed(c) => {
                    UnresolvedFuture::FirstSucceededOrAllFailed(
                        c.into_iter()
                            .map(|f| self.translate_to_handles(f))
                            .collect(),
                    )
                }
                UnresolvedFuture::AllSucceededOrFirstFailed(c) => {
                    UnresolvedFuture::AllSucceededOrFirstFailed(
                        c.into_iter()
                            .map(|f| self.translate_to_handles(f))
                            .collect(),
                    )
                }
                UnresolvedFuture::Unknown(c) => UnresolvedFuture::Unknown(
                    c.into_iter()
                        .map(|f| self.translate_to_handles(f))
                        .collect(),
                ),
            }
        }
    }

    // I'm a bit sick of giving names to tests,
    // and naming tests in rust is so hard because it lacks a test display name feature,
    // so whatev just read the test it's pretty explicative what it does.
    // -- Names are slop anyway --

    // first_completed!(0, 1) with replay [1, 0]
    #[test]
    fn first_completed_returns_first_resolved() {
        AwaitTest::given_n_futures(2)
            .when_await_then_awaiting_on(first_completed!(0, 1), first_completed!(0, 1))
            .given_notify([1, 0])
            .given_input_closed()
            .when_await_then_completed(first_completed!(0, 1))
            .expect_completed([1]);
    }

    // all_completed!(first_completed!(0, 1), all_completed!(0, 1, 2)) with replay [1] then [0] then [2]
    #[test]
    fn nested_all_of_first_and_all_progressive() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_completed!(first_completed!(0, 1), all_completed!(0, 1, 2)),
                all_completed!(first_completed!(0, 1), all_completed!(0, 1, 2)),
            )
            .given_notify([1])
            .when_await_then_completed(all_completed!(
                first_completed!(0, 1),
                all_completed!(0, 1, 2)
            ))
            .expect_completed([1])
            .when_await_then_awaiting_on(
                all_completed!(all_completed!(0, 2)),
                all_completed!(all_completed!(0, 2)),
            )
            .given_notify([0])
            .when_await_then_awaiting_on(
                all_completed!(all_completed!(0, 2)),
                all_completed!(all_completed!(2)),
            )
            .expect_completed([0])
            .given_notify([2])
            .when_await_then_completed(all_completed!(all_completed!(2)))
            .expect_completed([2]);
    }

    // all_completed!(first_completed!(0, 1), all_completed!(0, 1, 2)) with replay [1, 0, 2]
    #[test]
    fn nested_all_of_first_and_all_batch() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_completed!(first_completed!(0, 1), all_completed!(0, 1, 2)),
                all_completed!(first_completed!(0, 1), all_completed!(0, 1, 2)),
            )
            .given_notify([1, 0, 2])
            .when_await_then_completed(all_completed!(
                first_completed!(0, 1),
                all_completed!(0, 1, 2)
            ))
            .expect_completed([1])
            .when_await_then_completed(all_completed!(all_completed!(0, 2)))
            .expect_completed([0, 2]);
    }

    // all_completed!(first_completed!(0, 1), first_completed!(1, 2)) with replay [2, 1]
    #[test]
    fn all_of_two_first_completed_disjoint() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_completed!(first_completed!(0, 1), first_completed!(1, 2)),
                all_completed!(first_completed!(0, 1), first_completed!(1, 2)),
            )
            .given_notify([2, 1])
            .when_await_then_completed(all_completed!(
                first_completed!(0, 1),
                first_completed!(1, 2)
            ))
            .expect_completed([2])
            .when_await_then_completed(all_completed!(first_completed!(0, 1)))
            .expect_completed([1]);
    }

    // all_completed!(first_completed!(0, 1), first_completed!(2, 1)) with replay [2, 1]
    #[test]
    fn all_of_two_first_completed_shared_handle_outer_resolves_first() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_completed!(first_completed!(0, 1), first_completed!(2, 1)),
                all_completed!(first_completed!(0, 1), first_completed!(2, 1)),
            )
            .given_notify([2, 1])
            .when_await_then_completed(all_completed!(
                first_completed!(0, 1),
                first_completed!(2, 1)
            ))
            .expect_completed([2])
            .when_await_then_completed(all_completed!(first_completed!(0, 1)))
            .expect_completed([1]);
    }

    // all_completed!(first_completed!(0, 1), first_completed!(2, 1)) with replay [1, 0]
    #[test]
    fn all_of_two_first_completed_shared_handle_resolves_both() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_completed!(first_completed!(0, 1), first_completed!(2, 1)),
                all_completed!(first_completed!(0, 1), first_completed!(2, 1)),
            )
            .given_notify([1, 0])
            .when_await_then_completed(all_completed!(
                first_completed!(0, 1),
                first_completed!(2, 1)
            ))
            .expect_completed([1]);
    }

    // all_completed!(0, 1) with replay [1]
    #[test]
    fn all_completed_partial_resolution_then_suspends() {
        AwaitTest::given_n_futures(2)
            .given_notify([1])
            .when_await_then_awaiting_on(all_completed!(0, 1), all_completed!(0))
            .expect_completed([1])
            .given_input_closed()
            .when_await_then_suspended(all_completed!(0), all_completed!(0));
    }

    // all_completed!(0, 1) with replay [1]
    #[test]
    fn all_completed_partial_resolution_then_suspends_again() {
        AwaitTest::given_n_futures(2)
            .given_notify([1])
            .when_await_then_awaiting_on(all_completed!(0, 1), all_completed!(0))
            .expect_completed([1])
            .given_input_closed()
            .when_await_then_suspended(all_completed!(0), all_completed!(0));
    }

    // all_completed!(0, 1) with replay [1] and input closed before await
    #[test]
    fn all_completed_input_closed_before_await() {
        AwaitTest::given_n_futures(2)
            .given_notify([1])
            .given_input_closed()
            .when_await_then_suspended(all_completed!(0, 1), all_completed!(0));
    }

    // all_completed!(unknown!(0, 1), first_completed!(2, 1)) with replay [1, 0]
    #[test]
    fn all_completed_with_unknown_inner() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_completed!(unknown!(0, 1), first_completed!(2, 1)),
                all_completed!(unknown!(0, 1), first_completed!(2, 1)),
            )
            .given_notify([1, 0])
            .when_await_then_completed(all_completed!(unknown!(0, 1), first_completed!(2, 1)))
            .expect_completed([1]);
    }

    // first_succeeded_or_all_failed!(unknown!(0, 1), first_completed!(2, 1)) with replay [1, 0]
    #[test]
    fn first_succeeded_or_all_failed_with_unknown_inner() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                first_succeeded_or_all_failed!(unknown!(0, 1), first_completed!(2, 1)),
                first_succeeded_or_all_failed!(unknown!(0, 1), first_completed!(2, 1)),
            )
            .given_notify([1, 0])
            .when_await_then_completed(first_succeeded_or_all_failed!(
                unknown!(0, 1),
                first_completed!(2, 1)
            ))
            .expect_completed([1]);
    }

    // all_succeeded_or_first_failed!(first_completed!(0, 1), first_completed!(2, 3)) with replay [failed 1]
    #[test]
    fn all_succeeded_or_first_failed_short_circuits_on_failure() {
        AwaitTest::given_n_futures(4)
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(first_completed!(0, 1), first_completed!(2, 3)),
                all_succeeded_or_first_failed!(first_completed!(0, 1), first_completed!(2, 3)),
            )
            .given_notify_failure([1, 0, 2, 3])
            .when_await_then_completed(all_succeeded_or_first_failed!(
                first_completed!(0, 1),
                first_completed!(2, 3)
            ))
            .expect_completed([1]);
    }

    // all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)) with replay [failed 1, failed 0]
    #[test]
    fn all_succeeded_or_first_failed_inner_all_completed_fully_fails() {
        AwaitTest::given_n_futures(4)
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)),
                all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)),
            )
            .given_notify_failure([1, 0])
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)),
                all_succeeded_or_first_failed!(all_completed!(2, 3)),
            )
            .expect_completed([1, 0]);
    }

    // all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)) with replay [failed 1]
    #[test]
    fn all_succeeded_or_first_failed_inner_all_completed_partial_failure() {
        AwaitTest::given_n_futures(4)
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)),
                all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)),
            )
            .given_notify_failure([1])
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(all_completed!(0, 1), all_completed!(2, 3)),
                all_succeeded_or_first_failed!(all_completed!(0), all_completed!(2, 3)),
            )
            .expect_completed([1]);
    }

    // all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 1)) with replay [2, 1]
    #[test]
    fn all_succeeded_or_first_failed_unknown_shared_handle_batch() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 1)),
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 1)),
            )
            .given_notify([2, 1])
            .when_await_then_completed(all_succeeded_or_first_failed!(
                unknown!(0, 1),
                all_completed!(2, 1)
            ))
            .expect_completed([2, 1]);
    }

    // all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 1)) with replay [2] then [0]
    #[test]
    fn all_succeeded_or_first_failed_unknown_shared_handle_progressive() {
        AwaitTest::given_n_futures(3)
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 1)),
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 1)),
            )
            .given_notify([2])
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 1)),
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(1)),
            )
            .expect_completed([2])
            .given_notify([0])
            .when_await_then_completed(all_succeeded_or_first_failed!(
                unknown!(0, 1),
                all_completed!(1)
            ));
    }

    // all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 3)) with replay [2, 3, 1]
    #[test]
    fn all_succeeded_or_first_failed_unknown_disjoint_handles_batch() {
        AwaitTest::given_n_futures(4)
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 3)),
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 3)),
            )
            .given_notify([2, 3, 1])
            .when_await_then_completed(all_succeeded_or_first_failed!(
                unknown!(0, 1),
                all_completed!(2, 3)
            ))
            .expect_completed([2, 3, 1]);
    }

    // all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 3)) with replay [2, 3] then [1]
    #[test]
    fn all_succeeded_or_first_failed_unknown_disjoint_handles_progressive() {
        AwaitTest::given_n_futures(4)
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 3)),
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 3)),
            )
            .given_notify([2, 3])
            .when_await_then_awaiting_on(
                all_succeeded_or_first_failed!(unknown!(0, 1), all_completed!(2, 3)),
                all_succeeded_or_first_failed!(unknown!(0, 1)),
            )
            .expect_completed([2, 3])
            .given_notify([1])
            .when_await_then_completed(all_succeeded_or_first_failed!(unknown!(0, 1)))
            .expect_completed([1]);
    }
}

/// Multiple do_progress calls with progressively shrinking future.
/// all_completed(h1, h2, h3): first call nothing ready, second call h1 arrives,
/// third call h2 arrives. Each AwaitingOn should reflect the current state.
#[test]
fn awaiting_on_shrinks_across_calls() {
    let mut output = VMTestCase::new()
        .input(StartMessage {
            id: Bytes::from_static(b"123"),
            debug_id: "123".to_string(),
            known_entries: 1,
            ..Default::default()
        })
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let (_, h1) = vm.sys_awakeable().unwrap(); // signal 17
            let (_, h2) = vm.sys_awakeable().unwrap(); // signal 18
            let (_, h3) = vm.sys_awakeable().unwrap(); // signal 19

            let fut = || all_completed!(h1, h2, h3);

            // First call: nothing ready → WaitingExternalProgress, AwaitingOn has all 3
            assert_eq!(
                vm.do_await(fut()).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );

            // h1 arrives
            vm.notify_input(encoder.encode(&SignalNotificationMessage {
                signal_id: Some(signal_notification_message::SignalId::Idx(17)),
                result: Some(signal_notification_message::Result::Value(
                    Bytes::from_static(b"v1").into(),
                )),
            }));

            // Second call: h1 resolves, h2+h3 still pending → WaitingExternalProgress
            assert_eq!(
                vm.do_await(fut()).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );

            // h2 arrives
            vm.notify_input(encoder.encode(&SignalNotificationMessage {
                signal_id: Some(signal_notification_message::SignalId::Idx(18)),
                result: Some(signal_notification_message::Result::Value(
                    Bytes::from_static(b"v2").into(),
                )),
            }));

            // Third call: h1+h2 resolved, h3 still pending → WaitingExternalProgress
            assert_eq!(
                vm.do_await(fut()).unwrap(),
                AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }
            );

            vm.notify_input_closed();
            assert_that!(vm.do_await(fut()), err(is_suspended()));
        });

    // First AwaitingOn: all 3 handles
    // Cancel wraps: FirstCompleted([AllCompleted([17, 18, 19]), Single(cancel)])
    assert_that!(
        output.next_decoded::<AwaitingOnMessage>().unwrap(),
        pat!(AwaitingOnMessage {
            awaiting_on: some(pat!(Future {
                waiting_signals: eq(vec![1]),
                nested_futures: elements_are![pat!(Future {
                    waiting_signals: unordered_elements_are![eq(17), eq(18), eq(19)],
                    combinator_type: eq(CombinatorType::AllCompleted as i32)
                })],
                combinator_type: eq(CombinatorType::FirstCompleted as i32)
            })),
            executing_side_effects: eq(false)
        })
    );

    // Second AwaitingOn: h1 resolved, only h2+h3 remain
    assert_that!(
        output.next_decoded::<AwaitingOnMessage>().unwrap(),
        pat!(AwaitingOnMessage {
            awaiting_on: some(pat!(Future {
                waiting_signals: eq(vec![1]),
                nested_futures: elements_are![pat!(Future {
                    waiting_signals: unordered_elements_are![eq(18), eq(19)],
                    combinator_type: eq(CombinatorType::AllCompleted as i32)
                })],
                combinator_type: eq(CombinatorType::FirstCompleted as i32)
            })),
            executing_side_effects: eq(false)
        })
    );

    // Third AwaitingOn: h1+h2 resolved, only h3 remains
    // AllCompleted([h3]) stays as-is (no collapse during resolution, only normalization)
    assert_that!(
        output.next_decoded::<AwaitingOnMessage>().unwrap(),
        pat!(AwaitingOnMessage {
            awaiting_on: some(pat!(Future {
                waiting_signals: eq(vec![1]),
                waiting_completions: empty(),
                nested_futures: elements_are![pat!(Future {
                    waiting_signals: eq(vec![19]),
                    combinator_type: eq(CombinatorType::AllCompleted as i32)
                })],
                combinator_type: eq(CombinatorType::FirstCompleted as i32)
            })),
            executing_side_effects: eq(false)
        })
    );

    // Suspension with only h3, same structure
    assert_that!(
        output.next_decoded::<SuspensionMessage>().unwrap(),
        pat!(SuspensionMessage {
            awaiting_on: some(pat!(Future {
                waiting_signals: eq(vec![1]),
                waiting_completions: empty(),
                nested_futures: elements_are![pat!(Future {
                    waiting_signals: eq(vec![19]),
                    combinator_type: eq(CombinatorType::AllCompleted as i32)
                })],
                combinator_type: eq(CombinatorType::FirstCompleted as i32)
            }))
        })
    );
    assert_eq!(output.next(), None);
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
            .do_await(h2.call_notification_handle.into())
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
            .do_await(h1.call_notification_handle.into())
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
                    vm.do_await(first_completed!(a_handle, b_handle)),
                    ok(eq(AwaitResponse::AnyCompleted))
                );
                assert!(!vm.is_completed(a_handle));
                assert!(vm.is_completed(b_handle));

                // Code moves on to c = sleep() and suspends
                let c_handle = vm
                    .sys_sleep(Default::default(), Duration::ZERO, None)
                    .unwrap();
                assert_that!(vm.do_await(c_handle.into()), err(is_suspended()));
            });

        assert_that!(
            output.next_decoded::<SuspensionMessage>().unwrap(),
            suspended_waiting_completion(3)
        );
        assert_eq!(output.next(), None);
    }
}
