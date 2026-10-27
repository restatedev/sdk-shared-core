//! Repeated deliveries of the same signal, see https://github.com/restatedev/sdk-shared-core/issues/99

use super::*;

use crate::{SignalDelivery, Value, CANCEL_NOTIFICATION_HANDLE};
use test_log::test;

// --- SignalDelivery::Queued (default)

/// Journal: input, signal a(1), signal a(2).
/// Code: sys_input, create_signal_handle(a), await, create_signal_handle(a), await.
#[test]
fn replay_two_named_signals_then_await_directly() {
    VMTestCase::new()
        .input(start_message(3))
        .input(input_entry_message(b"my-data"))
        .input(signal_notification("a", b"1"))
        .input(signal_notification("a", b"2"))
        .run(|vm| {
            vm.sys_input().unwrap();

            let h1 = vm.create_signal_handle("a".into()).unwrap();
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(h1)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(h1),
                ok(some(eq(Value::Success(Bytes::from_static(b"1")))))
            );

            let h2 = vm.create_signal_handle("a".into()).unwrap();
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(h2)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(h2),
                ok(some(eq(Value::Success(Bytes::from_static(b"2")))))
            );
        });
}

/// Journal: input, signal a(1), signal a(2), awakeable 17 completion.
/// Code: sys_input, awakeable, await awakeable, create_signal_handle(a), await, create_signal_handle(a), await.
///
/// Awaiting the awakeable pops both a(1) and a(2) into `ready`: both must be kept, in order.
#[test]
fn replay_two_named_signals_while_awaiting_something_else() {
    VMTestCase::new()
        .input(start_message(4))
        .input(input_entry_message(b"my-data"))
        .input(signal_notification("a", b"1"))
        .input(signal_notification("a", b"2"))
        .input(empty_signal_notification(17))
        .run(|vm| {
            vm.sys_input().unwrap();

            let awk = vm.sys_awakeable().unwrap().handle;
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(vm.take_notification(awk), ok(some(eq(Value::Void))));

            let h1 = vm.create_signal_handle("a".into()).unwrap();
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(h1)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(h1),
                ok(some(eq(Value::Success(Bytes::from_static(b"1")))))
            );

            let h2 = vm.create_signal_handle("a".into()).unwrap();
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(h2)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(h2),
                ok(some(eq(Value::Success(Bytes::from_static(b"2")))))
            );
        });
}

/// Live run of the scenario above: only input in the journal, notifications arrive one by one
/// while awaiting the awakeable.
#[test]
fn live_two_named_signals_while_awaiting_something_else() {
    VMTestCase::new()
        .input(start_message(1))
        .input(input_entry_message(b"my-data"))
        .run_without_closing_input(|vm, encoder| {
            vm.sys_input().unwrap();

            let awk = vm.sys_awakeable().unwrap().handle;
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }))
            );
            vm.notify_input(encoder.encode(&signal_notification("a", b"1")));
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::WaitingExternalProgress {
                    waiting_input: true,
                    waiting_run_proposal: false
                }))
            );
            vm.notify_input(encoder.encode(&signal_notification("a", b"2")));
            vm.notify_input(encoder.encode(&empty_signal_notification(17)));
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(vm.take_notification(awk), ok(some(eq(Value::Void))));

            let h1 = vm.create_signal_handle("a".into()).unwrap();
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(h1)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(h1),
                ok(some(eq(Value::Success(Bytes::from_static(b"1")))))
            );

            let h2 = vm.create_signal_handle("a".into()).unwrap();
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(h2)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(h2),
                ok(some(eq(Value::Success(Bytes::from_static(b"2")))))
            );
        });
}

/// Journal: input, awakeable 17 completed with 1, awakeable 17 completed with 2.
/// Code: awakeable 17, await it.
#[test]
fn replay_awakeable_completed_twice_then_await_directly() {
    VMTestCase::new()
        .input(start_message(3))
        .input(input_entry_message(b"my-data"))
        .input(signal_notification(17, b"1"))
        .input(signal_notification(17, b"2"))
        .run(|vm| {
            vm.sys_input().unwrap();

            let awk = vm.sys_awakeable().unwrap().handle;
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(awk),
                ok(some(eq(Value::Success(Bytes::from_static(b"1")))))
            );
        });
}

/// Journal: input, awakeable 17 completed with 1, awakeable 17 completed with 2, awakeable 18 completed.
/// Code: awakeable 17, awakeable 18, await 18, await 17.
#[test]
fn replay_awakeable_completed_twice_while_awaiting_something_else() {
    VMTestCase::new()
        .input(start_message(4))
        .input(input_entry_message(b"my-data"))
        .input(signal_notification(17, b"1"))
        .input(signal_notification(17, b"2"))
        .input(empty_signal_notification(18))
        .run(|vm| {
            vm.sys_input().unwrap();

            let awk17 = vm.sys_awakeable().unwrap().handle;
            let awk18 = vm.sys_awakeable().unwrap().handle;
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk18)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(vm.take_notification(awk18), ok(some(eq(Value::Void))));

            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk17)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(
                vm.take_notification(awk17),
                ok(some(eq(Value::Success(Bytes::from_static(b"1")))))
            );
        });
}

/// Journal: input, cancel, cancel, awakeable 17 completed.
/// Code (implicit cancellation disabled): awakeable 17, await it, then await the cancel handle twice.
#[test]
fn replay_cancel_delivered_twice_while_awaiting_something_else() {
    VMTestCase::with_vm_options(VMOptions {
        implicit_cancellation: ImplicitCancellationOption::Disabled,
        ..VMOptions::default()
    })
    .input(start_message(4))
    .input(input_entry_message(b"my-data"))
    .input(cancel_signal_notification())
    .input(cancel_signal_notification())
    .input(empty_signal_notification(17))
    .run(|vm| {
        vm.sys_input().unwrap();

        let awk = vm.sys_awakeable().unwrap().handle;
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(vm.take_notification(awk), ok(some(eq(Value::Void))));

        assert_that!(
            vm.do_await(UnresolvedFuture::Single(CANCEL_NOTIFICATION_HANDLE)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(CANCEL_NOTIFICATION_HANDLE),
            ok(some(eq(Value::Void)))
        );
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(CANCEL_NOTIFICATION_HANDLE)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(CANCEL_NOTIFICATION_HANDLE),
            ok(some(eq(Value::Void)))
        );
    });
}

/// Same journal, implicit cancellation enabled: every await also races the cancel handle,
/// so each cancel is observed.
#[test]
fn replay_cancel_delivered_twice_with_implicit_cancellation() {
    VMTestCase::new()
        .input(start_message(4))
        .input(input_entry_message(b"my-data"))
        .input(cancel_signal_notification())
        .input(cancel_signal_notification())
        .input(empty_signal_notification(17))
        .run(|vm| {
            vm.sys_input().unwrap();

            let awk = vm.sys_awakeable().unwrap().handle;
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::CancelSignalReceived))
            );
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::CancelSignalReceived))
            );
            assert_that!(
                vm.do_await(UnresolvedFuture::Single(awk)),
                ok(eq(AwaitResponse::AnyCompleted))
            );
            assert_that!(vm.take_notification(awk), ok(some(eq(Value::Void))));
        });
}

// --- SignalDelivery::Overwrite: a delivery not yet taken is replaced by the next one

#[test]
fn overwrite_replay_two_named_signals_then_await_directly() {
    VMTestCase::with_vm_options(VMOptions {
        signal_delivery: SignalDelivery::Overwrite,
        ..VMOptions::default()
    })
    .input(start_message(3))
    .input(input_entry_message(b"my-data"))
    .input(signal_notification("a", b"1"))
    .input(signal_notification("a", b"2"))
    .run(|vm| {
        vm.sys_input().unwrap();

        let h1 = vm.create_signal_handle("a".into()).unwrap();
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(h1)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(h1),
            ok(some(eq(Value::Success(Bytes::from_static(b"1")))))
        );

        let h2 = vm.create_signal_handle("a".into()).unwrap();
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(h2)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(h2),
            ok(some(eq(Value::Success(Bytes::from_static(b"2")))))
        );
    });
}

#[test]
fn overwrite_replay_two_named_signals_while_awaiting_something_else() {
    VMTestCase::with_vm_options(VMOptions {
        signal_delivery: SignalDelivery::Overwrite,
        ..VMOptions::default()
    })
    .input(start_message(4))
    .input(input_entry_message(b"my-data"))
    .input(signal_notification("a", b"1"))
    .input(signal_notification("a", b"2"))
    .input(empty_signal_notification(17))
    .run(|vm| {
        vm.sys_input().unwrap();

        let awk = vm.sys_awakeable().unwrap().handle;
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(vm.take_notification(awk), ok(some(eq(Value::Void))));

        let h1 = vm.create_signal_handle("a".into()).unwrap();
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(h1)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(h1),
            ok(some(eq(Value::Success(Bytes::from_static(b"2")))))
        );

        let h2 = vm.create_signal_handle("a".into()).unwrap();
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(h2)),
            err(is_suspended())
        );
    });
}

#[test]
fn overwrite_live_two_named_signals_while_awaiting_something_else() {
    VMTestCase::with_vm_options(VMOptions {
        signal_delivery: SignalDelivery::Overwrite,
        ..VMOptions::default()
    })
    .input(start_message(1))
    .input(input_entry_message(b"my-data"))
    .run_without_closing_input(|vm, encoder| {
        vm.sys_input().unwrap();

        let awk = vm.sys_awakeable().unwrap().handle;
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk)),
            ok(eq(AwaitResponse::WaitingExternalProgress {
                waiting_input: true,
                waiting_run_proposal: false
            }))
        );
        vm.notify_input(encoder.encode(&signal_notification("a", b"1")));
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk)),
            ok(eq(AwaitResponse::WaitingExternalProgress {
                waiting_input: true,
                waiting_run_proposal: false
            }))
        );
        vm.notify_input(encoder.encode(&signal_notification("a", b"2")));
        vm.notify_input(encoder.encode(&empty_signal_notification(17)));
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(vm.take_notification(awk), ok(some(eq(Value::Void))));

        let h1 = vm.create_signal_handle("a".into()).unwrap();
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(h1)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(h1),
            ok(some(eq(Value::Success(Bytes::from_static(b"2")))))
        );

        vm.notify_input_closed();
        let h2 = vm.create_signal_handle("a".into()).unwrap();
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(h2)),
            err(is_suspended())
        );
    });
}

#[test]
fn overwrite_replay_awakeable_completed_twice_while_awaiting_something_else() {
    VMTestCase::with_vm_options(VMOptions {
        signal_delivery: SignalDelivery::Overwrite,
        ..VMOptions::default()
    })
    .input(start_message(4))
    .input(input_entry_message(b"my-data"))
    .input(signal_notification(17, b"1"))
    .input(signal_notification(17, b"2"))
    .input(empty_signal_notification(18))
    .run(|vm| {
        vm.sys_input().unwrap();

        let awk17 = vm.sys_awakeable().unwrap().handle;
        let awk18 = vm.sys_awakeable().unwrap().handle;
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk18)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(vm.take_notification(awk18), ok(some(eq(Value::Void))));

        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk17)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(awk17),
            ok(some(eq(Value::Success(Bytes::from_static(b"2")))))
        );
    });
}

#[test]
fn overwrite_replay_cancel_delivered_twice_while_awaiting_something_else() {
    VMTestCase::with_vm_options(VMOptions {
        implicit_cancellation: ImplicitCancellationOption::Disabled,
        signal_delivery: SignalDelivery::Overwrite,
        ..VMOptions::default()
    })
    .input(start_message(4))
    .input(input_entry_message(b"my-data"))
    .input(cancel_signal_notification())
    .input(cancel_signal_notification())
    .input(empty_signal_notification(17))
    .run(|vm| {
        vm.sys_input().unwrap();

        let awk = vm.sys_awakeable().unwrap().handle;
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(awk)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(vm.take_notification(awk), ok(some(eq(Value::Void))));

        assert_that!(
            vm.do_await(UnresolvedFuture::Single(CANCEL_NOTIFICATION_HANDLE)),
            ok(eq(AwaitResponse::AnyCompleted))
        );
        assert_that!(
            vm.take_notification(CANCEL_NOTIFICATION_HANDLE),
            ok(some(eq(Value::Void)))
        );
        assert_that!(
            vm.do_await(UnresolvedFuture::Single(CANCEL_NOTIFICATION_HANDLE)),
            err(is_suspended())
        );
    });
}
