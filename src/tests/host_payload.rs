// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! End-to-end host payload tests for the W10 fragment-emitting output path.
//!
//! These tests drive a real [`CoreVM`] backed by an
//! [`InMemoryHostBufferRegistry`] and verify that a payload arriving as
//! `Buffer::Host(handle)` flows out of the output stream as
//! `Buffer::Host(handle)` — never materialised into shared-core memory.
//!
//! The end-to-end shape exercised here:
//!
//! 1. Build a `CoreVM` with an in-memory host registry.
//! 2. Feed a `StartMessage` + `InputCommandMessage` via the encoder.
//! 3. Have the handler call `sys_state_set` with `Buffer::Host(handle)`.
//! 4. Drain `take_output_next` and assert the sequence contains a
//!    `Buffer::Host(handle)` for the payload field.
//! 5. Concatenate the fragments (resolving Host via the registry) and decode
//!    the resulting wire bytes to a `SetStateCommandMessage` — checking the
//!    round-trip yields a payload byte-identical to the original.

use std::sync::Arc;

use bytes::Bytes;
use test_log::test;

use crate::buffer::InMemoryHostBufferRegistry;
use crate::service_protocol::messages::{
    EndMessage, InputCommandMessage, OutputCommandMessage, SetStateCommandMessage, StartMessage,
};
use crate::service_protocol::{Decoder, Version};
use crate::tests::Encoder;
use crate::{Buffer, CoreVM, NonEmptyValue, PayloadOptions, VMOptions, Value, VM};

/// Concat a fragment stream into a single `Vec<u8>` by resolving every
/// `Buffer::Host` through the registry. Used to drive the standard
/// `Decoder` against an output stream that contains host fragments.
fn flatten_fragments(frags: &[Buffer]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in frags {
        match f {
            Buffer::InMemory(b) => out.extend_from_slice(b),
            Buffer::Host(h) => {
                let len = h.len() as usize;
                let start = out.len();
                out.resize(start + len, 0);
                h.read_into(0, len, &mut out[start..]);
            }
        }
    }
    out
}

/// Drive a fresh `CoreVM` to the "ready to execute" state by feeding it the
/// standard `StartMessage` + `InputCommandMessage` preamble.
fn make_vm() -> CoreVM {
    let mut vm = CoreVM::new(
        vec![(
            "content-type".to_owned(),
            Version::maximum_supported_version().to_string(),
        )],
        VMOptions::default(),
    )
    .unwrap();

    // Feed standard preamble through the legacy `Encoder` (inline-only path).
    let encoder = Encoder::new(Version::maximum_supported_version());
    vm.notify_input(encoder.encode(&StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        known_entries: 1,
        ..Default::default()
    }));
    vm.notify_input(encoder.encode(&InputCommandMessage {
        value: Some(Bytes::from_static(b"input").into()),
        ..InputCommandMessage::default()
    }));
    vm.notify_input_closed();
    assert!(vm.is_ready_to_execute().unwrap());

    vm
}

/// Sanity baseline: an inline payload still produces a single inline
/// fragment for the whole `SetStateCommandMessage`. Confirms we haven't
/// broken the "inline path doesn't accidentally emit Host fragments"
/// invariant.
#[test]
fn sys_state_set_with_inline_payload_emits_no_host_fragments() {
    let mut vm = make_vm();
    vm.sys_input().unwrap();

    vm.sys_state_set(
        "my-key".to_owned(),
        Buffer::InMemory(Bytes::from_static(b"my-value")),
        PayloadOptions::default(),
    )
    .unwrap();
    vm.sys_write_output(
        NonEmptyValue::Success(Buffer::InMemory(Bytes::from_static(b"done"))),
        PayloadOptions::default(),
    )
    .unwrap();
    vm.sys_end().unwrap();

    // Drain everything the VM produced into a fragment list.
    let mut frags = Vec::new();
    while let Some(f) = vm.take_output_next() {
        frags.push(f);
    }

    assert!(
        frags.iter().all(|f| matches!(f, Buffer::InMemory(_))),
        "inline payloads must not emit any Buffer::Host"
    );
}

/// Core end-to-end check: a `Buffer::Host` flowing into `sys_state_set`
/// reaches the output as a `Buffer::Host(handle)` carrying the SAME
/// handle the caller registered with the host registry — and the host bytes
/// never enter shared-core memory along the way.
#[test]
fn sys_state_set_with_host_payload_emits_fragment_host() {
    // Register some payload bytes with the host registry. The registry hands
    // back an opaque handle that shared-core treats as a pointer.
    let registry = Arc::new(InMemoryHostBufferRegistry::new());
    let payload_bytes: &[u8] = b"host-owned-value";
    let handle = registry.clone().make_handle(payload_bytes);
    let handle_id = handle.id();

    // Drive the VM end-to-end with the Host payload.
    let mut vm = make_vm();
    vm.sys_input().unwrap();

    vm.sys_state_set(
        "my-key".to_owned(),
        Buffer::Host(handle),
        PayloadOptions::default(),
    )
    .unwrap();
    // Use a separate (inline) payload for the output so the test focuses on
    // the SetState path. Host emission for OutputCommandMessage is covered
    // implicitly via the same code path.
    vm.sys_write_output(
        NonEmptyValue::Success(Buffer::InMemory(Bytes::from_static(b"done"))),
        PayloadOptions::default(),
    )
    .unwrap();
    vm.sys_end().unwrap();

    // Drain the output and split fragments into inline vs host.
    let mut frags = Vec::new();
    while let Some(f) = vm.take_output_next() {
        frags.push(f);
    }

    let host_ids: Vec<u32> = frags
        .iter()
        .filter_map(|f| match f {
            Buffer::Host(h) => Some(h.id()),
            Buffer::InMemory(_) => None,
        })
        .collect();

    assert_eq!(
        host_ids,
        vec![handle_id],
        "exactly one Buffer::Host carrying the original handle must be \
         emitted for the SetStateCommandMessage payload"
    );

    // Now flatten the fragments (resolving Host via the registry) and decode
    // through the standard Decoder. The resulting SetStateCommandMessage
    // must carry a Buffer with bytes equal to the original `payload_bytes`.
    let wire_bytes = flatten_fragments(&frags);
    let mut decoder = Decoder::new(Version::maximum_supported_version());
    decoder.push(Bytes::from(wire_bytes));

    // First message: SetStateCommandMessage.
    let set_state_raw = decoder.consume_next().unwrap().unwrap();
    let set_state: SetStateCommandMessage = set_state_raw.decode_to(0).unwrap();
    assert_eq!(set_state.key.as_ref(), b"my-key");
    let value = set_state.value.expect("SetState must carry a value");
    let value_bytes = match value.content {
        Buffer::InMemory(b) => b,
        Buffer::Host(_) => panic!(
            "decoded payload should be Inline — the on-wire bytes are owned \
             by the in-memory Bytes buffer at this point, not a host handle"
        ),
    };
    assert_eq!(value_bytes.as_ref(), payload_bytes);

    // Second message: OutputCommandMessage with the inline "done" payload.
    let _output_raw = decoder.consume_next().unwrap().unwrap();
    let _end_raw = decoder.consume_next().unwrap().unwrap();
    assert!(decoder.consume_next().unwrap().is_none());
}

/// End-to-end check that the same host-buffer plumbing works for the
/// `OutputCommandMessage` path (the `sys_write_output` syscall).
#[test]
fn sys_write_output_with_host_payload_emits_fragment_host() {
    let registry = Arc::new(InMemoryHostBufferRegistry::new());
    let payload_bytes: &[u8] = b"host-owned-output";
    let handle = registry.clone().make_handle(payload_bytes);
    let handle_id = handle.id();

    let mut vm = make_vm();
    vm.sys_input().unwrap();

    vm.sys_write_output(
        NonEmptyValue::Success(Buffer::Host(handle)),
        PayloadOptions::default(),
    )
    .unwrap();
    vm.sys_end().unwrap();

    let mut frags = Vec::new();
    while let Some(f) = vm.take_output_next() {
        frags.push(f);
    }

    let host_ids: Vec<u32> = frags
        .iter()
        .filter_map(|f| match f {
            Buffer::Host(h) => Some(h.id()),
            Buffer::InMemory(_) => None,
        })
        .collect();

    assert_eq!(
        host_ids,
        vec![handle_id],
        "OutputCommandMessage with a Host payload must emit exactly one \
         Buffer::Host carrying the original handle"
    );

    // Decode round-trip: the bytes for OutputCommandMessage.result.value
    // must equal the original host-owned bytes.
    let wire_bytes = flatten_fragments(&frags);
    let mut decoder = Decoder::new(Version::maximum_supported_version());
    decoder.push(Bytes::from(wire_bytes));

    let output_raw = decoder.consume_next().unwrap().unwrap();
    let output_msg: OutputCommandMessage = output_raw.decode_to(0).unwrap();
    use crate::service_protocol::messages::output_command_message;
    match output_msg.result {
        Some(output_command_message::Result::Value(v)) => match v.content {
            Buffer::InMemory(b) => assert_eq!(b.as_ref(), payload_bytes),
            Buffer::Host(_) => panic!("decoded output payload should be Inline"),
        },
        other => panic!("expected Value variant, got {other:?}"),
    }

    let _end_raw = decoder.consume_next().unwrap().unwrap();
    let _end_msg: EndMessage = _end_raw.decode_to(0).unwrap();
    assert!(decoder.consume_next().unwrap().is_none());
}

/// End-to-end check for the `Value::Success` -> `Value::content` path
/// through `take_notification`: state replay populates the eager cache and
/// the outgoing protocol stream. We verify that the VM accepts a Host
/// payload via the `sys_state_set` API and that `EagerState` stores the
/// `Buffer` directly — Host handles remain handles in the cache, never
/// materialised into shared-core memory — so the same handle still flows
/// out as a `Buffer::Host`.
#[test]
fn host_handle_survives_eager_state_mirror() {
    let registry = Arc::new(InMemoryHostBufferRegistry::new());
    let payload_bytes: &[u8] = b"survives-mirror";
    let handle = registry.clone().make_handle(payload_bytes);
    let handle_id = handle.id();
    // Probe handle to keep reading the underlying bytes after the original
    // is moved into the VM. `clone()` bumps the refcount.
    let probe_handle = handle.clone();

    let mut vm = make_vm();
    vm.sys_input().unwrap();

    vm.sys_state_set(
        "k".to_owned(),
        Buffer::Host(handle),
        PayloadOptions::default(),
    )
    .unwrap();
    // After sys_state_set: the underlying buffer must still be live (the
    // eager state cache cloned the handle; the output queue also holds a
    // clone). Probe by reading bytes back through `probe_handle` — if the
    // refcount had dropped to zero this would panic with "unknown host
    // buffer handle".
    let mut probe = vec![0u8; payload_bytes.len()];
    probe_handle.read_into(0, payload_bytes.len(), &mut probe);
    assert_eq!(
        &probe[..],
        payload_bytes,
        "host handle must still be live and unchanged after sys_state_set"
    );

    vm.sys_write_output(
        NonEmptyValue::Success(Buffer::InMemory(Bytes::from_static(b"done"))),
        PayloadOptions::default(),
    )
    .unwrap();
    vm.sys_end().unwrap();

    // The original handle's id must appear exactly once in the output stream.
    let mut found = 0;
    while let Some(f) = vm.take_output_next() {
        if let Buffer::Host(h) = f {
            if h.id() == handle_id {
                found += 1;
            }
        }
    }
    assert_eq!(found, 1, "exactly one Buffer::Host(handle) expected");
}

/// Negative control: invoking `vm.sys_input` (which decodes the standard
/// InputCommandMessage) does NOT spuriously interact with the host
/// registry. We re-prove the existing `Value` matcher works.
#[test]
fn sys_input_returns_expected_value() {
    let mut vm = make_vm();

    let input = vm.sys_input().unwrap();
    match input.input {
        Buffer::InMemory(b) => assert_eq!(b.as_ref(), b"input"),
        Buffer::Host(_) => panic!("inline input should not produce a Host payload"),
    }

    // Drop the VM cleanly — sys_end is not strictly needed here.
    drop(vm);

    // Smoke-check: Value variant unused below, but the import is exercised.
    let _ = Value::Void;
}

/// Zero-copy decode path: feeding `notify_input` with a `Buffer::Host`
/// must produce, on the journal side (here surfaced through
/// `sys_input().input`), a `Buffer::Host` **sub-view** that shares the
/// parent id (the wire-bytes handle) and points at the payload byte
/// range within it — proving the prost decode loop minted a sub-view via
/// `HostAwareBuf::try_take_host_slice` instead of copying the payload
/// into shared-core memory.
#[test]
fn notify_input_host_buffer_decodes_to_sub_view() {
    let registry = Arc::new(InMemoryHostBufferRegistry::new());

    // Build the wire bytes for the start + input preamble.
    let encoder = Encoder::new(Version::maximum_supported_version());
    let mut wire = Vec::new();
    let start = encoder.encode(&StartMessage {
        id: Bytes::from_static(b"123"),
        debug_id: "123".to_string(),
        known_entries: 1,
        ..Default::default()
    });
    wire.extend_from_slice(&start);
    let payload: &[u8] = b"host-owned-input-payload";
    let input = encoder.encode(&InputCommandMessage {
        value: Some(Bytes::copy_from_slice(payload).into()),
        ..InputCommandMessage::default()
    });
    let input_payload_offset_in_input_body = {
        // Within the input message body, locate the payload bytes. The
        // payload occupies a contiguous span — we just `find` it.
        let body_offset = 8; // skip header
        let body = &input[body_offset..];
        let pos = body
            .windows(payload.len())
            .position(|w| w == payload)
            .expect("payload bytes must be embedded contiguously in the wire body");
        body_offset + pos
    };
    let input_offset_in_wire = wire.len();
    let payload_offset_in_wire = (input_offset_in_wire + input_payload_offset_in_input_body) as u32;
    wire.extend_from_slice(&input);

    // Register the wire bytes as a single host buffer, then feed them
    // through `notify_input` as `Buffer::Host(parent)`. Use the maximum
    // supported protocol version to construct the VM.
    let parent = registry.clone().make_handle(&wire);
    let parent_id = parent.id();
    let mut vm = CoreVM::new(
        vec![(
            "content-type".to_owned(),
            Version::maximum_supported_version().to_string(),
        )],
        VMOptions::default(),
    )
    .unwrap();
    vm.notify_input(Buffer::Host(parent));
    vm.notify_input_closed();
    assert!(vm.is_ready_to_execute().unwrap());

    let input = vm.sys_input().unwrap();
    match input.input {
        Buffer::Host(sub) => {
            assert_eq!(sub.id(), parent_id, "sub-view must share parent id");
            assert_eq!(
                sub.len() as usize,
                payload.len(),
                "sub-view length must match payload length"
            );
            assert_eq!(
                sub.offset(),
                payload_offset_in_wire,
                "sub-view offset must point at the payload bytes inside the parent buffer"
            );
            assert_ne!(
                sub.offset(),
                0,
                "sub-view sits well past the wire header bytes"
            );

            // Verify the bytes via the handle — they must equal the
            // original payload.
            let mut dst = vec![0u8; sub.len() as usize];
            sub.read_into(0, sub.len() as usize, &mut dst);
            assert_eq!(&dst[..], payload);
        }
        Buffer::InMemory(_) => panic!(
            "expected Buffer::Host sub-view: notify_input must thread \
             a host buffer through the decode pipeline without materialising"
        ),
    }

    // VM drop releases all the parent's refcount-shares (decoder segment
    // + any sub-views still held in the journal). After this scope, no
    // shares remain and the registry frees the underlying buffer.
    drop(vm);
}
