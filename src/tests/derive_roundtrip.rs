//! Round-trip tests: encode via `proto::Message`, compare bytes to a
//! parallel `prost::Message`-derived struct, and round-trip decode.
//!
//! These tests prove that our derive emits *byte-identical* wire output to
//! stock prost for the InMemory-only payload case — i.e. the host-buffer
//! plumbing in `RestateBuf` / `RestateBufMut` doesn't change the format,
//! it just adds an opt-in dispatch path that's invisible on the wire.
//!
//! These tests originally lived in `restate-wire-derive/tests/roundtrip.rs`
//! but moved here once the derive macro hard-coded its emit paths to
//! `::restate_sdk_shared_core::proto::*`: that absolute path only resolves
//! when the derive is invoked from a crate that has the `proto` module
//! reachable, i.e. this one.

use crate::proto::{self, Message as _};
use crate::Buffer;
use ::bytes::Bytes;

use crate::{HostBufferHandle, HostBufferRegistry};

/// A `HostBufferRegistry` that panics on every method. Use in tests that
/// only exercise `Buffer::InMemory` — calls into the registry would
/// indicate the test accidentally hit a host-buffer code path.
struct PanickingRegistry;

impl HostBufferRegistry for PanickingRegistry {
    fn eq(&self, _: &HostBufferHandle, _: &HostBufferHandle) -> bool {
        panic!("PanickingRegistry::eq called");
    }
    fn read_into(&self, _: &HostBufferHandle, _: usize, _: usize, _: &mut [u8]) {
        panic!("PanickingRegistry::read_into called");
    }
    fn retain(&self, _: &HostBufferHandle) {
        panic!("PanickingRegistry::retain called");
    }
    fn release(&self, _: &HostBufferHandle) {
        panic!("PanickingRegistry::release called");
    }
}

// =====================================================================
// β'' struct using `restate_wire_derive::Message`
// =====================================================================

#[derive(Clone, PartialEq, restate_wire_derive::Message)]
struct WireMsg {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(uint32, tag = "2")]
    count: u32,
    #[prost(bytes = "bytes", tag = "3")]
    blob: Bytes,
    #[prost(bytes = "buffer", tag = "4")]
    body: Buffer,
}

// =====================================================================
// Parallel struct using stock `prost::Message`
// =====================================================================

#[derive(Clone, PartialEq, prost::Message)]
struct ProstMsg {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(uint32, tag = "2")]
    count: u32,
    #[prost(bytes = "bytes", tag = "3")]
    blob: Bytes,
    // `bytes = "bytes"` here stands in for the `Buffer` field — when the
    // payload is `InMemory(b)`, both sides emit exactly the same bytes for
    // a `bytes` field at the same tag.
    #[prost(bytes = "bytes", tag = "4")]
    body: Bytes,
}

#[test]
fn byte_identity_with_prost_all_fields_set() {
    let body_bytes = Bytes::from_static(b"hello world payload");
    let wire = WireMsg {
        name: "fred".to_owned(),
        count: 42,
        blob: Bytes::from_static(b"\x01\x02\x03"),
        body: Buffer::InMemory(body_bytes.clone()),
    };
    let prost = ProstMsg {
        name: "fred".to_owned(),
        count: 42,
        blob: Bytes::from_static(b"\x01\x02\x03"),
        body: body_bytes.clone(),
    };

    let mut wire_buf: Vec<u8> = Vec::new();
    wire.encode_raw(&mut wire_buf);

    let mut prost_buf: Vec<u8> = Vec::new();
    prost::Message::encode_raw(&prost, &mut prost_buf);

    assert_eq!(
        wire_buf, prost_buf,
        "proto::Message::encode_raw must be byte-identical to prost::Message::encode_raw"
    );

    // encoded_len must also agree.
    let _host = PanickingRegistry;
    assert_eq!(
        proto::Message::encoded_len(&wire),
        prost::Message::encoded_len(&prost),
    );
    assert_eq!(wire_buf.len(), proto::Message::encoded_len(&wire));
}

#[test]
fn byte_identity_with_prost_empty_payload() {
    // Empty Buffer(Inline(empty)) — exercise the length-prefix edge case.
    let wire = WireMsg {
        name: String::new(),
        count: 0,
        blob: Bytes::new(),
        body: Buffer::InMemory(Bytes::new()),
    };
    // prost elides zero-default scalar fields and length-prefixed empties
    // for `bytes` and `string`. Our derive does the same EXCEPT for the
    // `bytes = "payload"` field — by design, Buffer always emits (the
    // discriminator carries the info).
    let mut wire_buf: Vec<u8> = Vec::new();
    wire.encode_raw(&mut wire_buf);

    let _host = PanickingRegistry;
    assert_eq!(wire_buf.len(), proto::Message::encoded_len(&wire));

    // The Buffer field at tag=4 should be present (key + 0 len = 2 bytes:
    // tag-byte 0x22 then varint length 0x00).
    assert_eq!(wire_buf, vec![0x22, 0x00]);

    // Decode back.
    let decoded = WireMsg::decode(Bytes::from(wire_buf)).expect("decode");
    assert_eq!(decoded, wire);
}

#[test]
fn round_trip_via_decode() {
    let body_bytes = Bytes::from_static(b"some payload data \x00\x01\xff");
    let original = WireMsg {
        name: "alice".to_owned(),
        count: 7,
        blob: Bytes::from_static(b"abcdef"),
        body: Buffer::InMemory(body_bytes.clone()),
    };

    let mut buf: Vec<u8> = Vec::new();
    original.encode_raw(&mut buf);

    let decoded = WireMsg::decode(Bytes::from(buf.clone())).expect("decode");
    assert_eq!(decoded, original);

    // Also: decoding via prost (since the bytes are identical) must
    // produce the parallel struct.
    let prost_decoded =
        <ProstMsg as prost::Message>::decode(Bytes::from(buf)).expect("prost decode");
    assert_eq!(prost_decoded.name, original.name);
    assert_eq!(prost_decoded.count, original.count);
    assert_eq!(prost_decoded.blob, original.blob);
    assert_eq!(prost_decoded.body, body_bytes);
}

#[test]
fn nonzero_scalar_default_elision() {
    // Confirm we still elide zero-valued scalar fields exactly like prost.
    let wire = WireMsg {
        name: String::new(),
        count: 99,
        blob: Bytes::new(),
        body: Buffer::InMemory(Bytes::from_static(b"x")),
    };
    let prost = ProstMsg {
        name: String::new(),
        count: 99,
        blob: Bytes::new(),
        body: Bytes::from_static(b"x"),
    };

    let mut a: Vec<u8> = Vec::new();
    wire.encode_raw(&mut a);
    let mut b: Vec<u8> = Vec::new();
    prost::Message::encode_raw(&prost, &mut b);
    assert_eq!(a, b);
}
