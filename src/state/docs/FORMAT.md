# Storage format

Only this format (v22) is implemented. Use a dedicated namespace. Keys,
encodings and bounds live in [`format.rs`](../format.rs); there is no
migration or format discovery. Storage is assumed reliable (a value reads back as
written); every zstd frame carries a checksum, and decoding checks
structure and the bounds below.

| Store               | Key                                     | Value                                                      |
| ------------------- | --------------------------------------- | ---------------------------------------------------------- |
| Restate             | `v22/head`                              | The head, the commit point; absent until the first flush   |
| Restate             | `v22/tail/<version>/<part>`             | A part of the tail entry of the flush that made `version`  |
| Restate             | `v22/manifest/<version>-<nonce>/<part>` | A part of a checkpoint's manifest; put once, never changed |
| Restate (`KvBlobs`) | `v22/pack/l0/<version>-<nonce>/<part>`  | A part of a pack in L0; put once, never changed            |
| S3 (`BlobStore`)    | `l1/<version>-<nonce>`                  | A pack in L1; put once, never changed                      |

The head is the only key whose write commits. `<version>` and `<part>` in a
tail entry's key are decimal. A pack's `<version>` is the head version
whose state it holds, and `<nonce>` a random number, each 16 lowercase hex
digits: no two puts ever use one name. A checkpoint's manifest is in
Restate wherever its pack is, under the same `<version>-<nonce>`, cut into
parts of 64 KiB (the last shorter), numbered in decimal from 0; a
checkpoint that stores nothing outside its manifest puts no pack. A pack
is in one of two tiers, which its key and every reference to it name. In
L0 (`blobs::KvBlobs`), it is cut into parts of 64 KiB (the last shorter),
numbered in decimal from 0, under `v22/pack/l0/`; in L1, in S3
(`blobs::ObjectStoreBlobs`), it is one object, `l1/<version>-<nonce>`
under the namespace's prefix. Without S3, every pack is in L0.

Integers are LEB128 varints unless stated otherwise. A byte string is its
length, then its bytes. A name is a byte string of at most 255 bytes of
UTF-8. Nothing may follow the last field of a head, a tail entry or a
manifest.

## Head

`SH22`, then, uncompressed:

| Field       | Encoding                                                                                           |
| ----------- | -------------------------------------------------------------------------------------------------- |
| Id          | 8 bytes, little-endian: the database's random identity, drawn when it was made                     |
| Frame pages | Pages per frame (1 to 256), set when the database was made                                         |
| Version     | Flushes that changed something, ever: 1 to 2^62                                                    |
| Checkpoint  | `0` for none, or `1`, the pack's version, nonce, tier (`0` L0, `1` L1), then the manifest's length |
| Previous    | The checkpoint before, which readers that opened on it may still read; as above                    |
| Tail first  | The first version whose tail entry is kept; at least 1                                             |
| Tail bytes  | Bytes of the tail entries since the checkpoint                                                     |
| Parts       | Per version from tail first to the version, the number of parts of its tail entry                  |

The previous checkpoint is older than the checkpoint, or the same version,
and the checkpoint no newer than the version; there is no previous without
a checkpoint. A missing head means version 0, nothing stored, and the frame
size of the options that open it.

## Tail entry

What one flush changed, as it changed it: `SE22`, then, uncompressed (its
pages are zstd frames of their own), cut into parts of at most 1 MiB, put
at `v22/tail/<version>/0` onward. The parts, joined, are at most 1 GiB.

| Field   | Encoding                                                                                                                                                                                                                                                                    |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Records | Count, then per record set: its name, its value (at most 16 MiB)                                                                                                                                                                                                            |
| Files   | Count, then per paged file changed: its name, its size after in 512-byte units (at most 10 GiB), a count, then per page written, ascending: its number as the gap from the page after the one before (the first from 0), and a byte string: empty for a hole, else the page |

A page is a
zstd frame of its 4096 bytes, compressed with the page _before_ it as a raw
prefix: the page as the entries before it in the tail left it, from the
page as the head's checkpoint holds it (its patch, else its frame's page);
where the checkpoint holds none, or the tail made it a hole (a hole
written, or a shrink past it), zeros. Entries
decode in order. A page's number is below 2,621,440 (10 GiB).

## Pack

What one checkpoint wrote, back to back: the frames it rebuilt, the
patches, the values, the ranges it copied from older packs. Its manifest,
which says where they are, is in Restate.

| Range  | A zstd frame of                                                                                   |
| ------ | ------------------------------------------------------------------------------------------------- |
| Frame  | `frame_pages` pages of a file (its pages `index × frame_pages` onward), compressed with no prefix |
| Patch  | One page of a file, compressed with the page as its frame holds it as a raw prefix                |
| Value  | A record's value past `inline_record_bytes`; at most 16 MiB                                       |
| Copied | A range of an older pack, moved as it was (compaction): any of the above                          |

A frame decodes to exactly `frame_pages × 4096` bytes. A frame that would
hold only zeros is not stored. A page of zeros reads as a hole.

## Manifest

`SM22`, then one zstd frame of the body (at most 1 GiB decoded):

| Field   | Encoding                                                                                                                                                                                                                                                                                  |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Packs   | Count, then per pack the manifest names, its own included if it put one, ascending: its version, its nonce, its tier (`0` L0, `1` L1), and the bytes it stored (for compaction)                                                                                                           |
| Records | Count, then per record: its name, its value (below)                                                                                                                                                                                                                                       |
| Files   | Count, then per paged file: its name, its size in 512-byte units (at most 10 GiB), a count, then a location per frame (`0` for none: zeros); then a count of patches, and per patch, ascending: its page as the gap from the page after the one before (the first from 0), and a location |

A value is `0` and a byte string (inline, up to `inline_record_bytes`), or
`1` and a location. A location is its pack's index in the packs above plus
one (`0` is none), its offset as the zigzagged difference from the end of
the location before it in the manifest (the first from 0), and its length.

A file has as many frames as its size needs (`⌈size / (frame_pages ×
4096)⌉`), and patches only below its size, each in a stored frame. A page
with a patch reads from it; any other from its frame. Names are unique
across records and files, at most 65,536 of them. A missing
checkpoint means no files or records.

The files are records and paged files. The SQL database is the paged
file `db`; the records are the host's (the SDK's `outbox`, say). Nothing is
deleted: a record is set (empty reads as never set), a file cut.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the flush, checkpoint, open and
garbage rules.
