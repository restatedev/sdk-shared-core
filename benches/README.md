# Benchmarks

These are standalone Cargo bench targets (`harness = false`), separate from
tests and examples. All use the current (v22) storage format, a tail in
Restate and checkpoints in packs, and require the crate's `sql` and
`__bench` features. They count logical KV operations and bytes in MemoryKv; they do
not model a real remote backend.

Against a real Restate server, `pnpm --filter e2e bench` from the root of
the sdk-sessions repository runs its end-to-end benchmark.

Run from the crate's root:

```sh
cargo bench --locked --features sql,__bench --bench flush_costs
S3=1 cargo bench --locked --features sql,__bench --bench flush_costs   # packs in S3, priced
S3=1 cargo bench --locked --features sql,__bench --bench read_latency  # uncached reads, timed
cargo bench --locked --features sql,__bench --bench amplification -- 3 > amplification.csv
cargo bench --locked --features sql,__bench --bench small_databases -- 3 > small_databases.csv
cargo bench --locked --features sql,__bench --bench cache_cpu -- 3 > cache_cpu.csv
```

The positional argument is the repeat count; the default is three. Cargo's
`--bench` argument is filtered by each custom entry point.

## Targets

| Target            | Coverage                                                                                                                                                                                                                                                                                                                                      | Evidence                                                       |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `flush_costs`     | Requests, bytes and round trips per flush (its share of checkpoints and collections included), stored size and cold reads, by workload, up to 1M rows; `S3=1` keeps packs in S3 and prices them; `FRAME_PAGES`, `INLINE_BYTES`, `CHECKPOINT_ENTRIES`, `CHECKPOINT_BYTES` set the options, `WORKLOAD` and `FLUSHES` pick and age the databases | [ARCHITECTURE.md](../src/state/docs/ARCHITECTURE.md#6-numbers) |
| `read_latency`    | Uncached reads, each on a fresh open: the open, then a point read by key or index or a range of 100 rows, twice; Restate and S3 on one virtual clock (`RESTATE_MS`, `S3_MS`): milliseconds and round trips per store                                                                                                                          | [ARCHITECTURE.md](../src/state/docs/ARCHITECTURE.md#6-numbers) |
| `amplification`   | PK/range/index/scan reads, mutations, rollback, recovery; four database sizes                                                                                                                                                                                                                                                                 | CSV, on stdout                                                 |
| `small_databases` | Tiny databases, index counts, commit grouping and a larger control; tail and head bytes written                                                                                                                                                                                                                                               | CSV, on stdout                                                 |
| `cache_cpu`       | Process-wide memory and disk cache by memory size: misses, round trips, allocations and CPU time                                                                                                                                                                                                                                              | CSV, on stdout                                                 |

All SQL result checks remain active in release builds. Successful writes are
reopened and verified.

## Measurements

`amplification` resets counters between open, SQL/query, the engine's WAL
checkpoint, save and recovery-open phases. Each phase has a total row
(`all`) plus a row per kind of key: `head`, `tail`, `pack` (packs kept in
Restate) and `other`. Do not add totals to their components. Warm query
observations reuse one session; cold totals include initialization. Unique
keys are per-phase counts and are not summed across phases.

| Metric                          | Numerator                               | Denominator                        |
| ------------------------------- | --------------------------------------- | ---------------------------------- |
| Read amplification              | Get keys + returned encoded values      | Returned logical application bytes |
| Application write amplification | Put keys + encoded values + delete keys | Changed logical application bytes  |
| Storage/database ratio          | Live encoded values                     | Logical database bytes             |

Undefined denominators (misses, no-op writes, deletes) remain blank. Delete
traffic contains key bytes; reclaimed value bytes are a separate measure.
Private WAL/database writes are local I/O, not durable KV traffic.
Failed-operation costs are not inferred from successful-operation counters.

Elapsed timings include simulator scheduling and, for SQL, host-driven engine
execution and parsing costs. Three observations do not establish a latency
distribution. Wire framing, replication, disk writes, backend compression and
compaction are not measured.
