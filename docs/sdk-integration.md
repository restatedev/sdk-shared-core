# Integrating an SDK with `sdk-shared-core`

This document is for people building a new Restate SDK on top of `sdk-shared-core`. It describes how the
SDK is expected to drive the embedded state machine — the **VM** — across the lifetime of an invocation.

The wire-level protocol between the SDK and the Restate runtime is described in
[`service-invocation-protocol.md`](./service-invocation-protocol.md). The VM owns that
protocol end-to-end: it encodes and decodes messages, manages the journal and the replay cursor, tracks
async completions, and enforces protocol invariants. The SDK never touches protobuf or wire bytes
directly — it speaks to the VM through the [`VM` trait](../src/lib.rs).

## Responsibilities

The split between SDK and VM is sharp:

The **VM** owns:
- protocol message framing, encoding, decoding,
- the journal (replay cursor, command index, notification correlation),
- the suspension decision and the `SuspensionMessage` future tree,
- error reporting (`ErrorMessage`) and end-of-invocation framing (`EndMessage`),
- retry policy interaction (`should_pause`, `next_retry_delay`).

The **SDK** owns:
- the HTTP transport: reading the request body, writing the response body,
- pushing input bytes into the VM and pulling output bytes out,
- exposing context APIs to user code,
- scheduling user code, including `ctx.run` closures,
- input/output serde for user payloads.

The VM is single-threaded and does not perform I/O. Every call on a `VM` is synchronous and must be
serialized by the SDK.

## VM states

The VM walks through four states:

```
WaitingPreFlight  →  Replaying  →  Processing  →  Closed
```

- **WaitingPreFlight**: the VM has been constructed but has not yet decoded `StartMessage` and the
  replayed journal. The SDK feeds input until the VM has enough to start.
- **Replaying**: user code is running against a journal that the runtime already knows. No new commands
  are written to the wire; notifications are served from the buffered journal.
- **Processing**: the journal is exhausted. New commands flow out and notifications flow in.
- **Closed**: the invocation lifecycle has ended (success, suspension, or failure). The VM no longer
  accepts syscalls; the SDK only drains output and closes the transport.

## Lifecycle overview

```
┌────────────────────────────────────────────────────────────────────────────┐
│ 1. Construct VM with request headers                                       │
│ 2. Write response head                                                     │
│ 3. Bootstrap: notify_input(...) until is_ready_to_execute()                │
│ 4. sys_input() → build user-facing context                                 │
│ 5. Wire up input/output handling                                           │
│ 6. Run user code                                                           │
│      └─ each ctx.<op> → sys_<op> + (await) progress loop                   │
│ 7. Commit result: sys_write_output(...) + sys_end()                        │
│ 8. Flush output and close                                                  │
└────────────────────────────────────────────────────────────────────────────┘
```

The rest of this document walks each step.

## 1 — Construction and response head

The SDK constructs a `CoreVM` from the incoming request headers and immediately retrieves the HTTP
response head — status code, content-type, and any headers the VM wants on the response. The head is
stable for the lifetime of the invocation, so the SDK can flush it before knowing the body.

If the request is malformed (wrong content-type, unsupported protocol version, etc.), the response
head is a non-2xx status. The SDK writes that head, closes the response, and stops.

## 2 — Bootstrapping (initial input loop)

Before user code can start, the VM needs enough input bytes to consume `StartMessage` and the
replayed journal. The protocol is request-pull: the VM tells the SDK when it has seen enough.

```text
while !vm.is_ready_to_execute()? {
    match read_next_chunk() {
        Some(bytes) => vm.notify_input(bytes),
        None        => vm.notify_input_closed(),
    }
}
```

`notify_input` appends bytes to the VM's input buffer; `notify_input_closed` signals EOF. The loop
terminates as soon as the VM has parsed `StartMessage` + `known_entries` messages — at which point
**user code can start running**.

The very first step of running user code is fetching the input entry: the SDK calls `sys_input`,
which consumes the `InputCommand` from the journal and returns the invocation envelope (id, key,
headers, body, random seed, scope, limit key, idempotency key). The SDK uses this to build the
user-facing context and seed the deterministic RNG, then hands off to the user's handler.

## 3 — Input and output

Once user code starts running, the SDK has to keep two byte streams flowing: bytes from the runtime
in, bytes from the VM out.

### Input

The SDK feeds the request body into the VM via `notify_input(chunk)` and `notify_input_closed()` for
EOF. There are two valid strategies, and the choice is the SDK's:

- **Eager pumping**: a background task continuously reads chunks from the request body and forwards
  them to the VM as they arrive. It signals the await loop on every chunk so a pending notification
  that just became ready can be picked up immediately.
- **On-demand pulling**: the SDK only reads from the request body when the await loop returns
  `WaitingExternalProgress` (i.e. when the VM has nothing more to do until fresh input arrives), then
  pushes whatever it got into `notify_input`.

The eager approach minimizes latency at the cost of an extra background task; the on-demand approach
keeps everything in a single control flow at the cost of slightly later visibility of completions.
Both are correct.

### Output

The SDK pulls bytes out of the VM with `take_output()` and writes them to the response body.
`take_output()` returns `Buffer(bytes)` when there is data buffered, or `EOF` once the VM has been
closed and fully drained.

The output side does not need to be drained on every VM call — the VM buffers output internally and
nothing is lost. Drain it at the points where stalling the wire would block progress:

- **Before and after `do_progress`** — before, so the runtime sees everything the SDK has produced up
  to the await point; after, because `do_progress` itself can have produced more bytes (for instance
  a `SuspensionMessage`).
- **After proposing a run completion** (`propose_run_completion`) — so the
  `ProposeRunCompletionMessage` is flushed and the runtime can produce the corresponding ack.
- **Before closing the transport** — drain `take_output()` until it returns `EOF`, so the final
  `EndMessage` / `SuspensionMessage` / `ErrorMessage` actually reaches the runtime.

## 4 — Issuing commands

Each context API maps to a `sys_*` call on the VM. They split into two flavours, matching the
[completable / non-completable](./service-invocation-protocol.md#commands) command split on the wire:

- **Non-completable**: write a command and return immediately (state mutations, `sys_send`,
  `sys_complete_awakeable`, …).
- **Completable**: write a command and return one or more notification handles representing pending
  results. The SDK stashes the handle(s) in the user-facing future and later drives them through the
  progress loop.

## 5 — The progress loop

To complete one or more notification handles, the SDK runs the **progress loop**. Conceptually:

```text
loop {
    if vm.is_completed(handle) {
        return vm.take_notification(handle)?;
    }

    match vm.do_progress(future)? {
        AnyCompleted             => { /* loop again — at least one handle is ready */ }
        ExecuteRun(handle)       => execute_run_closure(handle),
        CancelSignalReceived     => return cancelled(),
        WaitingExternalProgress  => { flush_output(); await_external_progress(); }
    }
}
```

The `future` passed to `do_progress` is a tree of handles combined with promise-style combinators
(`FirstCompleted`, `AllCompleted`, `FirstSucceededOrAllFailed`, `AllSucceededOrFirstFailed`). The same
tree is reused as the `SuspensionMessage` payload if the VM decides to suspend — so combinators are
first-class, not a client-side concern.

The four responses:

- **`AnyCompleted`** — at least one leaf moved. The caller drains the leaves that completed via
  `take_notification` and re-enters the loop if the user-facing combinator still has unresolved
  children. `AnyCompleted` does not mean the whole tree is resolved.
- **`WaitingExternalProgress`** — the suspension threshold. The VM has done all the local work it
  can and is blocked waiting for either fresh input from the runtime or a pending run completion
  from the SDK. The SDK MUST flush output before yielding, otherwise the messages the VM produced
  never reach the runtime. If the VM decides the invocation should actually suspend rather than keep
  waiting, it produces a `SuspensionMessage` on the next `take_output` and transitions to `Closed`
  — the SDK simply drains and closes.
- **`ExecuteRun(handle)`** — the user code is awaiting a run handle whose closure has not been
  scheduled yet. The SDK runs the closure and then calls `propose_run_completion`; see
  [`ctx.run`](#7--ctxrun).
- **`CancelSignalReceived`** — only produced when implicit cancellation is enabled. The VM has
  observed a `CANCEL` signal; the SDK should propagate cancellation to the user-facing future.

When the SDK uses an eager input pump (see [§3](#3--input-and-output)), the two external sources of
progress — fresh input bytes and a freshly-proposed run completion — both need to wake the await
loop. A single "external progress channel" works well: the input pump signals it when fresh bytes
arrive, the run-execution code signals it on `propose_run_completion`. When the SDK pulls input
on-demand instead, only the run side needs explicit signalling.

## 6 — Taking notification results

Once a handle is completed, the SDK reads the result with `take_notification(handle)`. The value is
moved out, not copied — call it exactly once per handle. The user-facing future stores the value and
resolves itself in whatever representation it exposes (typed result, thrown terminal error, etc.).

## 7 — `ctx.run`

`ctx.run` is the only construct that lets user code execute arbitrary non-deterministic work and
durably persist its result. The flow:

1. The SDK calls `sys_run(name)` and gets a handle. This writes a `RunCommand` to the journal.
2. When user code awaits the resulting future, the progress loop eventually returns
   `ExecuteRun(handle)`. This is the VM telling the SDK: "the closure for this run hasn't been
   executed yet — go execute it." On replay, the run was already executed in a previous attempt, so
   `ExecuteRun` is never returned; the result is replayed as a normal notification.
3. The SDK runs the user closure. When it finishes, the SDK calls `propose_run_completion` with the
   outcome — success, terminal failure, or retryable failure — and a retry policy.
4. The VM persists the proposal and, on the runtime's ack, surfaces the completion through
   `take_notification` like any other awaitable.

The VM combines the SDK-supplied retry policy with the runtime's default to decide what to do on
retryable failures (retry with delay, pause, or fail). The SDK does not implement the retry decision
itself.

## 8 — Terminating the invocation

When user code completes, the SDK commits the result via `sys_write_output` (with the success or
failure value) followed by `sys_end`. For a non-terminal failure, the SDK calls `notify_error`
instead (see [§9](#9--error-reporting)).

After either path, the SDK enters its flush loop: pull from `take_output` until it returns `EOF`,
writing each buffer to the response body. The final `EndMessage` / `SuspensionMessage` /
`ErrorMessage` rides out on this drain. The SDK should also let the request stream close on its own
side before closing the response body — when the runtime is in request/response mode, it may still
be writing trailing bytes.

## 9 — Error reporting

`notify_error` is the single entrypoint for non-terminal failures. The SDK passes the error together
with an optional pointer to the related command (last command, next command, or a specific command
index) — the VM uses this to populate the `related_command_*` fields of `ErrorMessage`.

`notify_error` can be called at any point in the VM lifecycle. After it returns, the VM is on its way
to `Closed`; further `sys_*` calls will fail. The SDK still needs to drain output so the
`ErrorMessage` reaches the runtime.

How user-thrown exceptions are mapped to "terminal" or "retryable" is the SDK's choice. Terminal
user errors take the normal output path (`sys_write_output` with a failure value, then `sys_end`).
Retryable errors go through `notify_error`, optionally with a per-attempt delay override or a
`should_pause` instruction (see
[the protocol failures section](./service-invocation-protocol.md#failures)).

## 10 — Operational rules of thumb

A handful of invariants are worth keeping in mind:

- **Serialize VM calls.** The VM is `&mut self` throughout. Wrap it in whatever single-owner primitive
  the host language offers and never call into it from two tasks at once.
- **Drain output at the points listed in [§3](#output).** Missing a flush at a yield point stalls the
  wire.
- **`state().is_processing()` is the replay-aware logging gate.** While the VM is replaying, suppress
  user-visible log output — the runtime will see those logs again on the next replay anyway. Only
  during `Processing` are logs and side effects happening for real.
- **`last_command_index()` is the correlation key for errors.** When a `sys_*` call fails because the
  payload couldn't be encoded, snapshot `last_command_index()` before the failure and attach it to
  the eventual `notify_error`.
- **Don't fight the suspension model.** If the progress loop returns `WaitingExternalProgress` and
  neither input nor a pending run can deliver new progress, the VM will suspend. That's correct
  behaviour, not an error.

## Reference

- [`src/lib.rs`](../src/lib.rs) — the `VM` trait and all surface types.
- [`src/vm/mod.rs`](../src/vm/mod.rs) — the concrete `CoreVM` implementation.
- [`service-invocation-protocol.md`](./service-invocation-protocol.md) —
  the wire protocol the VM speaks on behalf of the SDK.