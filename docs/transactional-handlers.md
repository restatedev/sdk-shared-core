# Transactional handlers (actor-style execution)

> Status: **exploration / prototype**. Nothing here is a stable API or protocol.

This document describes an execution mode where a handler doesn't journal each step, and instead the whole
invocation is a **single commit boundary**: the handler runs against an in-memory key-value view of its state,
and when it returns, its state mutations, its outgoing messages and its result are committed atomically.

It works on top of the current service protocol (V5+) and the current Restate runtime: no runtime change is needed.
The prototype spans this crate (the VM API) and the TypeScript SDK (`restate.actor(...)` and `ActorContext`).

## Why

A regular Restate handler is a durable program: every context operation is a journal entry, reads included, and on
failure the handler is replayed against its journal, which requires the handler to be deterministic.
That's the right model for workflows and long-running orchestrations.

Many stateful services are better described with the actor model, like Orleans, Akka,
Cloudflare Durable Objects or Flink Stateful Functions:

```
handle(state, message) -> (state', outgoing messages, reply)
```

The handler is short, doesn't need to wait on other services, and what matters is that its effects are atomic.
For these handlers, journaling every step costs a lot and adds little:

* Every read is a journal entry. A handler that does 100 reads and 10 writes writes 110+ entries.
* The handler must be deterministic, because it's replayed.
* State writes and outgoing messages become visible one by one as they're journaled. Atomicity comes only from replay
  eventually completing the handler.
* Reads are async, `await ctx.get(...)`, even though the value is almost always already in the eager state shipped with the invocation.

In transactional mode:

* Reads are **synchronous** and **not journaled**: they're served from the state snapshot plus the
  transaction's own writes.
* Writes are buffered, and coalesced: writing the same key 10 times, or writing back the value it already has, results in at most one mutation.
* Outgoing one-way calls, including delayed ones (timers), are buffered too, and sent only if the transaction
  commits. This is the transactional outbox pattern, for free.
* The handler doesn't need to be deterministic: it's never replayed. If the attempt fails before the commit, the
  handler is executed again from scratch.

## Semantics

Transactional handlers are exclusive handlers of a virtual object: the per-key lock is the actor mailbox, and
invocations for the same key are processed one at a time.

| What happens                                                     | Effect                                                                                      |
|------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| The handler returns a value                                      | State mutations, one-way calls and the value are committed atomically.                      |
| The handler throws a terminal error                              | The transaction is aborted: mutations and calls are discarded, the error is committed as the result. |
| The handler throws a retryable error, or the attempt dies before the commit is durable | Nothing is visible. The next attempt executes the handler again, from scratch.               |
| The attempt dies after the commit is durable                     | The next attempt doesn't execute the handler again: it applies the committed record.        |

Consequences:

* Code with external side effects inside the handler body runs **at least once**, the same as code outside
  `ctx.run` in a regular handler. Effects that must happen once should be expressed as outgoing messages.
* The handler body can't await Restate operations (calls, sleeps, awakeables, durable promises, `ctx.run`).
  Request/response between actors is done with messages: send a message, and receive the reply as another message.
  Regular journaled handlers can still be defined on the same virtual object, and share its state.
* Once started, a transactional invocation ignores cancellation. Cancelling an invocation that hasn't started yet
  works as usual.
* Read-only (shared) handlers read the snapshot without journaling the reads. They don't need a transaction.

## How it maps to the protocol

The runtime applies every journal entry individually: each command the SDK sends is a separate effect, and state
mutations are applied to storage as soon as their `SetState` entry is appended. So a sequence of commands is not
atomic on its own. Atomicity comes from a **commit record**, which works as a redo log:

```
0  Input
1  Run "restate.tx.commit"          ─┐ commit point: the record is durable when the run result is
2  RunCompletion(<commit record>)   ─┘
3  SetState / ClearState / ClearAllState ...   ┐
4  OneWayCall ...                               │ redo: deterministic function of the record
5  Output                                       ┘
   End
```

1. While the handler body runs, nothing is written to the journal. Not even the `RunCommand` is sent until commit time.
2. At commit time the VM encodes the write set, the buffered calls and the output into a `TxCommitRecord`
   (protobuf, versioned, see `src/vm/tx.rs`), and makes it durable as the result of a single `RunCommand` named
   `restate.tx.commit`.
3. Once the record is durable, the VM re-emits it as regular commands.

On every attempt, the journal found at start determines what happens:

| Replayed journal                                           | Behaviour                                                                                   |
|------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `Input`                                                    | Execute the handler body, commit.                                                           |
| `Input`, `Run` (no result)                                 | Execute the handler body, propose the result for the existing `Run`.                        |
| `Input`, `Run`, `RunCompletion`, prefix of the applied commands | Skip the handler body. Apply the record: the prefix is replayed, the rest is written.        |
| Anything else                                              | Journal mismatch.                                                                           |

Because applying the record is deterministic, a half-applied record is completed by the next attempt with the
regular replay mechanics, including the journal mismatch checks.

Reads use the eager state (`StartMessage.state_map`). If the runtime ships a partial snapshot and the handler reads
a key outside of it, the attempt fails (`TX_PARTIAL_STATE`). The runtime ships the whole state unless lazy state is
enabled for the handler, or the state exceeds `worker.invoker.eager-state-size-limit`, which defaults to the message
size limit (32 MiB). The SDK refuses to register transactional handlers with lazy state enabled.

### Pipelined commit

By default, the SDK waits for the commit record to be durable before applying it. This costs a round trip, and
in request/response mode (e.g. AWS Lambda) the invocation suspends right after proposing the commit, and the
runtime re-invokes the deployment to apply it: two HTTP requests per invocation.

The VM can also apply a record proposed in the same attempt immediately: the commands are then streamed right
after the commit proposal, and the invocation completes in a single request. This is safe only if the runtime
stores the proposal before any command that follows it on the same stream, so that whatever prefix of the stream
survives a failure, the commands are never durable without the record. The current runtime behaves this way:
the invoker forwards commands and run completion proposals through the same ordered, fenced effect channel. But
the protocol doesn't guarantee it, so making this the default needs the guarantee written into the protocol.

In the TypeScript prototype the pipelined commit is enabled with `RESTATE_EXPERIMENTAL_ACTOR_PIPELINED_COMMIT=true`.

## VM API

```rust
fn sys_tx_begin(&mut self) -> VMResult<TxBegin>;            // Execute | Committed(handle)
fn tx_state_get(&mut self, key: &str) -> VMResult<Option<Bytes>>;
fn tx_state_get_keys(&mut self) -> VMResult<Vec<String>>;
fn tx_state_set(&mut self, key: String, value: Bytes) -> VMResult<()>;
fn tx_state_clear(&mut self, key: String) -> VMResult<()>;
fn tx_state_clear_all(&mut self) -> VMResult<()>;
fn tx_send(&mut self, target: Target, input: Bytes, execution_time: Option<Duration>, name: Option<String>) -> VMResult<()>;
fn sys_tx_commit(&mut self, output: NonEmptyValue) -> VMResult<NotificationHandle>;
fn sys_tx_end(&mut self) -> VMResult<NonEmptyValue>;         // apply + output + end
```

SDK usage:

```text
input = vm.sys_input()
handle = match vm.sys_tx_begin() {
    Execute => {
        output = run_handler(input)     // uses tx_state_* and tx_send, never awaits Restate operations
        vm.sys_tx_commit(output)        // terminal failure output => mutations and calls are discarded
    }
    Committed(handle) => handle         // don't run the handler
}
await_until(vm.is_completed(handle))    // progress loop, without take_notification; skip it for the pipelined commit
vm.sys_tx_end()
```

`tx_state_get` and `tx_state_get_keys` also work outside a transaction, for read-only handlers.
Once a transaction started, `do_await` stops racing the awaited future against the cancel signal.

## TypeScript API

```ts
const cart = restate.actor({
  name: "cart",
  handlers: {
    add: async (ctx: restate.ActorContext, item: { sku: string; quantity: number }) => {
      // Synchronous, not journaled
      const quantity = ctx.kv.update<number>(`item/${item.sku}`, (q) => (q ?? 0) + item.quantity);
      // Timers are delayed messages to self, committed together with the state
      if (!ctx.kv.has("expiresAt")) {
        ctx.objectSendClient(Cart, ctx.key).expire(restate.rpc.sendOpts({ delay: { hours: 1 } }));
      }
      ctx.kv.set("expiresAt", Date.now() + 3_600_000);
      return quantity;
    },
    checkout: async (ctx: restate.ActorContext) => {
      const items = ctx.kv.entries<number>("item/");
      if (items.length === 0) {
        throw new restate.TerminalError("The cart is empty"); // aborts the transaction
      }
      ctx.serviceSendClient(orders).place({ cart: ctx.key, items }); // sent iff the cart is emptied
      ctx.kv.clear();
      return items;
    },
    items: restate.handlers.actor.shared(async (ctx: restate.ActorSharedContext) => ctx.kv.entries("item/")),
    // A regular journaled handler on the same object, e.g. to await other services
    sync: restate.handlers.object.exclusive(async (ctx: restate.ObjectContext) => { /* ... */ }),
  },
});
```

* `ctx.kv`: `get`, `has`, `keys(prefix?)`, `entries(prefix?)`, `set`, `delete`, `update(key, fn)`, `clear(prefix?)`.
  Every `get` deserializes the stored value, so mutating a returned object doesn't change the state until it's `set`.
* `ctx.serviceSendClient`, `ctx.objectSendClient`, `ctx.workflowSendClient`, `ctx.genericSend`: buffered one-way calls.
* An actor is a virtual object: callers use the regular clients, ingress included.
* `handlers.actor.exclusive` / `handlers.actor.shared` also work inside a regular `restate.object(...)`.

See `packages/examples/node/src/actor.ts` in the TypeScript SDK.

## What the prototype shows

All tests ran against `restate-server` 1.7.13, on a single local node.

* Retries: a handler that writes state and sends a message, then throws a retryable error on its first two attempts,
  ends with exactly one committed write and one delivered message.
* Crash after commit: killing the endpoint process right after the commit became durable made the next attempt apply the
  record without executing the handler body again. The message was delivered once.
* Concurrency: 100 concurrent increments on the same key produce exactly 100.
* Request/response mode: one execution of the handler body per invocation in both modes. Waiting for the commit
  ack takes 2 HTTP requests per invocation; the pipelined commit takes 1.

Latency, p50 over 50 sequential invocations. This is a rough, noisy local measurement, not a benchmark:

| Handler                              | Actor, wait for commit ack | Actor, pipelined commit | Journaled object |
|--------------------------------------|----------------------------|-------------------------|------------------|
| increment (1 read, 1 write)          | 7.5 ms                     | 7.1 ms                  | 5.3–6.9 ms       |
| 100 reads, 10 writes                 | 7.8 ms                     | 6.7 ms                  | 13.1–13.3 ms     |

On today's runtime, transactional handlers win when they do many reads. On tiny handlers they cost a bit more,
because of the extra journal entries and because every write is stored twice: once in the record, once as a command.

## Limitations of the prototype

* The whole state must fit in the eager state snapshot.
* Write amplification: written values are stored twice, and every invocation adds the `Run` command and its completion.
* The journal value codec isn't supported in the TypeScript SDK: decoding is async, while `ctx.kv` reads are synchronous.
* Nothing is sent to the runtime until the commit, so long handler bodies run into the inactivity timeout.
  The storage journal mode, below, lets long handlers commit along the way.
* The commit record must fit in the message size limit.
* The Restate UI shows the commit as a `Run` entry named `restate.tx.commit`.

## Storage journal mode: multiple commit points without determinism

Actor handlers have one commit point per invocation. To have several, and to await calls, sleeps and signals
between them, without bringing back the determinism requirement, the VM has a second mode, `JournalMode::Storage`:
the journal is used as a **durable store of results, looked up by name**, instead of a log to replay.

On every attempt the handler runs from the beginning as regular code. Nothing is replayed and no determinism check is
performed. `sys_restore`, called right after `sys_input`, indexes the journal of the previous attempts and moves
straight to processing. From there:

| Operation                         | Key (n-th occurrence in the attempt: `key#n`) | If the key is in the journal                    | Otherwise              |
|-----------------------------------|-----------------------------------------------|-------------------------------------------------|------------------------|
| transaction (step)                | `tx:<name>`                                   | returns the stored result, body not executed    | executes and commits   |
| `ctx.run`                         | name, or `run`                                | returns the stored result, closure not executed | executes and appends   |
| call                              | name, or `Service/handler`                    | awaits the result of the call already issued    | appends a new call     |
| one-way call                      | name, or `Service/handler`                    | not sent again                                  | appends                |
| sleep                             | name, or `sleep`                              | awaits the timer already set                    | appends                |
| state read                        | (none)                                        | served from the eager state, **not journaled**  | lazy read if partial   |
| output                            | (none)                                        | `sys_restore` ends the invocation right away    | written at the end     |

A transaction (`sys_step_begin` / `sys_step_commit` / `sys_step_take_result`) is the commit record of the actor
mode with a result instead of the invocation output. Its `Run` command, the proposal and the apply commands are
written together, so that whatever prefix of the stream the runtime keeps, the apply commands are never durable
without their commit, and only the last transaction can be partially applied. `sys_restore` checks it and writes
the missing commands before handing over to the handler. A transaction without its result in the journal (the
proposal was lost) is just executed again, with a new `Run` command.

The new journal entries get completion ids after the largest one found in the journal. The runtime doesn't
check that the SDK consumed the replayed commands: it appends the new ones after them.

```ts
order: handlers.object.exclusive({ journal: "storage" }, async (ctx: ObjectContext, order: Order) => {
  // Non-deterministic code is fine: it runs again, for real, on every attempt
  ctx.console.info(`Processing ${order.id} at ${new Date().toISOString()}`);

  const left = await ctx.transaction("reserve", (tx) => {          // commit point 1
    const stock = tx.kv.get<number>(`stock/${order.sku}`) ?? 0;
    if (stock < order.quantity) throw new TerminalError("Out of stock");
    tx.kv.set(`stock/${order.sku}`, stock - order.quantity);
    return stock - order.quantity;
  });
  const label = await ctx.serviceClient(shipping).label(order.id, rpc.opts({ name: "label" }));
  await ctx.transaction("ship", (tx) => {                          // commit point 2
    tx.kv.set(`order/${order.id}`, `shipped with ${label}`);
    tx.serviceSendClient(notifications).shipped(order.id);
  });
  return { label, left };
})
```

The journal of that invocation:

```
Input, Run tx:reserve + result, SetState, SetState, Call label + results, Run tx:ship + result, SetState, OneWayCall, Output
```

### Semantics

* Each transaction is atomic and executes at most once; the invocation as a whole is not atomic. A later
  terminal failure doesn't undo earlier transactions: compensate explicitly if needed.
* Code outside transactions and runs executes at least once per attempt, and in request/response mode every
  resume after a suspension is a new attempt. That's CPU, not journal writes.
* Data must cross a commit point through the transaction result, as with `ctx.run`.
  Reads outside transactions see the latest committed state.
* The key is the only identity of an operation. A stored result is returned even if the arguments changed,
  as with idempotency keys. Operations whose relative order can change between attempts must be named, otherwise
  `#n` keys can be swapped (e.g. two unnamed calls to the same handler with different arguments).
* Awakeables aren't supported, since their ids are positional: use named signals.
* Transactions execute one at a time. A retryable error in a transaction body fails the attempt.
* `ctx.set` outside transactions is journaled right away, and executed again by every attempt.
* The partial-apply recovery relies on the same runtime ordering as the pipelined commit.

### What the prototype shows

Against `restate-server` 1.7.13:

* A handler running three transactions in a random order, and failing its first two attempts after them:
  in storage mode the third attempt ran them in a different order, each body executed exactly once, and
  the side effect `ctx.run` once. The same logic in replay mode failed attempts 2 and 3 with journal
  mismatches (`name: c != a`), and completed only when the random order happened to match the journal.
* Killing the process between two transactions: the next attempt restored the journal, didn't execute the
  first transaction again, issued the call and committed the second transaction.
* Request/response mode, where each await suspends: 5 attempts, each running the handler from the top with different
  random values, each transaction body executed once, the call and the named sleep issued once.
* State reads don't appear in the journal at all.

### Relation to actor handlers

An actor handler is a storage-mode handler with one implicit transaction whose result is the invocation output.
The two could be unified: the single-commit mode is the special case where the output is part of the record.

## Where this could go with runtime support

The prototype shows the semantics work on the current protocol. A runtime-native version could remove most of its costs:

1. **Atomic commit message.** A dedicated command, or batch, carrying state mutations, outbox, output and end,
   that the partition processor applies as a single log record. That's one append per invocation, no redo phase
   and no double writes. Pipelining becomes a protocol guarantee by construction.
2. **No journal.** Nothing needs replaying, so the runtime could skip storing the journal for these
   invocations, and keep only the output for idempotency and attach.
3. **Lazy, non-journaled reads.** A `ReadState` request/response control message, outside the journal, would remove
   the need to ship the whole state with every invocation.
4. **Optimistic concurrency.** The commit could carry the read set, or a state version, validated by the runtime,
   so that handlers on the same key could run concurrently and retry on conflict, instead of always taking the lock.
5. **Mailbox batching.** The runtime could deliver several queued messages for the same key to one attempt and
   commit them together, amortizing the round trip for hot keys.
6. **Continuations instead of awaits.** An actor that needs a reply could commit an outgoing call together with a
   "reply to handler X" continuation, keeping the single-commit model without journaled awaits.
