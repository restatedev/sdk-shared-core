# Fix Duration-overflow panic in exponential retry-interval computation

## Bug Fix

### What Changed

`RetryPolicy::Exponential::next_retry` computed the next retry interval as the
**unclamped** `initial_interval * factor^(retry_count - 1)` and only clamped the
result to `max_interval` *afterwards*. The multiplication used
`Duration::mul_f32`, which panics (`cannot convert float seconds to Duration:
value is either too big or NaN`) once the intermediate value overflows
`Duration`'s representable range or is non-finite. Because the panic happened
*before* the clamp, setting `max_interval` did not help.

With a deliberately-infinite exponential policy (no `max_attempts`, no
`max_duration`), `initial_interval = 1s`, `factor = 2`, the interval for
`retry_count = 65` is `1s * 2^64`, which exceeds `Duration`'s range and panicked
the WASM core mid-retry.

The interval is now computed so it can **never panic**:

- The clamp against the ceiling (`max_interval`, or `Duration::MAX` when
  unbounded) is applied *before* the fallible float→`Duration` conversion.
- `Duration::try_from_secs_f32` is used instead of the panicking `mul_f32`; any
  `Err` (overflow, negative, or NaN — e.g. a non-finite `factor`, or `powi`
  overflowing to `inf`) falls back to the ceiling.
- For every in-range input the produced delays are **bit-for-bit identical** to
  the previous behavior — this is purely a saturation fix at the overflow
  boundary.

Two related unclamped-arithmetic hazards found while auditing were also fixed:

- `ErrorMessage.next_retry_delay` was `delay.as_millis() as u64`, a truncating
  cast that **wraps** for delays whose millisecond count exceeds `u64::MAX`
  (e.g. `2^62s`/`2^63s` truncate to `0`, which would tell the runtime to retry
  immediately — a tight loop). It now saturates to `u64::MAX`.
- The retry accumulators (`retry_count += 1`, `retry_loop_duration +=
  attempt_duration`) and a `sys_sleep` debug-log `Duration` subtraction now use
  saturating arithmetic, since their operands originate from the runtime/SDK
  boundary.

### Impact on Downstream SDKs

- Affects all SDKs that use this crate's retry-policy machinery (TypeScript,
  Python, Rust). The panic surfaced downstream through
  `propose_run_completion_failure_transient`.
- **No SDK code changes are required** — the public API and all in-range
  behavior are unchanged. SDKs pick up the fix on the next released version
  bump of this crate.

### Migration Guidance

None. Upgrade to the released version that includes this fix.

### Related Issues

- Fixes the retry-storm `Duration`-overflow panic reported from production.
