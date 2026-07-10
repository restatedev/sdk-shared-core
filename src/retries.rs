use crate::EntryRetryInfo;
use std::cmp;
use std::time::Duration;

/// What to do when a `RetryPolicy` runs out of attempts or duration.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum OnMaxAttempts {
    /// Convert the retryable failure into a terminal failure on the run handle.
    #[default]
    FailAsTerminal,
    /// Pause the invocation instead of failing it. The invocation MUST be manually resumed by the user.
    /// Requires service protocol V7 or newer.
    Pause,
}

/// This struct represents the policy to execute retries.
#[derive(Debug, Clone, Default)]
pub enum RetryPolicy {
    /// # Infinite
    ///
    /// Infinite retry strategy.
    #[default]
    Infinite,
    /// # None
    ///
    /// No retry strategy, fail on first failure.
    None,
    /// # Fixed delay
    ///
    /// Retry with a fixed delay strategy.
    FixedDelay {
        /// # Interval
        ///
        /// Interval between retries. If none, the runtime will provide one based on the invoker retry policy.
        interval: Option<Duration>,

        /// # Max attempts
        ///
        /// Gives up retrying when either this number of attempts is reached,
        /// or `max_duration` (if set) is reached first.
        /// Infinite retries if this field and `max_duration` are unset.
        max_attempts: Option<u32>,

        /// # Max duration
        ///
        /// Gives up retrying when either the retry loop lasted for this given max duration,
        /// or `max_attempts` (if set) is reached first.
        /// Infinite retries if this field and `max_attempts` are unset.
        max_duration: Option<Duration>,

        /// # On max attempts
        ///
        /// What to do once `max_attempts` or `max_duration` is reached.
        on_max_attempts: OnMaxAttempts,
    },
    /// # Exponential
    ///
    /// Retry with an exponential strategy. The next retry is computed as `min(last_retry_interval * factor, max_interval)`.
    Exponential {
        /// # Initial Interval
        ///
        /// Initial interval for the first retry attempt.
        initial_interval: Duration,

        /// # Factor
        ///
        /// The factor to use to compute the next retry attempt. This value should be higher than 1.0
        factor: f32,

        /// # Max interval
        ///
        /// Maximum interval between retries.
        max_interval: Option<Duration>,

        /// # Max attempts
        ///
        /// Gives up retrying when either this number of attempts is reached,
        /// or `max_duration` (if set) is reached first.
        /// Infinite retries if this field and `max_duration` are unset.
        max_attempts: Option<u32>,

        /// # Max duration
        ///
        /// Gives up retrying when either the retry loop lasted for this given max duration,
        /// or `max_attempts` (if set) is reached first.
        /// Infinite retries if this field and `max_attempts` are unset.
        max_duration: Option<Duration>,

        /// # On max attempts
        ///
        /// What to do once `max_attempts` or `max_duration` is reached.
        on_max_attempts: OnMaxAttempts,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum NextRetry {
    Retry(Option<Duration>),
    FailAsTerminal,
    Pause,
}

impl RetryPolicy {
    pub fn fixed_delay(
        interval: Option<Duration>,
        max_attempts: Option<u32>,
        max_duration: Option<Duration>,
        on_max_attempts: OnMaxAttempts,
    ) -> Self {
        Self::FixedDelay {
            interval,
            max_attempts,
            max_duration,
            on_max_attempts,
        }
    }

    pub fn exponential(
        initial_interval: Duration,
        factor: f32,
        max_attempts: Option<u32>,
        max_interval: Option<Duration>,
        max_duration: Option<Duration>,
        on_max_attempts: OnMaxAttempts,
    ) -> Self {
        Self::Exponential {
            initial_interval,
            factor,
            max_attempts,
            max_interval,
            max_duration,
            on_max_attempts,
        }
    }

    pub(crate) fn should_pause_on_max_attempts(&self) -> bool {
        matches!(
            self,
            RetryPolicy::FixedDelay {
                on_max_attempts: OnMaxAttempts::Pause,
                ..
            } | RetryPolicy::Exponential {
                on_max_attempts: OnMaxAttempts::Pause,
                ..
            }
        )
    }

    pub(crate) fn next_retry(&self, retry_info: EntryRetryInfo) -> NextRetry {
        match self {
            RetryPolicy::Infinite => NextRetry::Retry(None),
            RetryPolicy::None => NextRetry::FailAsTerminal,
            RetryPolicy::FixedDelay {
                interval,
                max_attempts,
                max_duration,
                on_max_attempts,
            } => {
                if max_attempts.is_some_and(|max_attempts| max_attempts <= retry_info.retry_count)
                    || max_duration
                        .is_some_and(|max_duration| max_duration <= retry_info.retry_loop_duration)
                {
                    // Reached either max_attempts or max_duration bound
                    return match on_max_attempts {
                        OnMaxAttempts::FailAsTerminal => NextRetry::FailAsTerminal,
                        OnMaxAttempts::Pause => NextRetry::Pause,
                    };
                }

                // No bound reached, we need to retry
                NextRetry::Retry(*interval)
            }
            RetryPolicy::Exponential {
                initial_interval,
                factor,
                max_interval,
                max_attempts,
                max_duration,
                on_max_attempts,
            } => {
                if max_attempts.is_some_and(|max_attempts| max_attempts <= retry_info.retry_count)
                    || max_duration
                        .is_some_and(|max_duration| max_duration <= retry_info.retry_loop_duration)
                {
                    // Reached either max_attempts or max_duration bound
                    return match on_max_attempts {
                        OnMaxAttempts::FailAsTerminal => NextRetry::FailAsTerminal,
                        OnMaxAttempts::Pause => NextRetry::Pause,
                    };
                }

                // Ceiling we saturate to: the configured `max_interval`, or the
                // largest representable `Duration` when the policy is unbounded.
                let ceiling = max_interval.unwrap_or(Duration::MAX);

                // Compute `initial_interval * factor^(retry_count - 1)` WITHOUT
                // panicking. The historical computation was
                // `initial_interval.mul_f32(factor.powi(..))`; `mul_f32` panics
                // ("cannot convert float seconds to Duration: value is either
                // too big or NaN") once the scaled value overflows `Duration`'s
                // range or is non-finite (e.g. `factor` is inf/NaN, or `powi`
                // overflowed to `inf`), and it did so *before* the `min` clamp
                // could apply.
                //
                // `Duration::try_from_secs_f64` is the non-panicking counterpart:
                // it returns `Err` (instead of panicking) on overflow, negative,
                // or NaN, so any failure means we've exceeded the representable
                // range and we fall back to the `ceiling`. `mul_f32` scales
                // through `f64` internally, so `try_from_secs_f64(f64::from(m) *
                // initial_interval.as_secs_f64())` reproduces its result
                // bit-for-bit for every in-range input -- this is purely a
                // saturation fix at the overflow boundary, delays are unchanged
                // otherwise.
                //
                // `retry_count` is >= 1 on the production path (it is incremented
                // before `next_retry` is called); `saturating_sub`/`try_from`
                // keep the exponent well-defined for degenerate inputs -- a huge
                // exponent simply overflows `powi` to `inf`, which then clamps to
                // `ceiling`.
                let exponent =
                    i32::try_from(retry_info.retry_count.saturating_sub(1)).unwrap_or(i32::MAX);
                let next_interval = Duration::try_from_secs_f64(
                    f64::from(factor.powi(exponent)) * initial_interval.as_secs_f64(),
                )
                .unwrap_or(ceiling);

                NextRetry::Retry(Some(cmp::min(ceiling, next_interval)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exponential(
        initial_interval: Duration,
        factor: f32,
        max_interval: Option<Duration>,
    ) -> RetryPolicy {
        // No max_attempts / max_duration => deliberately-infinite retries,
        // reproducing the production configuration that hit the overflow panic.
        RetryPolicy::Exponential {
            initial_interval,
            factor,
            max_interval,
            max_attempts: None,
            max_duration: None,
            on_max_attempts: OnMaxAttempts::FailAsTerminal,
        }
    }

    fn next_interval(policy: &RetryPolicy, retry_count: u32) -> Duration {
        match policy.next_retry(EntryRetryInfo {
            retry_count,
            retry_loop_duration: Duration::ZERO,
        }) {
            NextRetry::Retry(Some(d)) => d,
            other => panic!("expected NextRetry::Retry(Some(_)), got {other:?}"),
        }
    }

    // Regression test for the production retry-storm panic:
    //
    //   panicked at library/core/src/time.rs:...:
    //   cannot convert float seconds to Duration: value is either too big or NaN
    //
    // An exponential policy with no max_attempts and no max_duration
    // (deliberately-infinite retries), initial_interval = 1s, factor = 2.
    // Around retry_count = 65, `1s * 2^64` exceeds Duration's range. The old
    // code computed the unclamped `initial_interval.mul_f32(..)` *before* the
    // `max_interval` clamp, so `mul_f32` panicked and the clamp never applied.
    // Setting `max_interval` did NOT help. Computing the interval must never
    // panic, regardless of retry_count.
    #[test]
    fn exponential_policy_does_not_panic_on_overflow() {
        for max_interval in [None, Some(Duration::from_secs(30))] {
            let policy = exponential(Duration::from_secs(1), 2.0, max_interval);
            // Iterate well past the overflow boundary (~retry_count 65).
            for retry_count in 1..=200 {
                let d = next_interval(&policy, retry_count);
                if let Some(max_interval) = max_interval {
                    assert!(
                        d <= max_interval,
                        "retry_count={retry_count} produced {d:?} > max_interval {max_interval:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn exponential_policy_saturates_at_overflow_boundary() {
        // Unbounded: saturates to Duration::MAX rather than panicking.
        let unbounded = exponential(Duration::from_secs(1), 2.0, None);
        assert_eq!(next_interval(&unbounded, 70), Duration::MAX);
        assert_eq!(next_interval(&unbounded, u32::MAX), Duration::MAX);

        // Bounded: saturates to max_interval.
        let bounded = exponential(Duration::from_secs(1), 2.0, Some(Duration::from_secs(30)));
        assert_eq!(next_interval(&bounded, 70), Duration::from_secs(30));
        assert_eq!(next_interval(&bounded, u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn exponential_policy_saturation_table() {
        struct Case {
            name: &'static str,
            initial_interval: Duration,
            factor: f32,
            max_interval: Option<Duration>,
            retry_count: u32,
            expected: Duration,
        }

        let cases = [
            Case {
                name: "first retry uses initial interval",
                initial_interval: Duration::from_secs(1),
                factor: 2.0,
                max_interval: None,
                retry_count: 1,
                // factor^0 == 1
                expected: Duration::from_secs(1),
            },
            Case {
                name: "in-range value stays bit-for-bit identical to mul_f32",
                initial_interval: Duration::from_millis(100),
                factor: 2.0,
                max_interval: None,
                retry_count: 3,
                // factor^2 == 4
                expected: Duration::from_millis(100).mul_f32(4.0),
            },
            Case {
                name: "large retry_count saturates to Duration::MAX when unbounded",
                initial_interval: Duration::from_secs(1),
                factor: 2.0,
                max_interval: None,
                retry_count: 128,
                expected: Duration::MAX,
            },
            Case {
                name: "large retry_count saturates to max_interval",
                initial_interval: Duration::from_secs(1),
                factor: 2.0,
                max_interval: Some(Duration::from_secs(30)),
                retry_count: 128,
                expected: Duration::from_secs(30),
            },
            Case {
                name: "factor == 1 never grows",
                initial_interval: Duration::from_secs(2),
                factor: 1.0,
                max_interval: None,
                retry_count: 1000,
                expected: Duration::from_secs(2),
            },
            Case {
                name: "very large factor saturates to max_interval",
                initial_interval: Duration::from_secs(1),
                factor: 1e30,
                max_interval: Some(Duration::from_secs(30)),
                retry_count: 5,
                expected: Duration::from_secs(30),
            },
            Case {
                name: "very large factor saturates to Duration::MAX when unbounded",
                initial_interval: Duration::from_secs(1),
                factor: 1e30,
                max_interval: None,
                retry_count: 5,
                expected: Duration::MAX,
            },
            Case {
                name: "NaN factor saturates to max_interval",
                initial_interval: Duration::from_secs(1),
                factor: f32::NAN,
                max_interval: Some(Duration::from_secs(30)),
                retry_count: 5,
                expected: Duration::from_secs(30),
            },
            Case {
                name: "infinite factor saturates to Duration::MAX when unbounded",
                initial_interval: Duration::from_secs(1),
                factor: f32::INFINITY,
                max_interval: None,
                retry_count: 5,
                expected: Duration::MAX,
            },
        ];

        for case in cases {
            let policy = exponential(case.initial_interval, case.factor, case.max_interval);
            assert_eq!(
                next_interval(&policy, case.retry_count),
                case.expected,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn test_exponential_policy() {
        let policy = RetryPolicy::Exponential {
            initial_interval: Duration::from_millis(100),
            factor: 2.0,
            max_interval: Some(Duration::from_millis(500)),
            max_attempts: None,
            max_duration: Some(Duration::from_secs(10)),
            on_max_attempts: OnMaxAttempts::FailAsTerminal,
        };

        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count: 2,
                retry_loop_duration: Duration::from_secs(1)
            }),
            NextRetry::Retry(Some(Duration::from_millis(100).mul_f32(2.0)))
        );
        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count: 3,
                retry_loop_duration: Duration::from_secs(1)
            }),
            NextRetry::Retry(Some(Duration::from_millis(100).mul_f32(4.0)))
        );
        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count: 4,
                retry_loop_duration: Duration::from_secs(1)
            }),
            NextRetry::Retry(Some(Duration::from_millis(500)))
        );
        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count: 4,
                retry_loop_duration: Duration::from_secs(10)
            }),
            NextRetry::FailAsTerminal
        );
    }
}
