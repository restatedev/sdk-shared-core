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

                let max_interval = max_interval.unwrap_or(Duration::MAX);

                // Next interval in the backoff sequence:
                // initial_interval * factor^(retry_count - 1)
                // Uses saturating and try to avoid overflows.
                let exponent =
                    i32::try_from(retry_info.retry_count.saturating_sub(1)).unwrap_or(i32::MAX);
                let Ok(next_interval) = Duration::try_from_secs_f32(
                    initial_interval.as_secs_f32() * factor.powi(exponent),
                ) else {
                    // Overflow, return max_interval instead.
                    return NextRetry::Retry(Some(max_interval));
                };

                NextRetry::Retry(Some(cmp::min(max_interval, next_interval)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::prelude::*;
    use rstest::rstest;

    // No max_attempts / max_duration / max_interval => always growing retries.
    #[test]
    fn exponential_policy_does_not_panic_on_overflow() {
        let policy = RetryPolicy::Exponential {
            initial_interval: Duration::from_secs(1),
            factor: 2.0,
            max_interval: None,
            max_attempts: None,
            max_duration: None,
            on_max_attempts: OnMaxAttempts::FailAsTerminal,
        };

        // Iterate well past the overflow boundary (~retry_count 65): every retry must
        // stay within Duration::MAX and never panic.
        for retry_count in 1..=200 {
            assert_that!(
                policy.next_retry(EntryRetryInfo {
                    retry_count,
                    retry_loop_duration: Duration::ZERO,
                }),
                pat!(NextRetry::Retry(some(le(Duration::MAX)))),
                "retry_count={retry_count}"
            );
        }
    }

    #[rstest]
    // factor^0 == 1: the first retry uses the initial interval.
    #[case::first_retry_uses_initial(Duration::from_secs(1), 2.0, None, 1, Duration::from_secs(1))]
    #[case::in_range_grows_by_factor(Duration::from_secs(1), 2.0, None, 3, Duration::from_secs(4))]
    // Unbounded saturates to Duration::MAX rather than panicking, at and past the boundary.
    #[case::overflow_boundary_unbounded(Duration::from_secs(1), 2.0, None, 70, Duration::MAX)]
    #[case::large_retry_count_unbounded(Duration::from_secs(1), 2.0, None, 128, Duration::MAX)]
    #[case::max_retry_count_unbounded(Duration::from_secs(1), 2.0, None, u32::MAX, Duration::MAX)]
    // Bounded saturates to max_interval, at and past the boundary.
    #[case::overflow_boundary_bounded(
        Duration::from_secs(1),
        2.0,
        Some(Duration::from_secs(30)),
        70,
        Duration::from_secs(30)
    )]
    #[case::large_retry_count_bounded(
        Duration::from_secs(1),
        2.0,
        Some(Duration::from_secs(30)),
        128,
        Duration::from_secs(30)
    )]
    #[case::max_retry_count_bounded(
        Duration::from_secs(1),
        2.0,
        Some(Duration::from_secs(30)),
        u32::MAX,
        Duration::from_secs(30)
    )]
    // factor == 1 never grows.
    #[case::factor_one_never_grows(Duration::from_secs(2), 1.0, None, 1000, Duration::from_secs(2))]
    // Extreme / non-finite factors saturate to the ceiling.
    #[case::huge_factor_bounded(
        Duration::from_secs(1),
        1e30,
        Some(Duration::from_secs(30)),
        5,
        Duration::from_secs(30)
    )]
    #[case::huge_factor_unbounded(Duration::from_secs(1), 1e30, None, 5, Duration::MAX)]
    #[case::nan_factor_bounded(
        Duration::from_secs(1),
        f32::NAN,
        Some(Duration::from_secs(30)),
        5,
        Duration::from_secs(30)
    )]
    #[case::infinite_factor_unbounded(
        Duration::from_secs(1),
        f32::INFINITY,
        None,
        5,
        Duration::MAX
    )]
    fn exponential_policy_saturation(
        #[case] initial_interval: Duration,
        #[case] factor: f32,
        #[case] max_interval: Option<Duration>,
        #[case] retry_count: u32,
        #[case] expected: Duration,
    ) {
        let policy = RetryPolicy::Exponential {
            initial_interval,
            factor,
            max_interval,
            max_attempts: None,
            max_duration: None,
            on_max_attempts: OnMaxAttempts::FailAsTerminal,
        };

        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count,
                retry_loop_duration: Duration::ZERO,
            }),
            NextRetry::Retry(Some(expected))
        );
    }

    #[test]
    fn test_exponential_policy() {
        // Intervals are computed in f32, so use f32-exact powers of two
        // (125ms * 2^n) to compare exactly rather than depending on rounding.
        let policy = RetryPolicy::Exponential {
            initial_interval: Duration::from_millis(125),
            factor: 2.0,
            max_interval: Some(Duration::from_millis(750)),
            max_attempts: None,
            max_duration: Some(Duration::from_secs(10)),
            on_max_attempts: OnMaxAttempts::FailAsTerminal,
        };

        // 125ms * 2^1
        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count: 2,
                retry_loop_duration: Duration::from_secs(1)
            }),
            NextRetry::Retry(Some(Duration::from_millis(250)))
        );
        // 125ms * 2^2, still below max_interval
        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count: 3,
                retry_loop_duration: Duration::from_secs(1)
            }),
            NextRetry::Retry(Some(Duration::from_millis(500)))
        );
        // 125ms * 2^3 == 1s, clamped to max_interval
        assert_eq!(
            policy.next_retry(EntryRetryInfo {
                retry_count: 4,
                retry_loop_duration: Duration::from_secs(1)
            }),
            NextRetry::Retry(Some(Duration::from_millis(750)))
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
