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

                NextRetry::Retry(Some(cmp::min(
                    max_interval.unwrap_or(Duration::MAX),
                    initial_interval.mul_f32(factor.powi((retry_info.retry_count - 1) as i32)),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
