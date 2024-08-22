use crate::EntryRetryInfo;
use std::cmp;
use std::time::Duration;

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
        /// Interval between retries.
        interval: Duration,

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
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum NextRetry {
    Retry(Option<Duration>),
    DoNotRetry,
}

impl RetryPolicy {
    pub fn fixed_delay(
        interval: Duration,
        max_attempts: Option<u32>,
        max_duration: Option<Duration>,
    ) -> Self {
        Self::FixedDelay {
            interval,
            max_attempts,
            max_duration,
        }
    }

    pub fn exponential(
        initial_interval: Duration,
        factor: f32,
        max_attempts: Option<u32>,
        max_interval: Option<Duration>,
        max_duration: Option<Duration>,
    ) -> Self {
        Self::Exponential {
            initial_interval,
            factor,
            max_attempts,
            max_interval,
            max_duration,
        }
    }

    pub(crate) fn next_retry(&self, retry_info: EntryRetryInfo) -> NextRetry {
        match self {
            RetryPolicy::Infinite => NextRetry::Retry(None),
            RetryPolicy::None => NextRetry::DoNotRetry,
            RetryPolicy::FixedDelay {
                interval,
                max_attempts,
                max_duration,
            } => {
                if max_attempts.is_some_and(|max_attempts| max_attempts <= retry_info.retry_count)
                    || max_duration
                        .is_some_and(|max_duration| max_duration <= retry_info.retry_loop_duration)
                {
                    // Reached either max_attempts or max_duration bound
                    return NextRetry::DoNotRetry;
                }

                // No bound reached, we need to retry
                NextRetry::Retry(Some(*interval))
            }
            RetryPolicy::Exponential {
                initial_interval,
                factor,
                max_interval,
                max_attempts,
                max_duration,
            } => {
                if max_attempts.is_some_and(|max_attempts| max_attempts <= retry_info.retry_count)
                    || max_duration
                        .is_some_and(|max_duration| max_duration <= retry_info.retry_loop_duration)
                {
                    // Reached either max_attempts or max_duration bound
                    return NextRetry::DoNotRetry;
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
            NextRetry::DoNotRetry
        );
    }
}
