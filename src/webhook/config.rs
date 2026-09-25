use super::{EventKind, WebhookTarget};
use serde_json::Value;
use std::time::{Duration, Instant};

pub const DEFAULT_WEBHOOK_MAX_IN_FLIGHT: usize = 4;
pub const DEFAULT_WEBHOOK_MAX_PENDING_DELIVERIES: usize = 256;
pub const DEFAULT_WEBHOOK_MAX_PENDING_BYTES: usize = 1_048_576;
pub const DEFAULT_WEBHOOK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_WEBHOOK_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_WEBHOOK_MAX_AGE: Duration = Duration::from_secs(30);
pub const DEFAULT_WEBHOOK_MAX_ATTEMPTS: u8 = 1;
pub const DEFAULT_WEBHOOK_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
pub const DEFAULT_WEBHOOK_MAX_BACKOFF: Duration = Duration::from_secs(5);

pub const MAX_WEBHOOK_RULES: usize = 64;
pub const MAX_WEBHOOK_IN_FLIGHT: usize = 128;
pub const MAX_WEBHOOK_PENDING_DELIVERIES: usize = 65_536;
pub const MAX_WEBHOOK_PENDING_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_WEBHOOK_ATTEMPTS: u8 = 5;

/// Generation-wide bounded delivery queue and its compiled rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookConfig {
    pub max_in_flight: usize,
    pub max_pending_deliveries: usize,
    pub max_pending_bytes: usize,
    /// Zero disables shutdown draining; positive values bound the drain.
    pub shutdown_timeout: Duration,
    pub delivery_defaults: DeliveryPolicy,
    pub rules: Vec<WebhookRule>,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            max_in_flight: DEFAULT_WEBHOOK_MAX_IN_FLIGHT,
            max_pending_deliveries: DEFAULT_WEBHOOK_MAX_PENDING_DELIVERIES,
            max_pending_bytes: DEFAULT_WEBHOOK_MAX_PENDING_BYTES,
            shutdown_timeout: DEFAULT_WEBHOOK_SHUTDOWN_TIMEOUT,
            delivery_defaults: DeliveryPolicy::default(),
            rules: Vec::new(),
        }
    }
}

impl WebhookConfig {
    pub fn is_enabled(&self) -> bool {
        !self.rules.is_empty()
    }

    pub fn validate(&self) -> Result<(), WebhookValidationError> {
        if self.max_in_flight == 0 || self.max_in_flight > MAX_WEBHOOK_IN_FLIGHT {
            return Err(WebhookValidationError::QueueLimit("max_in_flight"));
        }
        if self.max_pending_deliveries == 0
            || self.max_pending_deliveries > MAX_WEBHOOK_PENDING_DELIVERIES
        {
            return Err(WebhookValidationError::QueueLimit("max_pending_deliveries"));
        }
        if self.max_pending_bytes == 0 || self.max_pending_bytes > MAX_WEBHOOK_PENDING_BYTES {
            return Err(WebhookValidationError::QueueLimit("max_pending_bytes"));
        }
        if self.rules.len() > MAX_WEBHOOK_RULES {
            return Err(WebhookValidationError::TooManyRules(self.rules.len()));
        }
        if !instant_addition_is_safe(self.shutdown_timeout) {
            return Err(WebhookValidationError::DurationOverflow(
                "shutdown_timeout_s",
            ));
        }
        self.delivery_defaults.validate()?;
        let mut names = std::collections::HashSet::with_capacity(self.rules.len());
        for rule in &self.rules {
            if rule.name.is_empty()
                || rule.name.len() > 64
                || !rule.name.bytes().all(|byte| {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_".contains(&byte)
                })
            {
                return Err(WebhookValidationError::InvalidRuleName(rule.name.clone()));
            }
            if !names.insert(rule.name.as_str()) {
                return Err(WebhookValidationError::DuplicateRuleName(rule.name.clone()));
            }
            rule.delivery.validate()?;
            let includes_interval = rule
                .when
                .branches
                .iter()
                .any(|branch| branch.events.contains(&EventKind::PathInterval));
            match (includes_interval, rule.interval) {
                (true, None) => return Err(WebhookValidationError::IntervalRequired),
                (false, Some(_)) => return Err(WebhookValidationError::UnexpectedInterval),
                (true, Some(interval)) if interval.is_zero() => {
                    return Err(WebhookValidationError::IntervalZero);
                }
                (true, Some(interval)) if !instant_addition_is_safe(interval) => {
                    return Err(WebhookValidationError::DurationOverflow("interval_s"));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// Per-rule timeout, age ceiling, and optional retry budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryPolicy {
    pub timeout: Duration,
    pub max_age: Duration,
    pub max_attempts: u8,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for DeliveryPolicy {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_WEBHOOK_TIMEOUT,
            max_age: DEFAULT_WEBHOOK_MAX_AGE,
            max_attempts: DEFAULT_WEBHOOK_MAX_ATTEMPTS,
            initial_backoff: DEFAULT_WEBHOOK_INITIAL_BACKOFF,
            max_backoff: DEFAULT_WEBHOOK_MAX_BACKOFF,
        }
    }
}

impl DeliveryPolicy {
    pub fn validate(self) -> Result<(), WebhookValidationError> {
        if self.timeout.is_zero() || self.max_age.is_zero() {
            return Err(WebhookValidationError::DeliveryDurationZero);
        }
        if !(1..=MAX_WEBHOOK_ATTEMPTS).contains(&self.max_attempts) {
            return Err(WebhookValidationError::AttemptLimit(self.max_attempts));
        }
        if self.max_attempts > 1
            && (self.initial_backoff.is_zero()
                || self.max_backoff.is_zero()
                || self.initial_backoff > self.max_backoff)
        {
            return Err(WebhookValidationError::RetryBackoff);
        }
        for (field, duration) in [
            ("timeout_s", self.timeout),
            ("max_age_s", self.max_age),
            ("initial_backoff_s", self.initial_backoff),
            ("max_backoff_s", self.max_backoff),
        ] {
            if !instant_addition_is_safe(duration) {
                return Err(WebhookValidationError::DurationOverflow(field));
            }
        }
        Ok(())
    }
}

fn instant_addition_is_safe(duration: Duration) -> bool {
    Instant::now().checked_add(duration).is_some()
}

/// Common selectors and event-specific OR branches for one rule.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventMatcher {
    pub branches: Vec<EventMatcherBranch>,
    pub outbounds: Vec<String>,
    pub inbounds: Vec<String>,
    pub balancers: Vec<String>,
    pub paths: Vec<String>,
    pub transports: Vec<String>,
}

impl EventMatcher {
    pub fn matches(&self, kind: EventKind, envelope: &Value) -> bool {
        self.matches_sources(envelope)
            && self
                .branches
                .iter()
                .any(|branch| branch.matches(kind, envelope))
    }

    fn matches_sources(&self, envelope: &Value) -> bool {
        matches_any(
            &self.outbounds,
            string_at(envelope, &["path", "outbound"])
                .or_else(|| string_at(envelope, &["outbound", "name"]))
                .or_else(|| string_at(envelope, &["member", "outbound"])),
        ) && matches_any(&self.inbounds, string_at(envelope, &["inbound", "name"]))
            && matches_any(&self.balancers, string_at(envelope, &["balancer", "name"]))
            && matches_any(&self.paths, string_at(envelope, &["path", "name"]))
            && matches_any(
                &self.transports,
                string_at(envelope, &["carrier", "transport"]).or_else(|| {
                    string_array_first_match(envelope, &["path", "transports"], &self.transports)
                }),
            )
    }
}

impl WebhookRule {
    pub fn matches(&self, kind: EventKind, envelope: &Value) -> bool {
        self.when.matches(kind, envelope)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventMatcherBranch {
    pub events: Vec<EventKind>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub field: Vec<String>,
    pub trigger: Vec<String>,
    pub probe_state_at_start: Vec<String>,
    pub outcome: Vec<String>,
    pub initial: Option<bool>,
    pub changed: Vec<String>,
}

impl EventMatcherBranch {
    fn matches(&self, kind: EventKind, envelope: &Value) -> bool {
        self.events.contains(&kind)
            && matches_any(&self.from, string_at(envelope, &["change", "from"]))
            && matches_any(&self.to, string_at(envelope, &["change", "to"]))
            && matches_any(&self.field, string_at(envelope, &["change", "field"]))
            && matches_any(&self.trigger, string_at(envelope, &["probe", "trigger"]))
            && matches_any(
                &self.probe_state_at_start,
                string_at(envelope, &["probe", "state_at_start"]),
            )
            && matches_any(&self.outcome, string_at(envelope, &["probe", "outcome"]))
            && self.initial.is_none_or(|expected| {
                bool_at(envelope, &["event", "initial"]) == Some(expected)
                    || bool_at(envelope, &["initial"]) == Some(expected)
            })
            && self.changed.iter().all(|component| {
                string_array_contains(envelope, &["change", "components"], component)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookRule {
    pub name: String,
    pub when: EventMatcher,
    /// Required only for `path.interval`; that event is a timer subscription,
    /// not an immediate state snapshot.
    pub interval: Option<Duration>,
    pub target: WebhookTarget,
    pub delivery: DeliveryPolicy,
}

fn matches_any(expected: &[String], actual: Option<&str>) -> bool {
    expected.is_empty() || actual.is_some_and(|actual| expected.iter().any(|value| value == actual))
}

fn string_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for component in path {
        current = current.get(*component)?;
    }
    current.as_str()
}

fn bool_at(value: &Value, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for component in path {
        current = current.get(*component)?;
    }
    current.as_bool()
}

fn string_array_contains(value: &Value, path: &[&str], expected: &str) -> bool {
    let mut current = value;
    for component in path {
        let Some(next) = current.get(*component) else {
            return false;
        };
        current = next;
    }
    current
        .as_array()
        .is_some_and(|values| values.iter().any(|value| value.as_str() == Some(expected)))
}

fn string_array_first_match<'a>(
    value: &'a Value,
    path: &[&str],
    expected: &[String],
) -> Option<&'a str> {
    let mut current = value;
    for component in path {
        current = current.get(*component)?;
    }
    current
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .find(|actual| expected.iter().any(|value| value == actual))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookValidationError {
    QueueLimit(&'static str),
    TooManyRules(usize),
    InvalidRuleName(String),
    DuplicateRuleName(String),
    DeliveryDurationZero,
    DurationOverflow(&'static str),
    AttemptLimit(u8),
    RetryBackoff,
    IntervalRequired,
    UnexpectedInterval,
    IntervalZero,
}

impl std::fmt::Display for WebhookValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueLimit(name) => write!(formatter, "invalid webhook queue limit {name}"),
            Self::TooManyRules(actual) => {
                write!(
                    formatter,
                    "webhook rules exceed the supported limit ({actual})"
                )
            }
            Self::InvalidRuleName(name) => {
                write!(formatter, "invalid webhook rule name {name:?}")
            }
            Self::DuplicateRuleName(name) => {
                write!(formatter, "duplicate webhook rule name {name:?}")
            }
            Self::DeliveryDurationZero => {
                formatter.write_str("webhook timeout and max_age must be positive")
            }
            Self::DurationOverflow(field) => {
                write!(
                    formatter,
                    "webhook {field} exceeds the supported runtime range"
                )
            }
            Self::AttemptLimit(attempts) => write!(
                formatter,
                "webhook max_attempts must be between 1 and {}, got {attempts}",
                MAX_WEBHOOK_ATTEMPTS
            ),
            Self::RetryBackoff => formatter.write_str(
                "webhook retries require positive backoff with initial_backoff <= max_backoff",
            ),
            Self::IntervalRequired => {
                formatter.write_str("path.interval requires a positive interval_s")
            }
            Self::UnexpectedInterval => {
                formatter.write_str("interval_s is only valid with path.interval")
            }
            Self::IntervalZero => formatter.write_str("webhook interval_s must be positive"),
        }
    }
}

impl std::error::Error for WebhookValidationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_matcher_ors_branches_and_ands_common_sources() {
        let matcher = EventMatcher {
            branches: vec![
                EventMatcherBranch {
                    events: vec![EventKind::PathStateChanged],
                    from: vec!["up".into()],
                    to: vec!["down".into()],
                    ..EventMatcherBranch::default()
                },
                EventMatcherBranch {
                    events: vec![EventKind::PathProbeCompleted],
                    probe_state_at_start: vec!["down".into()],
                    ..EventMatcherBranch::default()
                },
            ],
            outbounds: vec!["edge".into()],
            paths: vec!["wan".into()],
            ..EventMatcher::default()
        };
        let down = json!({
            "path": {"outbound":"edge", "name":"wan"},
            "change": {"from":"up", "to":"down"}
        });
        assert!(matcher.matches(EventKind::PathStateChanged, &down));
        assert!(!matcher.matches(EventKind::PathProbeCompleted, &down));
        let probe = json!({
            "path": {"outbound":"edge", "name":"wan"},
            "probe": {"state_at_start":"down", "outcome":"success"}
        });
        assert!(matcher.matches(EventKind::PathProbeCompleted, &probe));
        let other_path = json!({
            "path": {"outbound":"edge", "name":"lan"},
            "probe": {"state_at_start":"down"}
        });
        assert!(!matcher.matches(EventKind::PathProbeCompleted, &other_path));
    }

    #[test]
    fn delivery_defaults_disable_retries_and_validation_bounds_explicit_retries() {
        assert_eq!(DeliveryPolicy::default().max_attempts, 1);
        assert!(DeliveryPolicy::default().validate().is_ok());
        let bad = DeliveryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(6),
            max_backoff: Duration::from_secs(5),
            ..DeliveryPolicy::default()
        };
        assert_eq!(bad.validate(), Err(WebhookValidationError::RetryBackoff));
    }

    #[test]
    fn queue_defaults_are_bounded_and_empty_configuration_stays_disabled() {
        let config = WebhookConfig::default();
        assert!(!config.is_enabled());
        assert_eq!(config.max_in_flight, 4);
        assert_eq!(config.max_pending_deliveries, 256);
        assert_eq!(config.max_pending_bytes, 1_048_576);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(2));
        assert!(config.validate().is_ok());
    }
}
