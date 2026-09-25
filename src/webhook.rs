//! Pure, compiled webhook configuration and rendering types.
//!
//! This module deliberately contains no task, socket, timer, or queue owner.
//! Runtime code consumes immutable rules and snapshots produced by the tunnel.

mod config;
mod event;
mod template;

pub use config::{
    DEFAULT_WEBHOOK_INITIAL_BACKOFF, DEFAULT_WEBHOOK_MAX_AGE, DEFAULT_WEBHOOK_MAX_ATTEMPTS,
    DEFAULT_WEBHOOK_MAX_BACKOFF, DEFAULT_WEBHOOK_MAX_IN_FLIGHT, DEFAULT_WEBHOOK_MAX_PENDING_BYTES,
    DEFAULT_WEBHOOK_MAX_PENDING_DELIVERIES, DEFAULT_WEBHOOK_SHUTDOWN_TIMEOUT,
    DEFAULT_WEBHOOK_TIMEOUT, DeliveryPolicy, EventMatcher, EventMatcherBranch, WebhookConfig,
    WebhookRule, WebhookValidationError,
};
pub use event::{EventKind, EventKindParseError};
pub use template::{
    FormTemplate, HeaderValueTemplate, JsonTemplate, QueryTemplate, RenderedWebhookRequest,
    Template, TemplateError, WebhookBody, WebhookHeader, WebhookScheme, WebhookTarget, WebhookUrl,
};
