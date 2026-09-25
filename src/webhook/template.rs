use crate::config::EgressRef;
use crate::product::{DnsPlanId, TargetResolutionMode};
use http::{HeaderName, HeaderValue, Method, Uri};
use rustls::pki_types::CertificateDer;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

const MAX_RENDERED_URL_BYTES: usize = 8 * 1024;
const MAX_RENDERED_HEADERS_BYTES: usize = 16 * 1024;
const MAX_RENDERED_BODY_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookScheme {
    Http,
    Https,
}

impl WebhookScheme {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    pub const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }
}

/// Parsed static URL authority plus compiled dynamic request-target values.
#[derive(Clone, PartialEq, Eq)]
pub struct WebhookUrl {
    pub scheme: WebhookScheme,
    /// Canonical hostname or bracketless IP literal used for DNS, Host, and TLS.
    pub host: String,
    pub port: u16,
    /// Path template. Dynamic values are encoded as one path component.
    pub path: Template,
    /// Original, static query string from `url`, retained byte-for-byte.
    pub static_query: Option<String>,
    /// Additional encoded query key/value pairs declared by `query`.
    pub query: Vec<QueryTemplate>,
}

impl fmt::Debug for WebhookUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookUrl")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("path", &"<redacted>")
            .field(
                "static_query",
                &self.static_query.as_ref().map(|_| "<redacted>"),
            )
            .field("query", &self.query.len())
            .finish()
    }
}

impl WebhookUrl {
    /// Parse a configured absolute HTTP URL. Fragments and user-info are not
    /// accepted because they are never part of a webhook request target.
    pub fn parse(url: &str, query: Vec<QueryTemplate>) -> Result<Self, TemplateError> {
        let (scheme, remainder) = url.split_once("://").ok_or(TemplateError::InvalidUrl)?;
        let scheme = match scheme.to_ascii_lowercase().as_str() {
            "http" => WebhookScheme::Http,
            "https" => WebhookScheme::Https,
            _ => return Err(TemplateError::InvalidUrl),
        };
        if remainder.contains('#') {
            return Err(TemplateError::InvalidUrl);
        }
        let authority_end = remainder.find(['/', '?']).unwrap_or(remainder.len());
        let authority = &remainder[..authority_end];
        if authority.is_empty()
            || authority.contains('@')
            || authority.contains('{')
            || authority.contains('}')
        {
            return Err(TemplateError::InvalidUrl);
        }
        let parsed_authority =
            http::uri::Authority::from_str(authority).map_err(|_| TemplateError::InvalidUrl)?;
        let host = parsed_authority.host().trim_matches(['[', ']']);
        if host.is_empty() {
            return Err(TemplateError::InvalidUrl);
        }
        let host = match host.parse::<std::net::IpAddr>() {
            Ok(address) => address.to_string(),
            Err(_) => idna::domain_to_ascii(host)
                .map_err(|_| TemplateError::InvalidUrl)?
                .to_ascii_lowercase(),
        };
        let port = match (parsed_authority.port(), parsed_authority.port_u16()) {
            (Some(_), Some(port)) => port,
            (Some(_), None) => return Err(TemplateError::InvalidUrl),
            (None, _) => scheme.default_port(),
        };
        if port == 0 {
            return Err(TemplateError::InvalidUrl);
        }

        let suffix = &remainder[authority_end..];
        let (path_and_query, static_query) = match suffix.split_once('?') {
            Some((path, query)) => (path, Some(query.to_owned())),
            None => (suffix, None),
        };
        if static_query
            .as_ref()
            .is_some_and(|query| query.contains(['{', '}']))
        {
            // Static URL query text is preserved verbatim. Dynamic values have
            // one unambiguous home in the separately encoded `query` table.
            return Err(TemplateError::InvalidUrl);
        }
        let path = if path_and_query.is_empty() {
            "/"
        } else {
            path_and_query
        };
        if !path.starts_with('/') || path.contains('?') || path.contains('#') {
            return Err(TemplateError::InvalidUrl);
        }
        let path = Template::compile(path)?;
        if query.len() > 128 || query.iter().any(|pair| pair.name.len() > 512) {
            return Err(TemplateError::TooManyQueryValues);
        }
        Ok(Self {
            scheme,
            host,
            port,
            path,
            static_query,
            query,
        })
    }

    fn render_uri(&self, context: &Value) -> Result<Uri, TemplateError> {
        let path = self.path.render_path(context)?;
        let mut uri =
            String::with_capacity(MAX_RENDERED_URL_BYTES.min(path.len() + self.host.len() + 32));
        push_limited(&mut uri, self.scheme.as_str(), MAX_RENDERED_URL_BYTES)?;
        push_limited(&mut uri, "://", MAX_RENDERED_URL_BYTES)?;
        if self.host.contains(':') {
            push_limited(&mut uri, "[", MAX_RENDERED_URL_BYTES)?;
            push_limited(&mut uri, &self.host, MAX_RENDERED_URL_BYTES)?;
            push_limited(&mut uri, "]", MAX_RENDERED_URL_BYTES)?;
        } else {
            push_limited(&mut uri, &self.host, MAX_RENDERED_URL_BYTES)?;
        }
        if self.port != self.scheme.default_port() {
            push_limited(&mut uri, ":", MAX_RENDERED_URL_BYTES)?;
            push_limited(&mut uri, &self.port.to_string(), MAX_RENDERED_URL_BYTES)?;
        }
        push_limited(&mut uri, &path, MAX_RENDERED_URL_BYTES)?;
        let mut has_query = false;
        if let Some(query) = &self.static_query {
            push_limited(&mut uri, "?", MAX_RENDERED_URL_BYTES)?;
            push_limited(&mut uri, query, MAX_RENDERED_URL_BYTES)?;
            has_query = true;
        }
        for pair in &self.query {
            let separator = if has_query { '&' } else { '?' };
            push_limited(&mut uri, &separator.to_string(), MAX_RENDERED_URL_BYTES)?;
            has_query = true;
            push_limited(
                &mut uri,
                &encode_query_component(pair.name.as_bytes()),
                MAX_RENDERED_URL_BYTES,
            )?;
            push_limited(&mut uri, "=", MAX_RENDERED_URL_BYTES)?;
            let remaining = MAX_RENDERED_URL_BYTES.saturating_sub(uri.len());
            let value = pair.value.render_text_limited(context, remaining / 3)?;
            push_limited(
                &mut uri,
                &encode_query_component(value.as_bytes()),
                MAX_RENDERED_URL_BYTES,
            )?;
        }
        if uri.len() > MAX_RENDERED_URL_BYTES {
            return Err(TemplateError::RenderedUrlTooLarge);
        }
        uri.parse().map_err(|_| TemplateError::RenderedUrlInvalid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTemplate {
    pub name: String,
    pub value: Template,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Template {
    parts: Vec<TemplatePart>,
}

impl fmt::Debug for Template {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Template")
            .field("parts", &self.parts.len())
            .finish()
    }
}

impl Template {
    pub fn compile(input: &str) -> Result<Self, TemplateError> {
        if input.len() > MAX_RENDERED_BODY_BYTES {
            return Err(TemplateError::SourceTooLarge);
        }
        let mut parts = Vec::new();
        let mut literal = String::new();
        let mut chars = input.chars().peekable();
        while let Some(character) = chars.next() {
            match character {
                '{' if chars.peek() == Some(&'{') => {
                    chars.next();
                    literal.push('{');
                }
                '}' if chars.peek() == Some(&'}') => {
                    chars.next();
                    literal.push('}');
                }
                '{' => {
                    if !literal.is_empty() {
                        parts.push(TemplatePart::Literal(std::mem::take(&mut literal)));
                    }
                    let mut field = String::new();
                    let mut closed = false;
                    for next in chars.by_ref() {
                        if next == '}' {
                            closed = true;
                            break;
                        }
                        if next == '{' {
                            return Err(TemplateError::InvalidSyntax);
                        }
                        field.push(next);
                    }
                    if !closed || !valid_field_path(&field) {
                        return Err(TemplateError::InvalidSyntax);
                    }
                    parts.push(TemplatePart::Field(field));
                }
                '}' => return Err(TemplateError::InvalidSyntax),
                other => literal.push(other),
            }
        }
        if !literal.is_empty() {
            parts.push(TemplatePart::Literal(literal));
        }
        Ok(Self { parts })
    }

    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.parts.iter().filter_map(|part| match part {
            TemplatePart::Field(field) => Some(field.as_str()),
            TemplatePart::Literal(_) => None,
        })
    }

    fn material_bytes(&self) -> usize {
        self.parts.iter().fold(0usize, |total, part| {
            total.saturating_add(match part {
                TemplatePart::Literal(value) => value.len(),
                TemplatePart::Field(field) => field.len().saturating_add(2),
            })
        })
    }

    pub fn has_dynamic_fields(&self) -> bool {
        self.parts
            .iter()
            .any(|part| matches!(part, TemplatePart::Field(_)))
    }

    pub fn is_literal(&self) -> bool {
        !self.has_dynamic_fields()
    }

    pub fn render_text(&self, context: &Value) -> Result<String, TemplateError> {
        self.render_text_limited(context, MAX_RENDERED_BODY_BYTES)
    }

    fn render_text_limited(
        &self,
        context: &Value,
        maximum_bytes: usize,
    ) -> Result<String, TemplateError> {
        let mut output = String::new();
        for part in &self.parts {
            match part {
                TemplatePart::Literal(value) => push_limited(&mut output, value, maximum_bytes)?,
                TemplatePart::Field(field) => {
                    let value = field_value(context, field)
                        .ok_or_else(|| TemplateError::MissingField(field.clone()))?;
                    if value.is_null() {
                        return Err(TemplateError::MissingField(field.clone()));
                    }
                    push_limited(&mut output, &template_text(value), maximum_bytes)?;
                }
            }
        }
        Ok(output)
    }

    pub fn render_json(&self, context: &Value) -> Result<Value, TemplateError> {
        let mut budget = MAX_RENDERED_BODY_BYTES;
        self.render_json_limited(context, &mut budget)
    }

    fn render_json_limited(
        &self,
        context: &Value,
        budget: &mut usize,
    ) -> Result<Value, TemplateError> {
        if let [TemplatePart::Field(field)] = self.parts.as_slice() {
            let value = field_value(context, field)
                .ok_or_else(|| TemplateError::MissingField(field.clone()))?;
            reserve_json_bytes(value, budget)?;
            return Ok(value.clone());
        }
        let text_budget = (*budget).min(MAX_RENDERED_BODY_BYTES);
        let text = self.render_text_limited(context, text_budget)?;
        reserve_budget(budget, json_string_encoded_len(&text))?;
        Ok(Value::String(text))
    }

    fn render_path(&self, context: &Value) -> Result<String, TemplateError> {
        let mut output = String::new();
        for part in &self.parts {
            match part {
                TemplatePart::Literal(value) => {
                    push_limited(&mut output, value, MAX_RENDERED_URL_BYTES)?
                }
                TemplatePart::Field(field) => {
                    let value = field_value(context, field)
                        .ok_or_else(|| TemplateError::MissingField(field.clone()))?;
                    if value.is_null() {
                        return Err(TemplateError::MissingField(field.clone()));
                    }
                    let remaining = MAX_RENDERED_URL_BYTES.saturating_sub(output.len());
                    let text = template_text(value);
                    if text.len() > remaining / 3 {
                        return Err(TemplateError::RenderedUrlTooLarge);
                    }
                    push_limited(
                        &mut output,
                        &encode_path_component(text.as_bytes()),
                        MAX_RENDERED_URL_BYTES,
                    )?;
                }
            }
        }
        if !output.starts_with('/') || output.contains('#') || output.contains('?') {
            return Err(TemplateError::RenderedUrlInvalid);
        }
        Ok(output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TemplatePart {
    Literal(String),
    Field(String),
}

#[derive(Clone, PartialEq, Eq)]
pub enum HeaderValueTemplate {
    Template(Template),
    Secret(Arc<[u8]>),
}

impl fmt::Debug for HeaderValueTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Template(_) => formatter.write_str("Template(<redacted>)"),
            Self::Secret(_) => formatter.write_str("Secret(<redacted>)"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookHeader {
    pub name: HeaderName,
    pub value: HeaderValueTemplate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormTemplate {
    pub name: String,
    pub value: Template,
}

#[derive(Clone, PartialEq, Eq)]
pub enum JsonTemplate {
    Static(Value),
    String(Template),
    Array(Vec<JsonTemplate>),
    Object(BTreeMap<String, JsonTemplate>),
}

impl fmt::Debug for JsonTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JsonTemplate(<redacted>)")
    }
}

impl JsonTemplate {
    pub fn compile(value: Value) -> Result<Self, TemplateError> {
        if json_encoded_size(&value) > MAX_RENDERED_BODY_BYTES {
            return Err(TemplateError::SourceTooLarge);
        }
        match value {
            Value::String(value) => Ok(Self::String(Template::compile(&value)?)),
            Value::Array(values) => values
                .into_iter()
                .map(Self::compile)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            Value::Object(values) => values
                .into_iter()
                .map(|(name, value)| Ok((name, Self::compile(value)?)))
                .collect::<Result<BTreeMap<_, _>, TemplateError>>()
                .map(Self::Object),
            value => Ok(Self::Static(value)),
        }
    }

    pub fn render(&self, context: &Value) -> Result<Value, TemplateError> {
        let mut budget = MAX_RENDERED_BODY_BYTES;
        self.render_limited(context, &mut budget)
    }

    fn render_limited(&self, context: &Value, budget: &mut usize) -> Result<Value, TemplateError> {
        match self {
            Self::Static(value) => {
                reserve_json_bytes(value, budget)?;
                Ok(value.clone())
            }
            Self::String(template) => template.render_json_limited(context, budget),
            Self::Array(values) => {
                reserve_budget(budget, 2)?;
                let mut rendered = Vec::with_capacity(values.len());
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        reserve_budget(budget, 1)?;
                    }
                    rendered.push(value.render_limited(context, budget)?);
                }
                Ok(Value::Array(rendered))
            }
            Self::Object(values) => {
                reserve_budget(budget, 2)?;
                let mut rendered = Map::with_capacity(values.len());
                for (index, (name, value)) in values.iter().enumerate() {
                    if index != 0 {
                        reserve_budget(budget, 1)?;
                    }
                    let key_size = json_string_encoded_len(name).saturating_add(1);
                    reserve_budget(budget, key_size)?;
                    rendered.insert(name.clone(), value.render_limited(context, budget)?);
                }
                Ok(Value::Object(rendered))
            }
        }
    }

    fn fields<'a>(&'a self, output: &mut Vec<&'a str>) {
        match self {
            Self::Static(_) => {}
            Self::String(template) => output.extend(template.fields()),
            Self::Array(values) => {
                for value in values {
                    value.fields(output);
                }
            }
            Self::Object(values) => {
                for value in values.values() {
                    value.fields(output);
                }
            }
        }
    }

    fn material_bytes(&self) -> usize {
        match self {
            Self::Static(value) => json_encoded_size(value),
            Self::String(template) => template.material_bytes(),
            Self::Array(values) => values.iter().fold(2usize, |total, value| {
                total.saturating_add(value.material_bytes())
            }),
            Self::Object(values) => values.iter().fold(2usize, |total, (name, value)| {
                total
                    .saturating_add(json_string_encoded_len(name))
                    .saturating_add(value.material_bytes())
            }),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum WebhookBody {
    None,
    StandardEvent,
    Json(JsonTemplate),
    Form(Vec<FormTemplate>),
    Text(Template),
}

impl fmt::Debug for WebhookBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::None => "none",
            Self::StandardEvent => "standard-event",
            Self::Json(_) => "json",
            Self::Form(_) => "form",
            Self::Text(_) => "text",
        };
        formatter.debug_tuple("WebhookBody").field(&kind).finish()
    }
}

/// Static target plus templates compiled during config loading.
#[derive(Clone, PartialEq, Eq)]
pub struct WebhookTarget {
    pub url: WebhookUrl,
    pub method: Method,
    pub egress: EgressRef,
    pub dns_policy: Option<DnsPlanId>,
    pub target_resolution: TargetResolutionMode,
    pub headers: Vec<WebhookHeader>,
    pub body: WebhookBody,
    pub tls_roots: Vec<CertificateDer<'static>>,
}

impl fmt::Debug for WebhookTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookTarget")
            .field("url", &self.url)
            .field("method", &self.method)
            .field("egress", &self.egress)
            .field("dns_policy", &self.dns_policy)
            .field("target_resolution", &self.target_resolution)
            .field("headers", &self.headers.len())
            .field("body", &self.body)
            .field("tls_roots", &self.tls_roots.len())
            .finish()
    }
}

impl WebhookTarget {
    /// Count the non-secret compiled config material held for this target.
    /// This is checked once at configuration validation, not in the delivery
    /// path, and prevents many individually-small templates exhausting memory.
    pub fn compiled_material_bytes(&self) -> usize {
        let mut size = self.url.path.material_bytes();
        size = size.saturating_add(self.url.static_query.as_ref().map_or(0, String::len));
        for pair in &self.url.query {
            size = size
                .saturating_add(pair.name.len())
                .saturating_add(pair.value.material_bytes());
        }
        for header in &self.headers {
            size = size.saturating_add(header.name.as_str().len());
            size = size.saturating_add(match &header.value {
                HeaderValueTemplate::Template(template) => template.material_bytes(),
                HeaderValueTemplate::Secret(secret) => secret.len(),
            });
        }
        size = size.saturating_add(match &self.body {
            WebhookBody::None | WebhookBody::StandardEvent => 0,
            WebhookBody::Json(template) => template.material_bytes(),
            WebhookBody::Form(fields) => fields.iter().fold(0usize, |total, field| {
                total
                    .saturating_add(field.name.len())
                    .saturating_add(field.value.material_bytes())
            }),
            WebhookBody::Text(template) => template.material_bytes(),
        });
        self.tls_roots.iter().fold(size, |total, root| {
            total.saturating_add(root.as_ref().len())
        })
    }

    pub fn template_fields(&self) -> Vec<&str> {
        let mut fields = Vec::new();
        fields.extend(self.url.path.fields());
        for value in &self.url.query {
            fields.extend(value.value.fields());
        }
        for header in &self.headers {
            if let HeaderValueTemplate::Template(template) = &header.value {
                fields.extend(template.fields());
            }
        }
        match &self.body {
            WebhookBody::None | WebhookBody::StandardEvent => {}
            WebhookBody::Json(template) => template.fields(&mut fields),
            WebhookBody::Form(values) => {
                for value in values {
                    fields.extend(value.value.fields());
                }
            }
            WebhookBody::Text(template) => fields.extend(template.fields()),
        }
        fields
    }

    pub fn render(&self, envelope: &Value) -> Result<RenderedWebhookRequest, TemplateError> {
        let uri = self.url.render_uri(envelope)?;
        let mut headers = Vec::with_capacity(self.headers.len());
        let mut header_bytes = 0usize;
        for header in &self.headers {
            let value = match &header.value {
                HeaderValueTemplate::Template(template) => HeaderValue::from_str(
                    &template.render_text_limited(envelope, MAX_RENDERED_HEADERS_BYTES)?,
                )
                .map_err(|_| TemplateError::InvalidHeaderValue)?,
                HeaderValueTemplate::Secret(secret) => HeaderValue::from_bytes(secret)
                    .map_err(|_| TemplateError::InvalidHeaderValue)?,
            };
            header_bytes = header_bytes
                .saturating_add(header.name.as_str().len())
                .saturating_add(value.as_bytes().len());
            if header_bytes > MAX_RENDERED_HEADERS_BYTES {
                return Err(TemplateError::RenderedHeadersTooLarge);
            }
            headers.push((header.name.clone(), value));
        }

        let (body, content_type) = self.body.render(envelope)?;
        if body
            .as_ref()
            .is_some_and(|body| body.len() > MAX_RENDERED_BODY_BYTES)
        {
            return Err(TemplateError::RenderedBodyTooLarge);
        }
        Ok(RenderedWebhookRequest {
            uri,
            headers,
            body,
            content_type,
        })
    }
}

impl WebhookBody {
    fn render(
        &self,
        context: &Value,
    ) -> Result<(Option<Vec<u8>>, Option<&'static str>), TemplateError> {
        match self {
            Self::None => Ok((None, None)),
            Self::StandardEvent => {
                if json_encoded_size(context) > MAX_RENDERED_BODY_BYTES {
                    return Err(TemplateError::RenderedBodyTooLarge);
                }
                serde_json::to_vec(context)
                    .map(|body| (Some(body), Some("application/json")))
                    .map_err(|_| TemplateError::Serialization)
            }
            Self::Json(template) => {
                let mut budget = MAX_RENDERED_BODY_BYTES;
                let value = template.render_limited(context, &mut budget)?;
                serde_json::to_vec(&value)
                    .map(|body| (Some(body), Some("application/json")))
                    .map_err(|_| TemplateError::Serialization)
            }
            Self::Form(values) => {
                let mut body = String::new();
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        push_limited(&mut body, "&", MAX_RENDERED_BODY_BYTES)?;
                    }
                    append_form_component(&mut body, value.name.as_bytes())?;
                    push_limited(&mut body, "=", MAX_RENDERED_BODY_BYTES)?;
                    let remaining = MAX_RENDERED_BODY_BYTES.saturating_sub(body.len());
                    let rendered = value.value.render_text_limited(context, remaining / 3)?;
                    append_form_component(&mut body, rendered.as_bytes())?;
                }
                Ok((
                    Some(body.into_bytes()),
                    Some("application/x-www-form-urlencoded"),
                ))
            }
            Self::Text(template) => Ok((
                Some(
                    template
                        .render_text_limited(context, MAX_RENDERED_BODY_BYTES)?
                        .into_bytes(),
                ),
                Some("text/plain; charset=utf-8"),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedWebhookRequest {
    pub uri: Uri,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub body: Option<Vec<u8>>,
    pub content_type: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateError {
    InvalidSyntax,
    InvalidUrl,
    InvalidHeaderValue,
    TooManyQueryValues,
    MissingField(String),
    RenderedUrlInvalid,
    RenderedUrlTooLarge,
    RenderedHeadersTooLarge,
    RenderedBodyTooLarge,
    RenderedOutputTooLarge,
    SourceTooLarge,
    Serialization,
}

impl fmt::Display for TemplateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSyntax => formatter.write_str("webhook template syntax is invalid"),
            Self::InvalidUrl => formatter.write_str("webhook target URL is invalid"),
            Self::InvalidHeaderValue => {
                formatter.write_str("webhook rendered header value is invalid")
            }
            Self::TooManyQueryValues => {
                formatter.write_str("webhook target has too many dynamic query values")
            }
            Self::MissingField(field) => {
                write!(formatter, "webhook template field {field:?} is unavailable")
            }
            Self::RenderedUrlInvalid => {
                formatter.write_str("rendered webhook request URL is invalid")
            }
            Self::RenderedUrlTooLarge => {
                formatter.write_str("rendered webhook request URL exceeds 8 KiB")
            }
            Self::RenderedHeadersTooLarge => {
                formatter.write_str("rendered webhook headers exceed 16 KiB")
            }
            Self::RenderedBodyTooLarge => {
                formatter.write_str("rendered webhook body exceeds 64 KiB")
            }
            Self::RenderedOutputTooLarge => {
                formatter.write_str("rendered webhook value exceeds its size limit")
            }
            Self::SourceTooLarge => formatter.write_str("webhook template material exceeds 64 KiB"),
            Self::Serialization => formatter.write_str("webhook event could not be serialized"),
        }
    }
}

impl std::error::Error for TemplateError {}

static JSON_NULL: Value = Value::Null;

fn field_value<'a>(context: &'a Value, field: &str) -> Option<&'a Value> {
    let mut current = context;
    for component in field.split('.') {
        current = match current {
            Value::Object(object) => match object.get(component) {
                Some(value) => value,
                None if is_optional_field(field) => return Some(&JSON_NULL),
                None => return None,
            },
            // Known address objects are represented as JSON null when a
            // socket address was unavailable. Preserve that as a typed JSON
            // null for a whole-value template like `{carrier.local.ip}`.
            Value::Null if is_optional_address_leaf(field) => return Some(&JSON_NULL),
            _ => return None,
        };
    }
    Some(current)
}

fn is_optional_address_leaf(field: &str) -> bool {
    matches!(
        field,
        "carrier.local.ip"
            | "carrier.local.port"
            | "carrier.peer.ip"
            | "carrier.peer.port"
            | "change.before.ip"
            | "change.before.port"
            | "change.after.ip"
            | "change.after.port"
    )
}

fn is_optional_field(field: &str) -> bool {
    is_optional_address_leaf(field)
        || matches!(
            field,
            "event.initial"
                | "event.reason"
                | "path.local_ips"
                | "path.last_ready_at"
                | "path.metrics"
                | "carrier.listen_path"
                | "carrier.peer_usage"
                | "change.reason"
        )
}

fn template_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
        }
    }
}

fn push_limited(output: &mut String, value: &str, limit: usize) -> Result<(), TemplateError> {
    if value.len() > limit.saturating_sub(output.len()) {
        return Err(TemplateError::RenderedOutputTooLarge);
    }
    output.push_str(value);
    Ok(())
}

fn reserve_budget(budget: &mut usize, amount: usize) -> Result<(), TemplateError> {
    if amount > *budget {
        return Err(TemplateError::RenderedBodyTooLarge);
    }
    *budget -= amount;
    Ok(())
}

fn reserve_json_bytes(value: &Value, budget: &mut usize) -> Result<(), TemplateError> {
    reserve_budget(budget, json_encoded_size(value))
}

fn json_encoded_size(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Number(number) => number.to_string().len(),
        Value::String(value) => json_string_encoded_len(value),
        Value::Array(values) => 2usize
            .saturating_add(values.len().saturating_sub(1))
            .saturating_add(values.iter().map(json_encoded_size).sum::<usize>()),
        Value::Object(values) => {
            let entries = values
                .iter()
                .map(|(name, value)| {
                    json_string_encoded_len(name)
                        .saturating_add(1)
                        .saturating_add(json_encoded_size(value))
                })
                .sum::<usize>();
            2usize
                .saturating_add(values.len().saturating_sub(1))
                .saturating_add(entries)
        }
    }
}

fn json_string_encoded_len(value: &str) -> usize {
    value.chars().fold(2usize, |length, character| {
        length.saturating_add(match character {
            '"' | '\\' | '\u{0008}' | '\u{000c}' | '\n' | '\r' | '\t' => 2,
            '\u{0000}'..='\u{001f}' => 6,
            other => other.len_utf8(),
        })
    })
}

fn valid_field_path(field: &str) -> bool {
    !field.is_empty()
        && field.split('.').all(|component| {
            !component.is_empty()
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                && component.as_bytes()[0].is_ascii_alphabetic()
        })
}

fn encode_path_component(bytes: &[u8]) -> String {
    const PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
        .add(b' ')
        .add(b'"')
        .add(b'#')
        .add(b'%')
        .add(b'/')
        .add(b'<')
        .add(b'>')
        .add(b'?')
        .add(b'`')
        .add(b'{')
        .add(b'}');
    percent_encoding::percent_encode(bytes, PATH_SEGMENT).to_string()
}

fn encode_query_component(bytes: &[u8]) -> String {
    const QUERY: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    percent_encoding::percent_encode(bytes, QUERY).to_string()
}

fn append_form_component(output: &mut String, bytes: &[u8]) -> Result<(), TemplateError> {
    for byte in bytes {
        match *byte {
            b' ' => push_limited(output, "+", MAX_RENDERED_BODY_BYTES)?,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                push_limited(
                    output,
                    std::str::from_utf8(std::slice::from_ref(byte))
                        .map_err(|_| TemplateError::Serialization)?,
                    MAX_RENDERED_BODY_BYTES,
                )?;
            }
            value => {
                if output.len().saturating_add(3) > MAX_RENDERED_BODY_BYTES {
                    return Err(TemplateError::RenderedBodyTooLarge);
                }
                use fmt::Write;
                let _ = write!(output, "%{value:02X}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn whole_json_tokens_keep_types_and_mixed_tokens_are_strings() {
        let context = json!({"change":{"to":"down"}, "path":{"local_ips":["192.0.2.1"]}});
        let whole = Template::compile("{path.local_ips}").expect("template");
        assert_eq!(
            whole.render_json(&context).expect("render"),
            json!(["192.0.2.1"])
        );
        let mixed = Template::compile("state={change.to}").expect("template");
        assert_eq!(
            mixed.render_json(&context).expect("render"),
            json!("state=down")
        );
    }

    #[test]
    fn url_path_query_and_form_values_are_context_encoded() {
        let context = json!({"path":{"name":"wan/a b"}, "event":{"type":"path.interval"}});
        let url = WebhookUrl::parse(
            "https://notify.example.net/hooks/{path.name}?source=static",
            vec![QueryTemplate {
                name: "kind".into(),
                value: Template::compile("{event.type}").expect("template"),
            }],
        )
        .expect("url");
        let uri = url.render_uri(&context).expect("render");
        assert_eq!(uri.path(), "/hooks/wan%2Fa%20b");
        assert_eq!(uri.query(), Some("source=static&kind=path.interval"));
        let mut form = String::new();
        append_form_component(&mut form, b"a b&c").expect("bounded form value");
        assert_eq!(form, "a+b%26c");
    }

    #[test]
    fn dynamic_query_values_use_the_query_table() {
        assert!(
            WebhookUrl::parse(
                "https://notify.example.net/hook?event={event.type}",
                Vec::new()
            )
            .is_err()
        );
        let url = WebhookUrl::parse(
            "https://notify.example.net/hook?source=static",
            vec![QueryTemplate {
                name: "event".to_owned(),
                value: Template::compile("{event.type}").expect("template"),
            }],
        )
        .expect("static query plus dynamic query table");
        let rendered = url
            .render_uri(&json!({"event":{"type":"path.interval"}}))
            .expect("rendered uri");
        assert_eq!(rendered.query(), Some("source=static&event=path.interval"));
    }

    #[test]
    fn secret_debug_is_redacted_and_crlf_is_rejected() {
        let secret = HeaderValueTemplate::Secret(Arc::from(&b"hidden-token"[..]));
        assert!(!format!("{secret:?}").contains("hidden-token"));
        assert!(HeaderValue::from_str("ok\r\ninjected").is_err());
    }

    #[test]
    fn repeated_fields_stop_at_render_budget_and_optional_addresses_keep_json_null() {
        let large = "x".repeat(MAX_RENDERED_BODY_BYTES / 2 + 1);
        let template = Template::compile("{value}{value}").expect("template");
        assert_eq!(
            template.render_text(&json!({"value": large})),
            Err(TemplateError::RenderedOutputTooLarge)
        );

        let address = Template::compile("{carrier.local.ip}").expect("template");
        assert_eq!(
            address
                .render_json(&json!({"carrier":{"local":null}}))
                .expect("null address"),
            Value::Null
        );
        assert!(
            address
                .render_text(&json!({"carrier":{"local":null}}))
                .is_err()
        );
    }

    #[test]
    fn nested_json_template_budget_includes_container_bytes() {
        let value = "x".repeat(MAX_RENDERED_BODY_BYTES / 2);
        let template =
            JsonTemplate::compile(json!(["{value}", "{value}"])).expect("compiled JSON template");
        assert_eq!(
            template.render(&json!({"value": value})),
            Err(TemplateError::RenderedBodyTooLarge)
        );
    }
}
