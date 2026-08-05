//! One best-effort release check at process startup.
//!
//! The check uses neutral system networking, never delays runtime readiness,
//! and is cancelled before the process emits its terminal lifecycle record.

use crate::config::LogLevel;
use crate::transport::Endpoint;
use crate::transport::tcp::{TcpConnectOptions, connect_endpoint};
use bytes::Bytes;
use http::{Method, Request, Version, header};
use rustls::pki_types::ServerName;
use serde::Deserialize;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;

const GITHUB_API_HOST: &str = "api.github.com";
const GITHUB_RELEASE_API: &str = "https://api.github.com/repos/mnihyc/mptunnel/releases/latest";
const GITHUB_RELEASE_BASE: &str = "https://github.com/mnihyc/mptunnel/releases/tag/";
const UPDATE_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const UPDATE_CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_RELEASE_RESPONSE_BYTES: usize = 256 * 1024;

pub(crate) fn spawn(runtime: &tokio::runtime::Runtime) -> JoinHandle<()> {
    runtime.spawn(async {
        let outcome = tokio::time::timeout(UPDATE_CHECK_TIMEOUT, fetch_latest_release()).await;
        match outcome {
            Ok(Ok(latest_tag)) => report_latest_release(&latest_tag),
            Ok(Err(error)) => report_check_failure(&error),
            Err(_) => report_check_failure(&UpdateCheckError::Timeout),
        }
    })
}

async fn fetch_latest_release() -> Result<String, UpdateCheckError> {
    let endpoint =
        Endpoint::new(GITHUB_API_HOST, 443).map_err(|_| UpdateCheckError::InvalidEndpoint)?;
    let stream = connect_endpoint(
        &endpoint,
        TcpConnectOptions {
            timeout: UPDATE_CONNECT_TIMEOUT,
            ..TcpConnectOptions::default()
        },
    )
    .await
    .map_err(|_| UpdateCheckError::Connect)?;

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut tls_config = rustls::ClientConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"h2".to_vec()];
    let server_name =
        ServerName::try_from(GITHUB_API_HOST).map_err(|_| UpdateCheckError::InvalidEndpoint)?;
    let tls = TlsConnector::from(Arc::new(tls_config))
        .connect(server_name, stream)
        .await
        .map_err(|_| UpdateCheckError::Tls)?;

    let (mut sender, connection) = h2::client::Builder::new()
        .handshake::<_, Bytes>(tls)
        .await
        .map_err(|_| UpdateCheckError::Protocol)?;
    let _connection = AbortOnDrop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    sender = sender
        .ready()
        .await
        .map_err(|_| UpdateCheckError::Protocol)?;
    let request = Request::builder()
        .method(Method::GET)
        .version(Version::HTTP_2)
        .uri(GITHUB_RELEASE_API)
        .header(header::ACCEPT, "application/vnd.github+json")
        .header(header::ACCEPT_ENCODING, "identity")
        .header(header::USER_AGENT, "mptunnel-update-check")
        .body(())
        .map_err(|_| UpdateCheckError::Protocol)?;
    let (response, _) = sender
        .send_request(request, true)
        .map_err(|_| UpdateCheckError::Protocol)?;
    let response = response.await.map_err(|_| UpdateCheckError::Protocol)?;
    if !response.status().is_success() {
        return Err(UpdateCheckError::HttpStatus(response.status().as_u16()));
    }
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if !matches!(
        content_type,
        Some("application/json" | "application/vnd.github+json")
    ) {
        return Err(UpdateCheckError::Metadata);
    }
    if response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_RELEASE_RESPONSE_BYTES)
    {
        return Err(UpdateCheckError::ResponseTooLarge);
    }

    let mut body = Vec::new();
    let mut stream = response.into_body();
    while let Some(chunk) = stream.data().await {
        let chunk = chunk.map_err(|_| UpdateCheckError::Protocol)?;
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or(UpdateCheckError::ResponseTooLarge)?;
        if next_len > MAX_RELEASE_RESPONSE_BYTES {
            return Err(UpdateCheckError::ResponseTooLarge);
        }
        let chunk_len = chunk.len();
        body.extend_from_slice(&chunk);
        stream
            .flow_control()
            .release_capacity(chunk_len)
            .map_err(|_| UpdateCheckError::Protocol)?;
    }
    parse_latest_release(&body)
}

fn parse_latest_release(body: &[u8]) -> Result<String, UpdateCheckError> {
    if body.len() > MAX_RELEASE_RESPONSE_BYTES {
        return Err(UpdateCheckError::ResponseTooLarge);
    }
    let release =
        serde_json::from_slice::<LatestRelease>(body).map_err(|_| UpdateCheckError::Metadata)?;
    if release.draft
        || release.prerelease
        || !release.immutable
        || release
            .assets
            .iter()
            .filter(|asset| asset.name == "version.json")
            .count()
            != 1
    {
        return Err(UpdateCheckError::Metadata);
    }
    parse_release_tag(&release.tag_name).ok_or(UpdateCheckError::Metadata)?;
    Ok(release.tag_name)
}

fn report_latest_release(latest_tag: &str) {
    let current = env!("CARGO_PKG_VERSION");
    let Some(current_version) = parse_version(current) else {
        report_check_failure(&UpdateCheckError::CurrentVersion);
        return;
    };
    let Some(latest_version) = parse_release_tag(latest_tag) else {
        report_check_failure(&UpdateCheckError::Metadata);
        return;
    };
    match latest_version.cmp(&current_version) {
        Ordering::Greater => crate::observability::emit_lifecycle(
            LogLevel::Info,
            "update",
            "available",
            format_args!(
                "MPTUNNEL {latest_tag} is available (running {current}): {GITHUB_RELEASE_BASE}{latest_tag}"
            ),
        ),
        Ordering::Equal => crate::observability::emit_lifecycle(
            LogLevel::Info,
            "update",
            "current",
            format_args!("MPTUNNEL {current} is up to date; newest checked release: {latest_tag}"),
        ),
        Ordering::Less => crate::observability::emit_lifecycle(
            LogLevel::Info,
            "update",
            "current",
            format_args!(
                "No update is available for MPTUNNEL {current}; newest checked release: {latest_tag}"
            ),
        ),
    }
}

fn report_check_failure(error: &UpdateCheckError) {
    crate::observability::emit_lifecycle(
        LogLevel::Info,
        "update",
        "check_failed",
        format_args!("GitHub update check could not complete: {error}"),
    );
}

fn parse_release_tag(tag: &str) -> Option<ReleaseVersion> {
    parse_version(tag.strip_prefix('v')?)
}

fn parse_version(version: &str) -> Option<ReleaseVersion> {
    let mut parts = version.split('.');
    let major = parse_version_component(parts.next()?)?;
    let minor = parse_version_component(parts.next()?)?;
    let patch = parse_version_component(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some(ReleaseVersion {
        major,
        minor,
        patch,
    })
}

fn parse_version_component(component: &str) -> Option<u64> {
    if component.is_empty()
        || !component.bytes().all(|byte| byte.is_ascii_digit())
        || (component.len() > 1 && component.starts_with('0'))
    {
        return None;
    }
    component.parse().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ReleaseVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

#[derive(Deserialize)]
struct LatestRelease {
    tag_name: String,
    draft: bool,
    prerelease: bool,
    immutable: bool,
    assets: Vec<ReleaseAsset>,
}

#[derive(Deserialize)]
struct ReleaseAsset {
    name: String,
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateCheckError {
    Timeout,
    InvalidEndpoint,
    Connect,
    Tls,
    Protocol,
    HttpStatus(u16),
    ResponseTooLarge,
    Metadata,
    CurrentVersion,
}

impl fmt::Display for UpdateCheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("request timed out"),
            Self::InvalidEndpoint => formatter.write_str("GitHub endpoint is invalid"),
            Self::Connect => formatter.write_str("GitHub could not be reached"),
            Self::Tls => formatter.write_str("GitHub TLS authentication failed"),
            Self::Protocol => formatter.write_str("GitHub returned an invalid HTTP response"),
            Self::HttpStatus(status) => write!(formatter, "GitHub returned HTTP {status}"),
            Self::ResponseTooLarge => {
                formatter.write_str("GitHub release metadata exceeded the response limit")
            }
            Self::Metadata => formatter.write_str("GitHub returned invalid release metadata"),
            Self::CurrentVersion => formatter.write_str("the running version is not stable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_versions_use_stable_semantic_ordering() {
        assert!(parse_release_tag("v0.10.0") > parse_release_tag("v0.9.9"));
        assert_eq!(parse_release_tag("v1.2.3"), parse_version("1.2.3"));
        for malformed in [
            "1.2.3",
            "v1.2",
            "v1.2.3.4",
            "v01.2.3",
            "v1.2.3-rc.1",
            "v1.two.3",
        ] {
            assert_eq!(parse_release_tag(malformed), None, "{malformed}");
        }
    }

    #[test]
    fn release_metadata_is_bounded_and_builds_only_the_canonical_url() {
        let tag = parse_latest_release(
            br#"{"tag_name":"v2.3.4","draft":false,"prerelease":false,"immutable":true,"assets":[{"name":"version.json"}]}"#,
        )
        .expect("stable GitHub release metadata");
        assert_eq!(tag, "v2.3.4");
        assert_eq!(
            format!("{GITHUB_RELEASE_BASE}{tag}"),
            "https://github.com/mnihyc/mptunnel/releases/tag/v2.3.4"
        );
        assert!(
            parse_latest_release(
                br#"{"tag_name":"latest","draft":false,"prerelease":false,"immutable":true,"assets":[{"name":"version.json"}]}"#,
            )
            .is_err()
        );
        assert!(
            parse_latest_release(
                br#"{"tag_name":"v2.3.4","draft":false,"prerelease":false,"immutable":false,"assets":[{"name":"version.json"}]}"#,
            )
            .is_err()
        );
        assert!(parse_latest_release(&vec![b'x'; MAX_RELEASE_RESPONSE_BYTES + 1]).is_err());
    }
}
