use super::tun::TunL4Config;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressConfig {
    Socks5 { listen: Vec<SocketAddr> },
    HttpConnect { listen: Vec<SocketAddr> },
    TunL4(TunL4Config),
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct ProxyAuthConfig {
    credentials: Option<ProxyCredentials>,
}

impl ProxyAuthConfig {
    pub fn disabled() -> Self {
        Self { credentials: None }
    }

    pub fn required(username: String, password: String) -> Self {
        Self {
            credentials: Some(ProxyCredentials { username, password }),
        }
    }

    pub fn is_required(&self) -> bool {
        self.credentials.is_some()
    }

    pub fn credentials(&self) -> Option<&ProxyCredentials> {
        self.credentials.as_ref()
    }

    pub fn verify(&self, username: &str, password: &str) -> bool {
        self.credentials
            .as_ref()
            .is_some_and(|credentials| credentials.verify(username, password))
    }

    pub fn verify_basic_header(&self, value: Option<&str>) -> bool {
        let Some(value) = value else {
            return false;
        };
        let Some(encoded) = value.trim().strip_prefix("Basic ") else {
            return false;
        };
        let Ok(decoded) = BASE64_STANDARD.decode(encoded.trim()) else {
            return false;
        };
        let Ok(decoded) = String::from_utf8(decoded) else {
            return false;
        };
        let Some((username, password)) = decoded.split_once(':') else {
            return false;
        };
        self.verify(username, password)
    }
}

impl std::fmt::Debug for ProxyAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.credentials {
            Some(credentials) => f
                .debug_struct("ProxyAuthConfig")
                .field("credentials", credentials)
                .finish(),
            None => f
                .debug_struct("ProxyAuthConfig")
                .field("credentials", &"disabled")
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProxyCredentials {
    username: String,
    password: String,
}

impl ProxyCredentials {
    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    fn verify(&self, username: &str, password: &str) -> bool {
        constant_time_eq(self.username.as_bytes(), username.as_bytes())
            & constant_time_eq(self.password.as_bytes(), password.as_bytes())
    }
}

impl std::fmt::Debug for ProxyCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    let max_len = expected.len().max(actual.len());
    let mut diff = expected.len() ^ actual.len();
    for index in 0..max_len {
        let lhs = expected.get(index).copied().unwrap_or(0);
        let rhs = actual.get(index).copied().unwrap_or(0);
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_credentials_debug_redacts_password() {
        let auth = ProxyAuthConfig::required("operator".to_string(), "secret".to_string());

        let rendered = format!("{auth:?}");

        assert!(rendered.contains("operator"));
        assert!(!rendered.contains("secret"));
    }

    #[test]
    fn proxy_auth_verifies_basic_header() {
        let auth = ProxyAuthConfig::required("operator".to_string(), "secret".to_string());
        let header = format!(
            "Basic {}",
            BASE64_STANDARD.encode("operator:secret".as_bytes())
        );

        assert!(auth.verify_basic_header(Some(&header)));
        assert!(!auth.verify_basic_header(Some("Basic bad")));
        assert!(!auth.verify_basic_header(None));
    }
}
