
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
