//! Canonical byte-material sources for persisted configuration.
//!
//! A source is resolved once while compiling a configuration generation.
//! Resolved bytes and inline values never enter diagnostics or management
//! projections.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub(super) enum MaterialSource {
    Tagged(TaggedMaterialSource),
    Raw(RawMaterialSource),
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawMaterialSource {
    value: String,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "from", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum TaggedMaterialSource {
    File { path: PathBuf },
    Env { var: String },
    Hex { value: String },
    Base64 { value: String },
    Raw { value: String },
}

impl MaterialSource {
    pub(super) fn resolve(
        &self,
        material_base: &Path,
        purpose: &'static str,
    ) -> Result<Material, MaterialSourceError> {
        let bytes = match self {
            Self::Raw(RawMaterialSource { value })
            | Self::Tagged(TaggedMaterialSource::Raw { value }) => value.as_bytes().to_vec(),
            Self::Tagged(TaggedMaterialSource::File { path }) => {
                read_material_path(material_base, path, purpose)?
            }
            Self::Tagged(TaggedMaterialSource::Env { var }) => {
                validate_environment_name(var, purpose)?;
                let path = std::env::var_os(var).ok_or_else(|| {
                    MaterialSourceError::EnvironmentMissing {
                        purpose,
                        name: var.clone(),
                    }
                })?;
                read_material_path(material_base, Path::new(&path), purpose)?
            }
            Self::Tagged(TaggedMaterialSource::Hex { value }) => decode_hex(value, purpose)?,
            Self::Tagged(TaggedMaterialSource::Base64 { value }) => BASE64
                .decode(value.as_bytes())
                .map_err(|_| MaterialSourceError::InvalidBase64 { purpose })?,
        };
        Ok(Material(bytes))
    }
}

impl std::fmt::Debug for MaterialSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tagged(TaggedMaterialSource::File { path }) => {
                formatter.debug_struct("File").field("path", path).finish()
            }
            Self::Tagged(TaggedMaterialSource::Env { var }) => {
                formatter.debug_struct("Env").field("var", var).finish()
            }
            Self::Tagged(TaggedMaterialSource::Hex { .. }) => formatter
                .debug_struct("Hex")
                .field("value", &"<redacted>")
                .finish(),
            Self::Tagged(TaggedMaterialSource::Base64 { .. }) => formatter
                .debug_struct("Base64")
                .field("value", &"<redacted>")
                .finish(),
            Self::Tagged(TaggedMaterialSource::Raw { .. }) | Self::Raw(_) => formatter
                .debug_struct("Raw")
                .field("value", &"<redacted>")
                .finish(),
        }
    }
}

fn read_material_path(
    material_base: &Path,
    configured: &Path,
    purpose: &'static str,
) -> Result<Vec<u8>, MaterialSourceError> {
    let resolved = resolve_material_path(material_base, configured);
    std::fs::read(&resolved).map_err(|source| MaterialSourceError::FileRead {
        purpose,
        path: configured.to_path_buf(),
        source,
    })
}

fn decode_hex(value: &str, purpose: &'static str) -> Result<Vec<u8>, MaterialSourceError> {
    if !value.len().is_multiple_of(2) {
        return Err(MaterialSourceError::InvalidHex { purpose });
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or(MaterialSourceError::InvalidHex { purpose })?;
            let low = hex_nibble(pair[1]).ok_or(MaterialSourceError::InvalidHex { purpose })?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn read_secret_file(
    path: &Path,
    purpose: &'static str,
) -> Result<Vec<u8>, MaterialSourceError> {
    let mut bytes = std::fs::read(path).map_err(|source| MaterialSourceError::FileRead {
        purpose,
        path: path.to_path_buf(),
        source,
    })?;
    normalize_secret_bytes(&mut bytes);
    Ok(bytes)
}

pub(crate) fn read_secret_environment(
    name: &str,
    purpose: &'static str,
) -> Result<Vec<u8>, MaterialSourceError> {
    validate_environment_name(name, purpose)?;
    let value = std::env::var_os(name).ok_or_else(|| MaterialSourceError::EnvironmentMissing {
        purpose,
        name: name.to_string(),
    })?;
    let value = value
        .into_string()
        .map_err(|_| MaterialSourceError::EnvironmentNotUnicode {
            purpose,
            name: name.to_string(),
        })?;
    let mut bytes = value.into_bytes();
    normalize_secret_bytes(&mut bytes);
    Ok(bytes)
}

pub(crate) fn normalize_secret_bytes(bytes: &mut Vec<u8>) {
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
}

fn validate_environment_name(name: &str, purpose: &'static str) -> Result<(), MaterialSourceError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(MaterialSourceError::EnvironmentNameInvalid {
            purpose,
            name: name.to_string(),
        });
    }
    Ok(())
}

fn resolve_material_path(base: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        base.join(configured)
    }
}

pub(super) struct Material(Vec<u8>);

impl Material {
    pub(super) fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub(super) fn into_utf8(self, purpose: &'static str) -> Result<String, MaterialSourceError> {
        String::from_utf8(self.0).map_err(|_| MaterialSourceError::NotUtf8 { purpose })
    }
}

impl std::fmt::Debug for Material {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Material(<redacted>)")
    }
}

#[derive(Debug)]
pub enum MaterialSourceError {
    FileRead {
        purpose: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    EnvironmentNameInvalid {
        purpose: &'static str,
        name: String,
    },
    EnvironmentMissing {
        purpose: &'static str,
        name: String,
    },
    EnvironmentNotUnicode {
        purpose: &'static str,
        name: String,
    },
    InvalidHex {
        purpose: &'static str,
    },
    InvalidBase64 {
        purpose: &'static str,
    },
    NotUtf8 {
        purpose: &'static str,
    },
}

impl std::fmt::Display for MaterialSourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileRead {
                purpose,
                path,
                source,
            } => write!(
                formatter,
                "failed to read {purpose} file {}: {source}",
                path.display()
            ),
            Self::EnvironmentNameInvalid { purpose, name } => write!(
                formatter,
                "{purpose} environment reference {name:?} has an invalid name"
            ),
            Self::EnvironmentMissing { purpose, name } => write!(
                formatter,
                "{purpose} environment reference {name:?} is not set"
            ),
            Self::EnvironmentNotUnicode { purpose, name } => write!(
                formatter,
                "{purpose} environment reference {name:?} is not valid Unicode"
            ),
            Self::InvalidHex { purpose } => {
                write!(formatter, "{purpose} material is not valid hexadecimal")
            }
            Self::InvalidBase64 { purpose } => {
                write!(formatter, "{purpose} material is not valid base64")
            }
            Self::NotUtf8 { purpose } => {
                write!(formatter, "{purpose} material is not valid UTF-8")
            }
        }
    }
}

impl std::error::Error for MaterialSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FileRead { source, .. } => Some(source),
            Self::EnvironmentNameInvalid { .. }
            | Self::EnvironmentMissing { .. }
            | Self::EnvironmentNotUnicode { .. }
            | Self::InvalidHex { .. }
            | Self::InvalidBase64 { .. }
            | Self::NotUtf8 { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::ffi::OsString;
    use std::sync::Mutex;

    static ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SourceDocument {
        material: MaterialSource,
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mptunnel-material-source-test-{}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir(&path).expect("create material-source test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct EnvironmentGuard {
        name: &'static str,
        previous: Option<OsString>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvironmentGuard {
        fn set(name: &'static str, value: &Path) -> Self {
            let lock = ENVIRONMENT_LOCK.lock().expect("environment test lock");
            let previous = std::env::var_os(name);
            // SAFETY: every test mutation of this variable is serialized by
            // ENVIRONMENT_LOCK and the name is private to this module.
            unsafe { std::env::set_var(name, value) };
            Self {
                name,
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvironmentGuard {
        fn drop(&mut self) {
            // SAFETY: this guard still owns ENVIRONMENT_LOCK.
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.name, value),
                    None => std::env::remove_var(self.name),
                }
            }
        }
    }

    fn parse(document: &str) -> MaterialSource {
        toml::from_str::<SourceDocument>(document)
            .expect("material-source document")
            .material
    }

    #[test]
    fn canonical_sources_resolve_to_exact_bytes() {
        const VARIABLE: &str = "MPTUNNEL_TEST_MATERIAL_SOURCE_FILE";
        let directory = TestDirectory::new();
        let bytes = b"exact\0bytes\r\n";
        std::fs::write(directory.0.join("material.bin"), bytes).expect("write material file");
        let _environment = EnvironmentGuard::set(VARIABLE, Path::new("material.bin"));

        let file = parse(r#"material = { from = "file", path = "material.bin" }"#);
        let env = parse(&format!(
            r#"material = {{ from = "env", var = "{VARIABLE}" }}"#
        ));
        let hex = parse(r#"material = { from = "hex", value = "65786163740062797465730d0a" }"#);
        let base64 = parse(r#"material = { from = "base64", value = "ZXhhY3QAYnl0ZXMNCg==" }"#);
        let raw = parse("material = { from = \"raw\", value = \"exact\\u0000bytes\\r\\n\" }");
        let implicit_raw = parse("material = { value = \"exact\\u0000bytes\\r\\n\" }");

        for source in [file, env, hex, base64, raw, implicit_raw] {
            assert_eq!(
                source
                    .resolve(&directory.0, "test material")
                    .expect("resolve material")
                    .into_bytes(),
                bytes
            );
        }
    }

    #[test]
    fn encoded_sources_are_strict_and_redacted() {
        for invalid in [
            r#"material = { from = "hex", value = "abc" }"#,
            r#"material = { from = "hex", value = "00 11" }"#,
            r#"material = { from = "base64", value = "YQ" }"#,
            r#"material = { from = "base64", value = "Y Q==" }"#,
            r#"material = { from = "base64", value = "-w==" }"#,
            r#"material = { from = "base64", value = "YQ===" }"#,
            r#"material = { from = "base64", value = "YR==" }"#,
        ] {
            let source = parse(invalid);
            assert!(source.resolve(Path::new("."), "canary purpose").is_err());
        }

        let source =
            parse(r#"material = { from = "raw", value = "material-source-secret-canary" }"#);
        assert!(!format!("{source:?}").contains("material-source-secret-canary"));
    }

    #[test]
    fn removed_source_spellings_are_rejected() {
        for removed in [
            r#"material = { from = "environment", variable = "OLD_SECRET" }"#,
            r#"material = { from = "file", value = "material.bin" }"#,
            r#"material = { from = "env", variable = "MATERIAL_FILE" }"#,
            r#"material = { from = "raw", value = "ok", extra = true }"#,
        ] {
            assert!(
                toml::from_str::<SourceDocument>(removed).is_err(),
                "{removed}"
            );
        }
    }
}
