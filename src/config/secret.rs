//! Canonical file/environment secret references.
//!
//! Persisted configuration stores only a reference. Resolution happens once
//! while compiling a configuration generation; resolved bytes never enter
//! diagnostics or management projections.

use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize)]
#[serde(tag = "from", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum SecretMaterialReference {
    File { path: PathBuf },
    Environment { variable: String },
}

impl SecretMaterialReference {
    pub(super) fn resolve(
        self,
        material_base: &Path,
        purpose: &'static str,
    ) -> Result<SecretMaterial, SecretMaterialError> {
        match self {
            Self::File { path } => {
                let resolved = resolve_material_path(material_base, &path);
                let bytes =
                    std::fs::read(&resolved).map_err(|source| SecretMaterialError::FileRead {
                        purpose,
                        path,
                        source,
                    })?;
                Ok(SecretMaterial::new(bytes))
            }
            Self::Environment { variable } => {
                validate_environment_name(&variable, purpose)?;
                let value = std::env::var_os(&variable).ok_or_else(|| {
                    SecretMaterialError::EnvironmentMissing {
                        purpose,
                        name: variable.clone(),
                    }
                })?;
                let value = value.into_string().map_err(|_| {
                    SecretMaterialError::EnvironmentNotUnicode {
                        purpose,
                        name: variable.clone(),
                    }
                })?;
                Ok(SecretMaterial::new(value.into_bytes()))
            }
        }
    }
}

impl std::fmt::Debug for SecretMaterialReference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File { path } => formatter.debug_struct("File").field("path", path).finish(),
            Self::Environment { variable } => formatter
                .debug_struct("Environment")
                .field("variable", variable)
                .finish(),
        }
    }
}

pub(crate) fn read_secret_file(
    path: &Path,
    purpose: &'static str,
) -> Result<Vec<u8>, SecretMaterialError> {
    std::fs::read(path)
        .map(SecretMaterial::new)
        .map(SecretMaterial::into_bytes)
        .map_err(|source| SecretMaterialError::FileRead {
            purpose,
            path: path.to_path_buf(),
            source,
        })
}

pub(crate) fn read_secret_environment(
    name: &str,
    purpose: &'static str,
) -> Result<Vec<u8>, SecretMaterialError> {
    validate_environment_name(name, purpose)?;
    let value = std::env::var_os(name).ok_or_else(|| SecretMaterialError::EnvironmentMissing {
        purpose,
        name: name.to_string(),
    })?;
    let value = value
        .into_string()
        .map_err(|_| SecretMaterialError::EnvironmentNotUnicode {
            purpose,
            name: name.to_string(),
        })?;
    Ok(SecretMaterial::new(value.into_bytes()).into_bytes())
}

pub(crate) fn normalize_secret_bytes(bytes: &mut Vec<u8>) {
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
}

fn validate_environment_name(name: &str, purpose: &'static str) -> Result<(), SecretMaterialError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(SecretMaterialError::EnvironmentNameInvalid {
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

pub(super) struct SecretMaterial(Vec<u8>);

impl SecretMaterial {
    fn new(mut bytes: Vec<u8>) -> Self {
        normalize_secret_bytes(&mut bytes);
        Self(bytes)
    }

    pub(super) fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub(super) fn into_utf8(self, purpose: &'static str) -> Result<String, SecretMaterialError> {
        String::from_utf8(self.0).map_err(|_| SecretMaterialError::NotUtf8 { purpose })
    }
}

impl std::fmt::Debug for SecretMaterial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretMaterial(<redacted>)")
    }
}

#[derive(Debug)]
pub enum SecretMaterialError {
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
    NotUtf8 {
        purpose: &'static str,
    },
}

impl std::fmt::Display for SecretMaterialError {
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
            Self::NotUtf8 { purpose } => {
                write!(formatter, "{purpose} secret material is not valid UTF-8")
            }
        }
    }
}

impl std::error::Error for SecretMaterialError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FileRead { source, .. } => Some(source),
            Self::EnvironmentNameInvalid { .. }
            | Self::EnvironmentMissing { .. }
            | Self::EnvironmentNotUnicode { .. }
            | Self::NotUtf8 { .. } => None,
        }
    }
}
