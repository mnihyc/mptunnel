//! Revisioned persistence for the canonical configuration document.
//!
//! This owner is deliberately independent from runtime actors. It validates a
//! complete candidate, compares one immutable revision, and durably replaces
//! the selected TOML file. Runtime generation preparation and activation sit
//! above this module and must never mutate the file directly.

use super::file::load_config_toml_str_at;
use super::{AppConfig, ConfigFileError};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MAX_CONFIG_DOCUMENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_PENDING_JOURNAL_BYTES: usize = 512;
const REVISION_PREFIX: &str = "sha256:";
const LAST_GOOD_SUFFIX: &str = ".mptunnel.last-good";
const PENDING_SUFFIX: &str = ".mptunnel.pending";
const PENDING_SCHEMA: &str = "mptunnel-config-pending-v1";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConfigRevision([u8; 32]);

impl ConfigRevision {
    pub fn from_bytes(document: &[u8]) -> Self {
        Self(Sha256::digest(document).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ConfigRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for ConfigRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REVISION_PREFIX)?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for ConfigRevision {
    type Err = ConfigRevisionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let digest = value
            .strip_prefix(REVISION_PREFIX)
            .ok_or(ConfigRevisionParseError)?;
        if digest.len() != 64 {
            return Err(ConfigRevisionParseError);
        }
        if !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ConfigRevisionParseError);
        }
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            let start = index * 2;
            *byte = u8::from_str_radix(&digest[start..start + 2], 16)
                .map_err(|_| ConfigRevisionParseError)?;
        }
        Ok(Self(bytes))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigRevisionParseError;

impl fmt::Display for ConfigRevisionParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("configuration revision must be sha256: followed by 64 lowercase hex digits")
    }
}

impl std::error::Error for ConfigRevisionParseError {}

pub struct ValidatedConfigCandidate {
    revision: ConfigRevision,
    document: Arc<[u8]>,
    config: AppConfig,
}

impl ValidatedConfigCandidate {
    pub fn revision(&self) -> ConfigRevision {
        self.revision
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }
}

impl fmt::Debug for ValidatedConfigCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedConfigCandidate")
            .field("revision", &self.revision)
            .field("document_bytes", &self.document.len())
            .field("config", &"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
pub struct CommittedConfig {
    pub revision: ConfigRevision,
    pub changed: bool,
    pub config: AppConfig,
}

struct StoreState {
    revision: ConfigRevision,
    document: Arc<[u8]>,
    config: AppConfig,
    active_revision: ConfigRevision,
    active_document: Arc<[u8]>,
    active_config: AppConfig,
    pending: Option<PendingActivation>,
}

pub struct CanonicalConfigStore {
    path: PathBuf,
    last_good_path: PathBuf,
    pending_path: PathBuf,
    mutation: Mutex<()>,
    state: Mutex<StoreState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingActivation {
    active: ConfigRevision,
    desired: ConfigRevision,
}

impl fmt::Debug for CommittedConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedConfig")
            .field("revision", &self.revision)
            .field("changed", &self.changed)
            .field("config", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for CanonicalConfigStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .state
            .lock()
            .expect("canonical configuration store lock");
        formatter
            .debug_struct("CanonicalConfigStore")
            .field("path", &self.path)
            .field("revision", &state.revision)
            .field("active_revision", &state.active_revision)
            .field(
                "pending_revision",
                &state.pending.map(|pending| pending.desired),
            )
            .field("document_bytes", &state.document.len())
            .finish()
    }
}

impl CanonicalConfigStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<(Self, AppConfig), ConfigStoreError> {
        let [path, last_good_path, pending_path] = canonical_config_owned_paths(path.into())?;
        let document = recover_interrupted_activation(&path, &last_good_path, &pending_path)?;
        let config = validate_document(&path, &document)?;
        let revision = ConfigRevision::from_bytes(&document);
        let document: Arc<[u8]> = document.into();
        Ok((
            Self {
                path,
                last_good_path,
                pending_path,
                mutation: Mutex::new(()),
                state: Mutex::new(StoreState {
                    revision,
                    document: document.clone(),
                    config: config.clone(),
                    active_revision: revision,
                    active_document: document,
                    active_config: config.clone(),
                    pending: None,
                }),
            },
            config,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn lock_mutation(&self) -> std::sync::MutexGuard<'_, ()> {
        self.mutation
            .lock()
            .expect("canonical configuration mutation lock")
    }

    pub(crate) fn owned_paths(&self) -> [&Path; 3] {
        [&self.path, &self.last_good_path, &self.pending_path]
    }

    pub fn revision(&self) -> ConfigRevision {
        self.state
            .lock()
            .expect("canonical configuration store lock")
            .revision
    }

    pub fn active_revision(&self) -> ConfigRevision {
        self.state
            .lock()
            .expect("canonical configuration store lock")
            .active_revision
    }

    pub fn pending_revision(&self) -> Option<ConfigRevision> {
        self.state
            .lock()
            .expect("canonical configuration store lock")
            .pending
            .map(|pending| pending.desired)
    }

    pub(crate) fn current_config(&self) -> AppConfig {
        self.state
            .lock()
            .expect("canonical configuration store lock")
            .config
            .clone()
    }

    pub(crate) fn active_config(&self) -> AppConfig {
        self.state
            .lock()
            .expect("canonical configuration store lock")
            .active_config
            .clone()
    }

    pub fn validate_candidate(
        &self,
        document: impl AsRef<[u8]>,
    ) -> Result<ValidatedConfigCandidate, ConfigStoreError> {
        let document = document.as_ref();
        validate_document_size(document)?;
        let config = validate_document(&self.path, document)?;
        Ok(ValidatedConfigCandidate {
            revision: ConfigRevision::from_bytes(document),
            document: Arc::from(document),
            config,
        })
    }

    /// Durably replaces the complete canonical document if `expected` is still
    /// the active desired-state revision.
    ///
    /// A manual edit observed after this process opened the store is never
    /// overwritten. The caller must reload and deliberately start a new
    /// transaction from that document.
    pub fn replace(
        &self,
        expected: ConfigRevision,
        candidate: ValidatedConfigCandidate,
    ) -> Result<CommittedConfig, ConfigStoreError> {
        let mut state = self
            .state
            .lock()
            .expect("canonical configuration store lock");
        if expected != state.revision {
            return Err(ConfigStoreError::RevisionConflict {
                expected,
                actual: state.revision,
            });
        }

        let disk_document = read_document(&self.path)?;
        let disk_revision = ConfigRevision::from_bytes(&disk_document);
        if disk_revision != state.revision || disk_document.as_slice() != state.document.as_ref() {
            return Err(ConfigStoreError::ExternalModification {
                known: state.revision,
                actual: disk_revision,
            });
        }

        if candidate.revision == state.revision
            && candidate.document.as_ref() == state.document.as_ref()
        {
            return Ok(CommittedConfig {
                revision: state.revision,
                changed: false,
                config: candidate.config,
            });
        }

        if let Some(pending) = state.pending {
            return Err(ConfigStoreError::ActivationPending {
                desired: pending.desired,
            });
        }

        atomic_replace(&self.last_good_path, &state.active_document)?;
        let pending = PendingActivation {
            active: state.active_revision,
            desired: candidate.revision,
        };
        atomic_replace(&self.pending_path, &encode_pending_activation(pending))?;
        if let Err(error) = atomic_replace(&self.path, &candidate.document) {
            // The canonical document is still the active one. Clearing the
            // journal keeps this live store usable; if cleanup itself is
            // interrupted, open-time recovery recognizes the active revision.
            let _ = remove_sidecar(&self.pending_path);
            return Err(error);
        }
        state.revision = candidate.revision;
        state.document = candidate.document;
        state.config = candidate.config.clone();
        state.pending = Some(pending);
        Ok(CommittedConfig {
            revision: state.revision,
            changed: true,
            config: candidate.config,
        })
    }

    /// Marks the desired document as the durable last-good generation.
    ///
    /// The runtime must call this only after every required service in that
    /// generation has reported ready. Until then, an interrupted process start
    /// restores the prior active document from the sidecar journal.
    pub fn activate_desired(
        &self,
        revision: ConfigRevision,
    ) -> Result<CommittedConfig, ConfigStoreError> {
        let mut state = self
            .state
            .lock()
            .expect("canonical configuration store lock");
        if revision != state.revision {
            return Err(ConfigStoreError::ActivationRevisionConflict {
                expected: revision,
                actual: state.revision,
            });
        }
        let changed = state.active_revision != state.revision || state.pending.is_some();
        verify_disk_document(&self.path, state.revision, &state.document)?;
        atomic_replace(&self.last_good_path, &state.document)?;
        if state.pending.is_some() {
            remove_sidecar(&self.pending_path)?;
        }
        state.active_revision = state.revision;
        state.active_document = state.document.clone();
        state.active_config = state.config.clone();
        state.pending = None;
        Ok(CommittedConfig {
            revision: state.revision,
            changed,
            config: state.config.clone(),
        })
    }

    /// Restores the active last-good document after a candidate generation
    /// fails before readiness.
    pub fn rollback_pending(&self) -> Result<CommittedConfig, ConfigStoreError> {
        let mut state = self
            .state
            .lock()
            .expect("canonical configuration store lock");
        let Some(pending) = state.pending else {
            return Ok(CommittedConfig {
                revision: state.revision,
                changed: false,
                config: state.config.clone(),
            });
        };
        debug_assert_eq!(pending.active, state.active_revision);
        debug_assert_eq!(pending.desired, state.revision);
        verify_disk_document(&self.path, state.revision, &state.document)?;
        atomic_replace(&self.path, &state.active_document)?;
        remove_sidecar(&self.pending_path)?;
        state.revision = state.active_revision;
        state.document = state.active_document.clone();
        state.config = state.active_config.clone();
        state.pending = None;
        Ok(CommittedConfig {
            revision: state.revision,
            changed: true,
            config: state.config.clone(),
        })
    }
}

pub(crate) fn canonical_config_owned_paths(
    path: impl Into<PathBuf>,
) -> Result<[PathBuf; 3], ConfigStoreError> {
    let path = path.into();
    let last_good_path = sidecar_path(&path, LAST_GOOD_SUFFIX)?;
    let pending_path = sidecar_path(&path, PENDING_SUFFIX)?;
    Ok([path, last_good_path, pending_path])
}

pub(crate) fn paths_equivalent(left: &Path, right: &Path) -> bool {
    material_path_for_comparison(left) == material_path_for_comparison(right)
        || same_file::is_same_file(left, right).unwrap_or(false)
}

fn material_path_for_comparison(path: &Path) -> PathBuf {
    if let Ok(canonical) = fs::canonicalize(path) {
        return canonical;
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    match path.file_name() {
        Some(file_name) => parent.join(file_name),
        None => parent,
    }
}

fn sidecar_path(path: &Path, suffix: &str) -> Result<PathBuf, ConfigStoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut file_name = path
        .file_name()
        .ok_or(ConfigStoreError::InvalidPath)?
        .to_os_string();
    file_name.push(suffix);
    Ok(parent.join(file_name))
}

fn recover_interrupted_activation(
    path: &Path,
    last_good_path: &Path,
    pending_path: &Path,
) -> Result<Vec<u8>, ConfigStoreError> {
    let current = read_document(path)?;
    let Some(journal) = read_optional_bounded(pending_path, MAX_PENDING_JOURNAL_BYTES)? else {
        return Ok(current);
    };
    let pending = decode_pending_activation(&journal)?;
    let current_revision = ConfigRevision::from_bytes(&current);
    if current_revision == pending.active {
        remove_sidecar(pending_path)?;
        return Ok(current);
    }
    if current_revision != pending.desired {
        return Err(ConfigStoreError::RecoveryConflict(Box::new(
            ConfigRecoveryConflict {
                active: pending.active,
                desired: pending.desired,
                current: current_revision,
                last_good: None,
            },
        )));
    }

    let last_good = read_optional_bounded(last_good_path, MAX_CONFIG_DOCUMENT_BYTES)?
        .ok_or(ConfigStoreError::LastGoodMissing)?;
    validate_document(path, &last_good)?;
    let last_good_revision = ConfigRevision::from_bytes(&last_good);
    if last_good_revision == pending.desired {
        // Activation committed the new last-good document but crashed before
        // deleting the journal. The canonical document is already correct.
        remove_sidecar(pending_path)?;
        return Ok(current);
    }
    if last_good_revision != pending.active {
        return Err(ConfigStoreError::RecoveryConflict(Box::new(
            ConfigRecoveryConflict {
                active: pending.active,
                desired: pending.desired,
                current: current_revision,
                last_good: Some(last_good_revision),
            },
        )));
    }

    atomic_replace(path, &last_good)?;
    remove_sidecar(pending_path)?;
    Ok(last_good)
}

fn verify_disk_document(
    path: &Path,
    known_revision: ConfigRevision,
    known_document: &[u8],
) -> Result<(), ConfigStoreError> {
    let disk_document = read_document(path)?;
    let disk_revision = ConfigRevision::from_bytes(&disk_document);
    if disk_revision != known_revision || disk_document.as_slice() != known_document {
        return Err(ConfigStoreError::ExternalModification {
            known: known_revision,
            actual: disk_revision,
        });
    }
    Ok(())
}

fn encode_pending_activation(pending: PendingActivation) -> Vec<u8> {
    format!(
        "{PENDING_SCHEMA}\nactive={}\ndesired={}\n",
        pending.active, pending.desired
    )
    .into_bytes()
}

fn decode_pending_activation(document: &[u8]) -> Result<PendingActivation, ConfigStoreError> {
    let document =
        std::str::from_utf8(document).map_err(|_| ConfigStoreError::InvalidPendingJournal)?;
    let mut lines = document.lines();
    if lines.next() != Some(PENDING_SCHEMA) {
        return Err(ConfigStoreError::InvalidPendingJournal);
    }
    let active = lines
        .next()
        .and_then(|line| line.strip_prefix("active="))
        .ok_or(ConfigStoreError::InvalidPendingJournal)?
        .parse()
        .map_err(|_| ConfigStoreError::InvalidPendingJournal)?;
    let desired = lines
        .next()
        .and_then(|line| line.strip_prefix("desired="))
        .ok_or(ConfigStoreError::InvalidPendingJournal)?
        .parse()
        .map_err(|_| ConfigStoreError::InvalidPendingJournal)?;
    if lines.next().is_some() || active == desired {
        return Err(ConfigStoreError::InvalidPendingJournal);
    }
    Ok(PendingActivation { active, desired })
}

fn read_optional_bounded(path: &Path, limit: usize) -> Result<Option<Vec<u8>>, ConfigStoreError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ConfigStoreError::Io(error)),
    };
    let metadata = file.metadata().map_err(ConfigStoreError::Io)?;
    let file_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if file_len > limit {
        return Err(ConfigStoreError::DocumentTooLarge {
            actual: file_len,
            limit,
        });
    }
    let mut document = Vec::with_capacity(file_len);
    file.take((limit + 1) as u64)
        .read_to_end(&mut document)
        .map_err(ConfigStoreError::Io)?;
    if document.len() > limit {
        return Err(ConfigStoreError::DocumentTooLarge {
            actual: document.len(),
            limit,
        });
    }
    Ok(Some(document))
}

fn remove_sidecar(path: &Path) -> Result<(), ConfigStoreError> {
    match fs::remove_file(path) {
        Ok(()) => sync_parent_directory(path.parent().unwrap_or_else(|| Path::new(".")))
            .map_err(ConfigStoreError::Io),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ConfigStoreError::Io(error)),
    }
}

fn read_document(path: &Path) -> Result<Vec<u8>, ConfigStoreError> {
    let file = File::open(path).map_err(ConfigStoreError::Io)?;
    let metadata = file.metadata().map_err(ConfigStoreError::Io)?;
    let file_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if file_len > MAX_CONFIG_DOCUMENT_BYTES {
        return Err(ConfigStoreError::DocumentTooLarge {
            actual: file_len,
            limit: MAX_CONFIG_DOCUMENT_BYTES,
        });
    }
    let mut document = Vec::with_capacity(file_len);
    file.take((MAX_CONFIG_DOCUMENT_BYTES + 1) as u64)
        .read_to_end(&mut document)
        .map_err(ConfigStoreError::Io)?;
    validate_document_size(&document)?;
    Ok(document)
}

fn validate_document_size(document: &[u8]) -> Result<(), ConfigStoreError> {
    if document.len() > MAX_CONFIG_DOCUMENT_BYTES {
        Err(ConfigStoreError::DocumentTooLarge {
            actual: document.len(),
            limit: MAX_CONFIG_DOCUMENT_BYTES,
        })
    } else {
        Ok(())
    }
}

fn validate_document(path: &Path, document: &[u8]) -> Result<AppConfig, ConfigStoreError> {
    let contents = std::str::from_utf8(document).map_err(|_| ConfigStoreError::NonUtf8)?;
    let material_base = path.parent().unwrap_or_else(|| Path::new("."));
    load_config_toml_str_at(contents, material_base).map_err(ConfigStoreError::Config)
}

fn atomic_replace(path: &Path, document: &[u8]) -> Result<(), ConfigStoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(ConfigStoreError::InvalidPath)?;
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".{file_name}.mptunnel.tmp.{}.{}",
        std::process::id(),
        sequence
    ));

    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(document)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        sync_parent_directory(parent)?;
        Ok::<(), io::Error>(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(ConfigStoreError::Io)
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> io::Result<()> {
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> io::Result<()> {
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigRecoveryConflict {
    pub active: ConfigRevision,
    pub desired: ConfigRevision,
    pub current: ConfigRevision,
    pub last_good: Option<ConfigRevision>,
}

#[derive(Debug)]
pub enum ConfigStoreError {
    Io(io::Error),
    Config(ConfigFileError),
    NonUtf8,
    InvalidPath,
    DocumentTooLarge {
        actual: usize,
        limit: usize,
    },
    RevisionConflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
    ExternalModification {
        known: ConfigRevision,
        actual: ConfigRevision,
    },
    ActivationPending {
        desired: ConfigRevision,
    },
    ActivationRevisionConflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
    InvalidPendingJournal,
    LastGoodMissing,
    RecoveryConflict(Box<ConfigRecoveryConflict>),
}

impl fmt::Display for ConfigStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::NonUtf8 => formatter.write_str("configuration document must be valid UTF-8"),
            Self::InvalidPath => formatter.write_str("configuration path must name a file"),
            Self::DocumentTooLarge { actual, limit } => write!(
                formatter,
                "configuration document is {actual} bytes, limit is {limit}"
            ),
            Self::RevisionConflict { expected, actual } => write!(
                formatter,
                "configuration revision conflict: expected {expected}, active revision is {actual}"
            ),
            Self::ExternalModification { known, actual } => write!(
                formatter,
                "configuration file changed outside the active transaction: known {known}, disk revision {actual}"
            ),
            Self::ActivationPending { desired } => write!(
                formatter,
                "configuration activation {desired} is already pending"
            ),
            Self::ActivationRevisionConflict { expected, actual } => write!(
                formatter,
                "cannot activate configuration {expected}; desired revision is {actual}"
            ),
            Self::InvalidPendingJournal => {
                formatter.write_str("configuration activation journal is invalid")
            }
            Self::LastGoodMissing => {
                formatter.write_str("configuration activation journal has no last-good document")
            }
            Self::RecoveryConflict(conflict) => write!(
                formatter,
                "configuration activation recovery conflict: active {}, desired {}, current {}, last-good {}",
                conflict.active,
                conflict.desired,
                conflict.current,
                conflict
                    .last_good
                    .map(|revision| revision.to_string())
                    .unwrap_or_else(|| "missing".to_string())
            ),
        }
    }
}

impl std::error::Error for ConfigStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Config(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
