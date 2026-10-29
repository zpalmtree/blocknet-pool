//! TLS for miners (`stratum+ssl://`). The certificate is reloaded when its
//! files change, so a renewal needs no gateway restart (a restart
//! disconnects every miner).

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// How often the certificate and key files are checked for changes.
pub const CERT_CHECK_INTERVAL: Duration = Duration::from_secs(300);

/// The miner TLS certificate and key, swapped atomically on reload.
pub struct TlsCerts {
    cert_path: PathBuf,
    key_path: PathBuf,
    config: RwLock<Arc<ServerConfig>>,
    /// Identity of the cert and key files the current config came from.
    loaded: Mutex<[FileStamp; 2]>,
}

/// Enough of a file's metadata to notice a rewrite or a moved symlink
/// (Let's Encrypt's live/ files link to a new archive/ file on renewal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
    dev: u64,
    ino: u64,
}

impl FileStamp {
    fn of(path: &Path) -> Result<Self> {
        let meta = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
        Ok(Self {
            modified: meta.modified().ok(),
            len: meta.len(),
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }
}

impl TlsCerts {
    /// Loads a PEM certificate chain (leaf first, e.g. fullchain.pem) and
    /// its PEM private key.
    pub fn load(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Result<Self> {
        let cert_path = cert_path.into();
        let key_path = key_path.into();
        let stamps = [FileStamp::of(&cert_path)?, FileStamp::of(&key_path)?];
        let config = server_config(&cert_path, &key_path)?;
        Ok(Self {
            cert_path,
            key_path,
            config: RwLock::new(config),
            loaded: Mutex::new(stamps),
        })
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        let config = self.config.read().unwrap_or_else(|err| err.into_inner());
        TlsAcceptor::from(Arc::clone(&config))
    }

    /// Reloads the certificate if either file changed since it was loaded.
    /// On error the current certificate stays in use, and the next check
    /// tries again.
    pub fn reload_if_changed(&self) -> Result<bool> {
        // Stamped before reading, so a write racing the read is picked up
        // by the next check.
        let stamps = [
            FileStamp::of(&self.cert_path)?,
            FileStamp::of(&self.key_path)?,
        ];
        if *self.loaded.lock().unwrap_or_else(|err| err.into_inner()) == stamps {
            return Ok(false);
        }
        let config = server_config(&self.cert_path, &self.key_path)?;
        *self.config.write().unwrap_or_else(|err| err.into_inner()) = config;
        *self.loaded.lock().unwrap_or_else(|err| err.into_inner()) = stamps;
        Ok(true)
    }
}

fn server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>> {
    let chain = CertificateDer::pem_file_iter(cert_path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .with_context(|| format!("read certificates from {}", cert_path.display()))?;
    if chain.is_empty() {
        bail!("no certificates in {}", cert_path.display());
    }
    let key = PrivateKeyDer::from_pem_file(key_path)
        .with_context(|| format!("read private key from {}", key_path.display()))?;
    // An explicit provider: the process-wide default is ambiguous when more
    // than one is compiled in.
    let config = ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .with_context(|| {
            format!(
                "certificate {} does not match key {}",
                cert_path.display(),
                key_path.display()
            )
        })?;
    Ok(Arc::new(config))
}

/// Checks the certificate files every `every` and reloads them on change.
pub(crate) async fn watch(certs: Arc<TlsCerts>, every: Duration) {
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    loop {
        interval.tick().await;
        match certs.reload_if_changed() {
            Ok(true) => {
                tracing::info!(cert = %certs.cert_path.display(), "reloaded miner TLS certificate")
            }
            Ok(false) => {}
            Err(err) => tracing::warn!(
                error = %format!("{err:#}"),
                "miner TLS certificate reload failed; keeping the current certificate"
            ),
        }
    }
}
