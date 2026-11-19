//! TLS backends: Rustls (with the `ring` or `symcrypt` provider) and
//! SChannel through native-tls (Windows, `schannel` feature).

#[cfg(any(feature = "ring", feature = "symcrypt"))]
use std::{io, sync::Arc};

use rustls::pki_types::ServerName;
#[cfg(any(feature = "ring", feature = "symcrypt"))]
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::RelayErrorKind;

#[cfg(not(any(feature = "ring", feature = "symcrypt", feature = "schannel")))]
compile_error!("Enable the `ring`, `symcrypt`, or `schannel` feature.");

#[cfg(all(feature = "schannel", not(windows)))]
compile_error!(
    "The `schannel` feature needs a Windows target. On other targets, native-tls uses OpenSSL."
);

/// Returns the Rustls crypto provider that the cargo features select.
///
/// The `symcrypt` feature has priority over the `ring` feature.
#[cfg(any(feature = "ring", feature = "symcrypt"))]
pub fn crypto_provider() -> rustls::crypto::CryptoProvider {
    #[cfg(feature = "symcrypt")]
    return rustls_symcrypt::default_symcrypt_provider();
    #[cfg(not(feature = "symcrypt"))]
    return rustls::crypto::ring::default_provider();
}

/// Returns the name of the crypto provider that [`crypto_provider`] uses.
#[cfg(any(feature = "ring", feature = "symcrypt"))]
pub fn crypto_provider_name() -> &'static str {
    if cfg!(feature = "symcrypt") {
        "symcrypt"
    } else {
        "ring"
    }
}

/// Makes a Rustls client configuration that trusts `roots`.
#[cfg(any(feature = "ring", feature = "symcrypt"))]
pub fn client_config(roots: RootCertStore) -> Result<ClientConfig, rustls::Error> {
    Ok(
        ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// The TLS backend and its settings for a [`Relay`](crate::Relay).
#[derive(Clone)]
pub enum TlsConfig {
    /// Rustls with the crypto provider from [`crypto_provider`].
    #[cfg(any(feature = "ring", feature = "symcrypt"))]
    Rustls(Arc<ClientConfig>),
    /// SChannel through native-tls.
    #[cfg(all(windows, feature = "schannel"))]
    Schannel(native_tls::TlsConnector),
}

#[cfg(any(feature = "ring", feature = "symcrypt"))]
impl From<Arc<ClientConfig>> for TlsConfig {
    fn from(config: Arc<ClientConfig>) -> TlsConfig {
        TlsConfig::Rustls(config)
    }
}

impl TlsConfig {
    /// Makes an SChannel configuration that trusts the Windows root store
    /// and also `extra_roots`.
    ///
    /// SChannel checks the broker host name. It accepts a chain that ends
    /// in one of `extra_roots`, also if that CA is not in a Windows store.
    /// The minimum protocol is TLS 1.2.
    #[cfg(all(windows, feature = "schannel"))]
    pub fn schannel(
        extra_roots: &[rustls::pki_types::CertificateDer<'_>],
    ) -> Result<TlsConfig, native_tls::Error> {
        let mut builder = native_tls::TlsConnector::builder();
        builder.min_protocol_version(Some(native_tls::Protocol::Tlsv12));
        for root in extra_roots {
            builder.add_root_certificate(native_tls::Certificate::from_der(root)?);
        }
        Ok(TlsConfig::Schannel(builder.build()?))
    }

    /// Returns the backend name, and the crypto provider for Rustls.
    pub fn describe(&self) -> String {
        match self {
            #[cfg(any(feature = "ring", feature = "symcrypt"))]
            TlsConfig::Rustls(_) => format!("rustls (crypto provider: {})", crypto_provider_name()),
            #[cfg(all(windows, feature = "schannel"))]
            TlsConfig::Schannel(_) => "schannel".to_string(),
        }
    }
}

pub(crate) trait TlsStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> TlsStream for S {}

pub(crate) enum Connector {
    #[cfg(any(feature = "ring", feature = "symcrypt"))]
    Rustls(tokio_rustls::TlsConnector),
    #[cfg(all(windows, feature = "schannel"))]
    Schannel(tokio_native_tls::TlsConnector),
}

impl From<TlsConfig> for Connector {
    fn from(config: TlsConfig) -> Connector {
        match config {
            #[cfg(any(feature = "ring", feature = "symcrypt"))]
            TlsConfig::Rustls(config) => Connector::Rustls(config.into()),
            #[cfg(all(windows, feature = "schannel"))]
            TlsConfig::Schannel(connector) => Connector::Schannel(connector.into()),
        }
    }
}

impl Connector {
    pub(crate) async fn connect(
        &self,
        name: ServerName<'static>,
        tcp: TcpStream,
    ) -> Result<Box<dyn TlsStream>, (RelayErrorKind, String)> {
        match self {
            #[cfg(any(feature = "ring", feature = "symcrypt"))]
            Connector::Rustls(connector) => match connector.connect(name, tcp).await {
                Ok(stream) => Ok(Box::new(stream)),
                Err(e) => Err((classify_rustls(&e), e.to_string())),
            },
            #[cfg(all(windows, feature = "schannel"))]
            Connector::Schannel(connector) => match connector.connect(&name.to_str(), tcp).await {
                Ok(stream) => Ok(Box::new(stream)),
                Err(e) => {
                    let message = e.to_string();
                    Err((classify_schannel(&message), message))
                }
            },
        }
    }
}

#[cfg(any(feature = "ring", feature = "symcrypt"))]
pub(crate) fn classify_rustls(e: &io::Error) -> RelayErrorKind {
    match e.get_ref().and_then(|e| e.downcast_ref::<rustls::Error>()) {
        Some(rustls::Error::InvalidCertificate(_)) => RelayErrorKind::CertificateInvalid,
        _ => RelayErrorKind::Tls,
    }
}

/// Classifies an SChannel error from its message.
///
/// native-tls gives no access to the Windows error code, so this function
/// reads the code from the `(os error N)` suffix of the message.
#[cfg(any(all(windows, feature = "schannel"), test))]
pub(crate) fn classify_schannel(message: &str) -> RelayErrorKind {
    const SEC_E_WRONG_PRINCIPAL: u32 = 0x8009_0322;
    const SEC_E_UNTRUSTED_ROOT: u32 = 0x8009_0325;
    const SEC_E_CERT_UNKNOWN: u32 = 0x8009_0327;
    const SEC_E_CERT_EXPIRED: u32 = 0x8009_0328;
    const SEC_E_CERT_WRONG_USAGE: u32 = 0x8009_0349;
    const CERT_E_FIRST: u32 = 0x800B_0100;
    const CERT_E_LAST: u32 = 0x800B_01FF;
    const CRYPT_E_REVOKED: u32 = 0x8009_2010;
    const CRYPT_E_NO_REVOCATION_CHECK: u32 = 0x8009_2012;
    const CRYPT_E_REVOCATION_OFFLINE: u32 = 0x8009_2013;

    let code = message
        .rsplit_once("(os error ")
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .and_then(|n| n.parse::<i32>().ok())
        .map(|n| n as u32);
    match code {
        Some(
            SEC_E_WRONG_PRINCIPAL
            | SEC_E_UNTRUSTED_ROOT
            | SEC_E_CERT_UNKNOWN
            | SEC_E_CERT_EXPIRED
            | SEC_E_CERT_WRONG_USAGE
            | CERT_E_FIRST..=CERT_E_LAST
            | CRYPT_E_REVOKED
            | CRYPT_E_NO_REVOCATION_CHECK
            | CRYPT_E_REVOCATION_OFFLINE,
        ) => RelayErrorKind::CertificateInvalid,
        _ => RelayErrorKind::Tls,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_schannel() {
        let untrusted = "A certificate chain processed, but terminated in a root \
                         certificate which is not trusted by the trust provider. \
                         (os error -2146762487)";
        assert_eq!(
            classify_schannel(untrusted),
            RelayErrorKind::CertificateInvalid
        );
        let wrong_name = "The target principal name is incorrect. (os error -2146893022)";
        assert_eq!(
            classify_schannel(wrong_name),
            RelayErrorKind::CertificateInvalid
        );
        let reset = "An existing connection was forcibly closed by the remote host. \
                     (os error 10054)";
        assert_eq!(classify_schannel(reset), RelayErrorKind::Tls);
        assert_eq!(classify_schannel("no code"), RelayErrorKind::Tls);
    }

    #[cfg(any(feature = "ring", feature = "symcrypt"))]
    #[test]
    fn test_classify_rustls() {
        let cert = io::Error::new(
            io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert_eq!(classify_rustls(&cert), RelayErrorKind::CertificateInvalid);
        let alert = io::Error::new(
            io::ErrorKind::InvalidData,
            rustls::Error::AlertReceived(rustls::AlertDescription::HandshakeFailure),
        );
        assert_eq!(classify_rustls(&alert), RelayErrorKind::Tls);
    }
}
