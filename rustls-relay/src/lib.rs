//! Prototype: TLS to Kafka brokers through Rustls, with librdkafka built
//! without OpenSSL.
//!
//! librdkafka uses `security.protocol=plaintext` (or `sasl_plaintext`). The
//! connect hook on [`ClientContext`] sends each broker socket to a loopback
//! listener in this process. The relay opens TCP to the real broker, does the
//! TLS handshake with Rustls, and copies bytes in both directions.
//!
//! After a failed broker connection, the relay refuses new connections to
//! that broker address for a backoff time. The refusal happens in the
//! connect callback, so librdkafka applies its own reconnect backoff.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use log::{debug, error, info, warn};
use rdkafka::client::BrokerSocket;
use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use socket2::{SockAddr, SockRef};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio_rustls::TlsConnector;

#[cfg(not(any(feature = "ring", feature = "symcrypt")))]
compile_error!("Enable the `ring` or the `symcrypt` feature.");

const BROKER_TIMEOUT: Duration = Duration::from_secs(10);
const PENDING_TTL: Duration = Duration::from_secs(10);
const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_CAP: Duration = Duration::from_secs(30);

/// Returns the Rustls crypto provider that the cargo features select.
///
/// The `symcrypt` feature has priority over the `ring` feature.
pub fn crypto_provider() -> rustls::crypto::CryptoProvider {
    #[cfg(feature = "symcrypt")]
    return rustls_symcrypt::default_symcrypt_provider();
    #[cfg(not(feature = "symcrypt"))]
    return rustls::crypto::ring::default_provider();
}

/// Returns the name of the crypto provider that [`crypto_provider`] uses.
pub fn crypto_provider_name() -> &'static str {
    if cfg!(feature = "symcrypt") {
        "symcrypt"
    } else {
        "ring"
    }
}

/// Makes a Rustls client configuration that trusts `roots`.
pub fn client_config(roots: RootCertStore) -> Result<ClientConfig, rustls::Error> {
    Ok(
        ClientConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// The type of a failed broker connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayErrorKind {
    /// The broker host is not a valid DNS name or IP address for TLS.
    InvalidServerName,
    /// The broker refused the TCP connection.
    TcpConnectRefused,
    /// The TCP connection to the broker failed for a different reason.
    TcpConnect,
    /// The TCP connection or the TLS handshake took too long.
    Timeout,
    /// The broker certificate is not valid for the broker host.
    CertificateInvalid,
    /// The TLS handshake failed for a different reason.
    Tls,
}

/// A failed connection from the relay to a broker.
#[derive(Clone, Debug)]
pub struct RelayError {
    /// The broker name from librdkafka, in the form `host:port`.
    pub broker_name: String,
    /// The resolved broker address.
    pub broker: SocketAddr,
    /// The type of the failure.
    pub kind: RelayErrorKind,
    /// The error message.
    pub message: String,
}

impl fmt::Display for RelayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} on broker {} ({}): {}",
            self.kind, self.broker_name, self.broker, self.message
        )
    }
}

/// A function that receives each [`RelayError`].
///
/// The relay calls it on its own runtime threads. It must not block.
pub type ErrorCallback = Arc<dyn Fn(&RelayError) + Send + Sync>;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Returns the backoff after `failures` failures in sequence.
///
/// `jitter` is in `[0, 1)`. The result is between half and all of the
/// exponential delay, and never more than the cap.
fn backoff_delay(failures: u32, jitter: f64) -> Duration {
    let exp = failures.saturating_sub(1).min(16);
    let full = BACKOFF_BASE.saturating_mul(1 << exp).min(BACKOFF_CAP);
    full.mul_f64(0.5 + 0.5 * jitter.clamp(0.0, 1.0))
}

fn random_jitter() -> f64 {
    let bits = RandomState::new().build_hasher().finish() >> 11;
    bits as f64 / (1u64 << 53) as f64
}

type BrokerKey = (String, SocketAddr);

#[derive(Default)]
struct Backoff {
    brokers: HashMap<BrokerKey, (u32, Instant)>,
}

impl Backoff {
    fn remaining(&self, key: &BrokerKey, now: Instant) -> Option<Duration> {
        let (_, until) = self.brokers.get(key)?;
        until.checked_duration_since(now).filter(|d| !d.is_zero())
    }

    fn record_failure(&mut self, key: BrokerKey, now: Instant, jitter: f64) -> Duration {
        let entry = self.brokers.entry(key).or_insert((0, now));
        entry.0 = entry.0.saturating_add(1);
        let delay = backoff_delay(entry.0, jitter);
        entry.1 = now + delay;
        delay
    }

    fn record_success(&mut self, key: &BrokerKey) {
        self.brokers.remove(key);
    }
}

#[derive(Clone, Debug)]
struct Target {
    broker_name: String,
    host: String,
    broker: SocketAddr,
}

#[derive(Default)]
struct PendingMap {
    entries: HashMap<SocketAddr, (Target, Instant)>,
}

impl PendingMap {
    /// Registers `local`. A new entry replaces an old one for a reused port.
    fn insert(&mut self, local: SocketAddr, target: Target, now: Instant) {
        self.sweep(now);
        self.entries.insert(local, (target, now));
    }

    fn take(&mut self, local: &SocketAddr, now: Instant) -> Option<Target> {
        let (target, added) = self.entries.remove(local)?;
        (now.saturating_duration_since(added) < PENDING_TTL).then_some(target)
    }

    fn remove(&mut self, local: &SocketAddr) {
        self.entries.remove(local);
    }

    fn sweep(&mut self, now: Instant) {
        self.entries
            .retain(|_, (_, added)| now.saturating_duration_since(*added) < PENDING_TTL);
    }
}

struct Shared {
    pending: Mutex<PendingMap>,
    backoff: Mutex<Backoff>,
    connector: TlsConnector,
    on_error: Option<ErrorCallback>,
}

impl Shared {
    fn fail(&self, target: &Target, kind: RelayErrorKind, message: String) {
        let key = (target.broker_name.clone(), target.broker);
        let delay = lock(&self.backoff).record_failure(key, Instant::now(), random_jitter());
        let err = RelayError {
            broker_name: target.broker_name.clone(),
            broker: target.broker,
            kind,
            message,
        };
        error!("Relay connection failed: {err}. Next attempt in {delay:?} or later.");
        if let Some(on_error) = &self.on_error {
            if panic::catch_unwind(AssertUnwindSafe(|| on_error(&err))).is_err() {
                error!("Relay error callback panicked");
            }
        }
    }
}

/// A loopback TCP listener that wraps broker connections in TLS.
pub struct Relay {
    runtime: Option<Runtime>,
    listen_v4: SocketAddr,
    listen_v6: Option<SocketAddr>,
    shared: Arc<Shared>,
}

impl Relay {
    /// Starts the relay listeners and its Tokio runtime.
    pub fn start(tls: Arc<ClientConfig>) -> io::Result<Relay> {
        Relay::start_with_error_callback(tls, None)
    }

    /// Starts the relay, and calls `on_error` for each failed broker
    /// connection.
    pub fn start_with_error_callback(
        tls: Arc<ClientConfig>,
        on_error: Option<ErrorCallback>,
    ) -> io::Result<Relay> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("rustls-relay")
            .enable_all()
            .build()?;
        let shared = Arc::new(Shared {
            pending: Mutex::default(),
            backoff: Mutex::default(),
            connector: TlsConnector::from(tls),
            on_error,
        });
        let v4 = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let v6 = std::net::TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).ok();
        let listen_v4 = v4.local_addr()?;
        let listen_v6 = v6.as_ref().map(|l| l.local_addr()).transpose()?;
        for listener in std::iter::once(v4).chain(v6) {
            listener.set_nonblocking(true)?;
            let shared = shared.clone();
            runtime.spawn(async move {
                match TcpListener::from_std(listener) {
                    Ok(listener) => accept_loop(listener, shared).await,
                    Err(e) => error!("Relay listener failed: {e}"),
                }
            });
        }
        Ok(Relay {
            runtime: Some(runtime),
            listen_v4,
            listen_v6,
            shared,
        })
    }

    /// Connects a librdkafka broker socket to the relay.
    ///
    /// Call this method from [`ClientContext::connect`]. It does not block.
    /// It returns a "connection refused" error while the broker address is
    /// in backoff after a failure.
    pub fn connect(
        &self,
        socket: BrokerSocket,
        broker: SocketAddr,
        broker_name: &str,
    ) -> io::Result<()> {
        let key = (broker_name.to_string(), broker);
        if let Some(wait) = lock(&self.shared.backoff).remaining(&key, Instant::now()) {
            debug!("Relay refuses {broker_name} ({broker}) for {wait:?} after a failure");
            return Err(connection_refused());
        }
        let (bind, listener) = match broker {
            SocketAddr::V4(_) => (Ipv4Addr::LOCALHOST.into(), self.listen_v4),
            SocketAddr::V6(_) => (
                Ipv6Addr::LOCALHOST.into(),
                self.listen_v6.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::AddrNotAvailable, "no IPv6 loopback")
                })?,
            ),
        };
        let target = Target {
            broker_name: broker_name.to_string(),
            host: broker_host(broker_name).to_string(),
            broker,
        };
        let socket = borrow_socket(socket);
        let sock = SockRef::from(&socket);
        sock.bind(&SockAddr::from(SocketAddr::new(bind, 0)))?;
        let local = sock
            .local_addr()?
            .as_socket()
            .ok_or_else(|| io::Error::other("socket is not TCP/IP"))?;
        debug!("Relay {local} -> {broker_name} ({broker})");
        // Register before the connect, so the accept loop always finds the
        // entry for this source address.
        lock(&self.shared.pending).insert(local, target, Instant::now());
        match sock.connect(&listener.into()) {
            Ok(()) => Ok(()),
            Err(e) if connect_in_progress(&e) => Ok(()),
            Err(e) => {
                lock(&self.shared.pending).remove(&local);
                Err(e)
            }
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

#[cfg(unix)]
fn borrow_socket(socket: BrokerSocket) -> std::os::fd::BorrowedFd<'static> {
    // librdkafka owns the socket and keeps it open during the callback.
    unsafe { std::os::fd::BorrowedFd::borrow_raw(socket) }
}

#[cfg(windows)]
fn borrow_socket(socket: BrokerSocket) -> std::os::windows::io::BorrowedSocket<'static> {
    unsafe { std::os::windows::io::BorrowedSocket::borrow_raw(socket) }
}

fn connection_refused() -> io::Error {
    #[cfg(unix)]
    return io::Error::from_raw_os_error(libc::ECONNREFUSED);
    #[cfg(windows)]
    return io::Error::from_raw_os_error(10061); // WSAECONNREFUSED
}

fn connect_in_progress(e: &io::Error) -> bool {
    #[cfg(unix)]
    if e.raw_os_error() == Some(libc::EINPROGRESS) {
        return true;
    }
    e.kind() == io::ErrorKind::WouldBlock
}

fn broker_host(broker_name: &str) -> &str {
    let host = broker_name
        .rsplit_once(':')
        .map_or(broker_name, |(host, _)| host);
    host.trim_start_matches('[').trim_end_matches(']')
}

fn classify_tcp(e: &io::Error) -> RelayErrorKind {
    if e.kind() == io::ErrorKind::ConnectionRefused {
        RelayErrorKind::TcpConnectRefused
    } else {
        RelayErrorKind::TcpConnect
    }
}

fn classify_handshake(e: &io::Error) -> RelayErrorKind {
    match e.get_ref().and_then(|e| e.downcast_ref::<rustls::Error>()) {
        Some(rustls::Error::InvalidCertificate(_)) => RelayErrorKind::CertificateInvalid,
        _ => RelayErrorKind::Tls,
    }
}

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        let (local, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!("Relay accept failed: {e}");
                continue;
            }
        };
        let Some(target) = lock(&shared.pending).take(&peer, Instant::now()) else {
            warn!("Relay dropped a connection from unknown or expired peer {peer}");
            continue;
        };
        tokio::spawn(relay(local, target, shared.clone()));
    }
}

async fn relay(mut local: TcpStream, target: Target, shared: Arc<Shared>) {
    let name = match ServerName::try_from(target.host.clone()) {
        Ok(name) => name,
        Err(e) => {
            return shared.fail(&target, RelayErrorKind::InvalidServerName, e.to_string());
        }
    };
    let tcp = match tokio::time::timeout(BROKER_TIMEOUT, TcpStream::connect(target.broker)).await {
        Ok(Ok(tcp)) => tcp,
        Ok(Err(e)) => return shared.fail(&target, classify_tcp(&e), e.to_string()),
        Err(_) => {
            let message = format!("TCP connect took more than {BROKER_TIMEOUT:?}");
            return shared.fail(&target, RelayErrorKind::Timeout, message);
        }
    };
    let _ = tcp.set_nodelay(true);
    let handshake = shared.connector.connect(name, tcp);
    let mut tls = match tokio::time::timeout(BROKER_TIMEOUT, handshake).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(e)) => return shared.fail(&target, classify_handshake(&e), e.to_string()),
        Err(_) => {
            let message = format!("TLS handshake took more than {BROKER_TIMEOUT:?}");
            return shared.fail(&target, RelayErrorKind::Timeout, message);
        }
    };
    lock(&shared.backoff).record_success(&(target.broker_name.clone(), target.broker));
    info!(
        "Relay TLS session up to {} ({})",
        target.broker_name, target.broker
    );
    let _ = local.set_nodelay(true);
    match tokio::io::copy_bidirectional(&mut local, &mut tls).await {
        Ok((up, down)) => debug!(
            "Relay to {} closed: {up} bytes up, {down} bytes down",
            target.broker_name
        ),
        Err(e) => debug!("Relay to {} closed: {e}", target.broker_name),
    }
}

/// A client and consumer context that sends broker connections through a
/// [`Relay`].
#[derive(Clone)]
pub struct RelayContext {
    relay: Arc<Relay>,
}

impl RelayContext {
    /// Makes a context that uses `relay`.
    pub fn new(relay: Arc<Relay>) -> RelayContext {
        RelayContext { relay }
    }
}

impl ClientContext for RelayContext {
    const ENABLE_CONNECT_CALLBACK: bool = true;

    fn connect(&self, socket: BrokerSocket, addr: SocketAddr, broker_name: &str) -> io::Result<()> {
        self.relay.connect(socket, addr, broker_name)
    }
}

impl ConsumerContext for RelayContext {}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn test_broker_host() {
        assert_eq!(
            broker_host("kafka-1.example.com:9093"),
            "kafka-1.example.com"
        );
        assert_eq!(broker_host("[::1]:9093"), "::1");
        assert_eq!(broker_host("localhost"), "localhost");
    }

    #[test]
    fn test_backoff_delay_schedule() {
        let low: Vec<_> = (1..=8).map(|n| backoff_delay(n, 0.0)).collect();
        let high: Vec<_> = (1..=8).map(|n| backoff_delay(n, 1.0)).collect();
        assert_eq!(
            low,
            [250, 500, 1000, 2000, 4000, 8000, 15000, 15000].map(|n| n * MS)
        );
        assert_eq!(
            high,
            [500, 1000, 2000, 4000, 8000, 16000, 30000, 30000].map(|n| n * MS)
        );
        assert_eq!(backoff_delay(u32::MAX, 1.0), BACKOFF_CAP);
        for _ in 0..100 {
            let jitter = random_jitter();
            assert!((0.0..1.0).contains(&jitter));
        }
    }

    #[test]
    fn test_backoff_window_and_reset() {
        let key: BrokerKey = ("b:9093".into(), "127.0.0.1:9093".parse().unwrap());
        let other: BrokerKey = ("b:9093".into(), "127.0.0.2:9093".parse().unwrap());
        let mut backoff = Backoff::default();
        let t0 = Instant::now();
        assert_eq!(backoff.remaining(&key, t0), None);

        assert_eq!(backoff.record_failure(key.clone(), t0, 0.0), 250 * MS);
        assert_eq!(backoff.remaining(&key, t0 + 100 * MS), Some(150 * MS));
        assert_eq!(backoff.remaining(&key, t0 + 250 * MS), None);
        assert_eq!(backoff.remaining(&other, t0), None);

        let t1 = t0 + 300 * MS;
        assert_eq!(backoff.record_failure(key.clone(), t1, 0.0), 500 * MS);
        backoff.record_success(&key);
        assert_eq!(backoff.remaining(&key, t1), None);
        assert_eq!(backoff.record_failure(key, t1, 0.0), 250 * MS);
    }

    #[test]
    fn test_pending_ttl_and_reuse() {
        let target = |name: &str| Target {
            broker_name: name.into(),
            host: "b".into(),
            broker: "127.0.0.1:9093".parse().unwrap(),
        };
        let a: SocketAddr = "127.0.0.1:50001".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:50002".parse().unwrap();
        let t0 = Instant::now();
        let mut pending = PendingMap::default();

        pending.insert(a, target("old"), t0);
        assert!(pending.take(&a, t0 + PENDING_TTL).is_none());

        pending.insert(a, target("old"), t0);
        pending.insert(b, target("other"), t0 + PENDING_TTL);
        assert_eq!(pending.entries.len(), 1, "insert sweeps expired entries");

        pending.insert(b, target("new"), t0 + PENDING_TTL + MS);
        let got = pending.take(&b, t0 + PENDING_TTL + 2 * MS).unwrap();
        assert_eq!(got.broker_name, "new");
        assert!(pending.take(&b, t0 + PENDING_TTL + 3 * MS).is_none());
    }

    #[test]
    fn test_classify_errors() {
        let cert = io::Error::new(
            io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert_eq!(
            classify_handshake(&cert),
            RelayErrorKind::CertificateInvalid
        );
        let alert = io::Error::new(
            io::ErrorKind::InvalidData,
            rustls::Error::AlertReceived(rustls::AlertDescription::HandshakeFailure),
        );
        assert_eq!(classify_handshake(&alert), RelayErrorKind::Tls);
        let refused = io::Error::from(io::ErrorKind::ConnectionRefused);
        assert_eq!(classify_tcp(&refused), RelayErrorKind::TcpConnectRefused);
    }
}
