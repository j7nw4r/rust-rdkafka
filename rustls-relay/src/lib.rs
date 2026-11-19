//! Prototype: TLS to Kafka brokers through Rustls, with librdkafka built
//! without OpenSSL.
//!
//! librdkafka uses `security.protocol=plaintext` (or `sasl_plaintext`). The
//! connect hook on [`ClientContext`] sends each broker socket to a loopback
//! listener in this process. The relay opens TCP to the real broker, does the
//! TLS handshake with Rustls, and copies bytes in both directions.

use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, error, warn};
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

#[derive(Debug)]
struct Target {
    host: String,
    broker: SocketAddr,
}

type Pending = Arc<Mutex<HashMap<SocketAddr, Target>>>;

/// A loopback TCP listener that wraps broker connections in TLS.
pub struct Relay {
    runtime: Option<Runtime>,
    listen_v4: SocketAddr,
    listen_v6: Option<SocketAddr>,
    pending: Pending,
}

impl Relay {
    /// Starts the relay listeners and its Tokio runtime.
    pub fn start(tls: Arc<ClientConfig>) -> io::Result<Relay> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("rustls-relay")
            .enable_all()
            .build()?;
        let pending = Pending::default();
        let connector = TlsConnector::from(tls);
        let v4 = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let v6 = std::net::TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).ok();
        let listen_v4 = v4.local_addr()?;
        let listen_v6 = v6.as_ref().map(|l| l.local_addr()).transpose()?;
        for listener in std::iter::once(v4).chain(v6) {
            listener.set_nonblocking(true)?;
            let (pending, connector) = (pending.clone(), connector.clone());
            runtime.spawn(async move {
                match TcpListener::from_std(listener) {
                    Ok(listener) => accept_loop(listener, pending, connector).await,
                    Err(e) => error!("Relay listener failed: {e}"),
                }
            });
        }
        Ok(Relay {
            runtime: Some(runtime),
            listen_v4,
            listen_v6,
            pending,
        })
    }

    /// Connects a librdkafka broker socket to the relay.
    ///
    /// Call this method from [`ClientContext::connect`]. It does not block.
    pub fn connect(
        &self,
        socket: BrokerSocket,
        broker: SocketAddr,
        broker_name: &str,
    ) -> io::Result<()> {
        let (bind, listener) = match broker {
            SocketAddr::V4(_) => (Ipv4Addr::LOCALHOST.into(), self.listen_v4),
            SocketAddr::V6(_) => (
                Ipv6Addr::LOCALHOST.into(),
                self.listen_v6.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::AddrNotAvailable, "no IPv6 loopback")
                })?,
            ),
        };
        let host = broker_host(broker_name).to_string();
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
        self.pending
            .lock()
            .unwrap()
            .insert(local, Target { host, broker });
        match sock.connect(&listener.into()) {
            Ok(()) => Ok(()),
            Err(e) if connect_in_progress(&e) => Ok(()),
            Err(e) => {
                self.pending.lock().unwrap().remove(&local);
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

async fn accept_loop(listener: TcpListener, pending: Pending, connector: TlsConnector) {
    loop {
        let (local, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!("Relay accept failed: {e}");
                continue;
            }
        };
        let Some(target) = pending.lock().unwrap().remove(&peer) else {
            warn!("Relay dropped a connection from unknown peer {peer}");
            continue;
        };
        tokio::spawn(relay(local, target, connector.clone()));
    }
}

async fn relay(mut local: TcpStream, target: Target, connector: TlsConnector) {
    let Target { host, broker } = target;
    let tls = async {
        let name = ServerName::try_from(host.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let tcp = TcpStream::connect(broker).await?;
        tcp.set_nodelay(true)?;
        connector.connect(name, tcp).await
    };
    let mut tls = match tokio::time::timeout(BROKER_TIMEOUT, tls).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(e)) => {
            error!("TLS connection to broker {host} ({broker}) failed: {e}");
            return;
        }
        Err(_) => {
            error!("TLS connection to broker {host} ({broker}) timed out");
            return;
        }
    };
    let _ = local.set_nodelay(true);
    match tokio::io::copy_bidirectional(&mut local, &mut tls).await {
        Ok((up, down)) => debug!("Relay to {host} closed: {up} bytes up, {down} bytes down"),
        Err(e) => debug!("Relay to {host} closed: {e}"),
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
    use super::broker_host;

    #[test]
    fn test_broker_host() {
        assert_eq!(
            broker_host("kafka-1.example.com:9093"),
            "kafka-1.example.com"
        );
        assert_eq!(broker_host("[::1]:9093"), "::1");
        assert_eq!(broker_host("localhost"), "localhost");
    }
}
