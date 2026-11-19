//! Round trip through a Kafka broker that has only TLS listeners.
//!
//! Run with `cargo test -p rustls-relay --test tls_round_trip -- --ignored --nocapture`.
//! The test needs Docker. Set `RELAY_KAFKA_IMAGE` to change the broker image.

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};
use rustls::RootCertStore;
use rustls_relay::{client_config, crypto_provider_name, Relay, RelayContext};

const MESSAGES: usize = 100;

struct Broker {
    container: String,
    ssl_port: u16,
    sasl_ssl_port: u16,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
    }
}

fn free_ports() -> (u16, u16) {
    let a = TcpListener::bind("127.0.0.1:0").unwrap();
    let b = TcpListener::bind("127.0.0.1:0").unwrap();
    (
        a.local_addr().unwrap().port(),
        b.local_addr().unwrap().port(),
    )
}

/// Writes a test CA and a broker key store, and returns the CA certificate.
fn write_certs(dir: &Path) -> rustls::pki_types::CertificateDer<'static> {
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);

    let server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let keystore = format!(
        "{}{}{}",
        server_key.serialize_pem(),
        server_cert.pem(),
        ca_cert.pem()
    );
    fs::write(dir.join("server.pem"), keystore).unwrap();
    ca_cert.der().clone()
}

fn start_broker(dir: &Path) -> Broker {
    let (ssl_port, sasl_ssl_port) = free_ports();
    let properties = format!(
        r#"process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9094
listeners=INTERNAL://:19092,CONTROLLER://:9094,SSL://:{ssl_port},SASL_SSL://:{sasl_ssl_port}
advertised.listeners=INTERNAL://localhost:19092,SSL://localhost:{ssl_port},SASL_SSL://localhost:{sasl_ssl_port}
listener.security.protocol.map=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
log.dirs=/tmp/kraft-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
ssl.keystore.type=PEM
ssl.keystore.location=/certs/server.pem
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret" user_admin="admin-secret" user_alice="alice-secret";
"#
    );
    fs::write(dir.join("server.properties"), properties).unwrap();

    let image = std::env::var("RELAY_KAFKA_IMAGE").unwrap_or_else(|_| "apache/kafka:4.1.1".into());
    let script = "/opt/kafka/bin/kafka-storage.sh format -t $(/opt/kafka/bin/kafka-storage.sh random-uuid) -c /certs/server.properties \
                  && exec /opt/kafka/bin/kafka-server-start.sh /certs/server.properties";
    let out = Command::new("docker")
        .args(["run", "-d", "--rm"])
        .args(["-v", &format!("{}:/certs:ro", dir.display())])
        .args(["-p", &format!("127.0.0.1:{ssl_port}:{ssl_port}")])
        .args(["-p", &format!("127.0.0.1:{sasl_ssl_port}:{sasl_ssl_port}")])
        .args(["--entrypoint", "/bin/bash", &image, "-c", script])
        .output()
        .expect("docker run");
    assert!(
        out.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let broker = Broker {
        container: String::from_utf8(out.stdout).unwrap().trim().to_string(),
        ssl_port,
        sasl_ssl_port,
    };

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let logs = Command::new("docker")
            .args(["logs", &broker.container])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&logs.stdout).contains("Kafka Server started") {
            return broker;
        }
        assert!(
            Instant::now() < deadline,
            "broker did not start: {}{}",
            String::from_utf8_lossy(&logs.stdout),
            String::from_utf8_lossy(&logs.stderr)
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn base_config(bootstrap: &str, security_protocol: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap)
        .set("security.protocol", security_protocol)
        .set("broker.address.family", "v4");
    if security_protocol == "sasl_plaintext" {
        config
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", "alice")
            .set("sasl.password", "alice-secret");
    }
    config
}

async fn round_trip(relay: &Arc<Relay>, bootstrap: &str, security_protocol: &str, topic: &str) {
    let producer: FutureProducer<RelayContext> = base_config(bootstrap, security_protocol)
        .set("message.timeout.ms", "30000")
        .create_with_context(RelayContext::new(relay.clone()))
        .expect("producer");
    for i in 0..MESSAGES {
        let payload = format!("message {i}");
        producer
            .send(
                FutureRecord::<(), _>::to(topic).payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("produce {i} failed: {e}"));
    }
    println!("[{security_protocol}] produced {MESSAGES} messages to {topic}");

    let consumer: BaseConsumer<RelayContext> = base_config(bootstrap, security_protocol)
        .set("group.id", format!("{topic}-group"))
        .set("enable.auto.commit", "false")
        .create_with_context(RelayContext::new(relay.clone()))
        .expect("consumer");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();

    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while received.len() < MESSAGES && Instant::now() < deadline {
        if let Some(msg) = consumer.poll(Duration::from_millis(500)) {
            let msg = msg.expect("consume");
            received.push(String::from_utf8(msg.payload().unwrap().to_vec()).unwrap());
        }
    }
    println!(
        "[{security_protocol}] consumed {} of {MESSAGES} messages from {topic}",
        received.len()
    );
    let expected: Vec<String> = (0..MESSAGES).map(|i| format!("message {i}")).collect();
    assert_eq!(received, expected);
}

/// Returns true if a metadata request through `relay` gets an answer.
fn metadata_ok(relay: Option<&Arc<Relay>>, bootstrap: &str) -> bool {
    let config = base_config(bootstrap, "plaintext");
    let timeout = Duration::from_secs(8);
    match relay {
        Some(relay) => {
            let consumer: BaseConsumer<RelayContext> = config
                .create_with_context(RelayContext::new(relay.clone()))
                .unwrap();
            consumer.fetch_metadata(None, timeout).is_ok()
        }
        None => {
            let consumer: BaseConsumer = config.create().unwrap();
            consumer.fetch_metadata(None, timeout).is_ok()
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn tls_round_trip() {
    let _ = env_logger::builder().is_test(true).try_init();
    println!("crypto provider: {}", crypto_provider_name());
    let features = ClientConfig::new()
        .create_native_config()
        .unwrap()
        .get("builtin.features")
        .unwrap();
    println!("librdkafka builtin.features: {features}");
    assert!(!features.split(',').any(|f| f == "ssl"));

    let dir =
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("relay-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let ca = write_certs(&dir);
    let broker = start_broker(&dir);
    let ssl = format!("localhost:{}", broker.ssl_port);
    let sasl_ssl = format!("localhost:{}", broker.sasl_ssl_port);

    let mut roots = RootCertStore::empty();
    roots.add(ca).unwrap();
    let relay = Arc::new(Relay::start(Arc::new(client_config(roots).unwrap())).unwrap());

    assert!(
        !metadata_ok(None, &ssl),
        "plaintext without the relay must fail on the TLS listener"
    );
    println!("[control] plaintext without the relay: metadata request failed, as expected");

    let untrusted =
        Arc::new(Relay::start(Arc::new(client_config(RootCertStore::empty()).unwrap())).unwrap());
    assert!(
        !metadata_ok(Some(&untrusted), &ssl),
        "a relay that does not trust the CA must fail"
    );
    println!("[control] relay without the test CA: metadata request failed, as expected");

    round_trip(&relay, &ssl, "plaintext", "relay-ssl").await;
    round_trip(&relay, &sasl_ssl, "sasl_plaintext", "relay-sasl-ssl").await;
    drop(broker);
    let _ = fs::remove_dir_all(&dir);
}
