//! Round trip through a Kafka broker that has only TLS listeners.
//!
//! Run with `cargo test -p rustls-relay --test tls_round_trip -- --ignored --nocapture`.
//! By default the test starts the broker in Docker. See the README for the
//! environment variables that select an external broker.

#[path = "support/broker.rs"]
mod broker;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use rustls::RootCertStore;
use rustls_relay::{
    client_config, crypto_provider_name, ErrorCallback, Relay, RelayContext, RelayErrorKind,
};

const MESSAGES: usize = 100;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
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
            .set(
                "sasl.username",
                env_or("RELAY_SASL_USERNAME", broker::SASL_USERNAME),
            )
            .set(
                "sasl.password",
                env_or("RELAY_SASL_PASSWORD", broker::SASL_PASSWORD),
            );
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

    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let mut _docker = None;
    let (ssl, sasl_ssl, ca) = match std::env::var("RELAY_BOOTSTRAP_SSL") {
        Ok(ssl) => {
            let ca_path = std::env::var("RELAY_CA_PEM").expect("RELAY_CA_PEM");
            let ca = CertificateDer::from_pem_file(&ca_path).expect("read RELAY_CA_PEM");
            println!("external broker: {ssl}");
            (ssl, std::env::var("RELAY_BOOTSTRAP_SASL_SSL").ok(), ca)
        }
        Err(_) => {
            let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("relay-{run_id}"));
            fs::create_dir_all(&dir).unwrap();
            let ca = broker::write_certs(&dir, "localhost");
            let docker = broker::start(&dir, "localhost", "127.0.0.1");
            let ssl = format!("localhost:{}", docker.ssl_port);
            let sasl_ssl = format!("localhost:{}", docker.sasl_ssl_port);
            _docker = Some(docker);
            (ssl, Some(sasl_ssl), ca)
        }
    };

    let mut roots = RootCertStore::empty();
    roots.add(ca).unwrap();
    let relay = Arc::new(Relay::start(Arc::new(client_config(roots).unwrap())).unwrap());

    assert!(
        !metadata_ok(None, &ssl),
        "plaintext without the relay must fail on the TLS listener"
    );
    println!("[control] plaintext without the relay: metadata request failed, as expected");

    let failures = Arc::new(AtomicUsize::new(0));
    let kinds = Arc::new(Mutex::new(Vec::new()));
    let on_error: ErrorCallback = {
        let (failures, kinds) = (failures.clone(), kinds.clone());
        Arc::new(move |e| {
            failures.fetch_add(1, Ordering::SeqCst);
            kinds.lock().unwrap().push(e.kind);
        })
    };
    let untrusted_config = Arc::new(client_config(RootCertStore::empty()).unwrap());
    let untrusted =
        Arc::new(Relay::start_with_error_callback(untrusted_config, Some(on_error)).unwrap());
    let started = Instant::now();
    assert!(
        !metadata_ok(Some(&untrusted), &ssl),
        "a relay that does not trust the CA must fail"
    );
    let failures = failures.load(Ordering::SeqCst);
    println!(
        "[control] relay without the test CA: metadata request failed, as expected; \
         {failures} TLS failures in {:?}",
        started.elapsed()
    );
    // The shortest backoff schedule allows 6 failures in 8 seconds.
    assert!((1..=8).contains(&failures), "{failures} TLS failures");
    assert!(kinds
        .lock()
        .unwrap()
        .iter()
        .all(|k| *k == RelayErrorKind::CertificateInvalid));

    round_trip(&relay, &ssl, "plaintext", &format!("relay-ssl-{run_id}")).await;
    match sasl_ssl {
        Some(sasl_ssl) => {
            let topic = format!("relay-sasl-ssl-{run_id}");
            round_trip(&relay, &sasl_ssl, "sasl_plaintext", &topic).await;
        }
        None => println!("[sasl_plaintext] skipped: RELAY_BOOTSTRAP_SASL_SSL is not set"),
    }
}
