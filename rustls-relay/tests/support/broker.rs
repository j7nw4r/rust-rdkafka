//! A single Kafka broker in Docker with only SSL and SASL_SSL client
//! listeners. The test and the `serve_broker` example share this file.

use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};

pub const SASL_USERNAME: &str = "alice";
pub const SASL_PASSWORD: &str = "alice-secret";

pub struct DockerBroker {
    pub container: String,
    pub ssl_port: u16,
    pub sasl_ssl_port: u16,
}

impl Drop for DockerBroker {
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

/// Writes `ca.pem` and the broker key store `server.pem` for `host` into
/// `dir`, and returns the CA certificate.
pub fn write_certs(dir: &Path, host: &str) -> rustls::pki_types::CertificateDer<'static> {
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);

    let mut names = vec!["localhost".to_string()];
    if host != "localhost" {
        names.push(host.to_string());
    }
    let server_params = CertificateParams::new(names).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let keystore = format!(
        "{}{}{}",
        server_key.serialize_pem(),
        server_cert.pem(),
        ca_cert.pem()
    );
    fs::write(dir.join("server.pem"), keystore).unwrap();
    fs::write(dir.join("ca.pem"), ca_cert.pem()).unwrap();
    ca_cert.der().clone()
}

/// Starts the broker. It advertises `host` and publishes its ports on
/// `publish_ip`, unless `RELAY_DOCKER_NETWORK` is set.
pub fn start(dir: &Path, host: &str, publish_ip: &str) -> DockerBroker {
    let (ssl_port, sasl_ssl_port) = free_ports();
    let properties = format!(
        r#"process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9094
listeners=INTERNAL://:19092,CONTROLLER://:9094,SSL://:{ssl_port},SASL_SSL://:{sasl_ssl_port}
advertised.listeners=INTERNAL://localhost:19092,SSL://{host}:{ssl_port},SASL_SSL://{host}:{sasl_ssl_port}
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
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret" user_admin="admin-secret" user_{SASL_USERNAME}="{SASL_PASSWORD}";
"#
    );
    fs::write(dir.join("server.properties"), properties).unwrap();

    let image = std::env::var("RELAY_KAFKA_IMAGE").unwrap_or_else(|_| "apache/kafka:4.1.1".into());
    let script = "/opt/kafka/bin/kafka-storage.sh format -t $(/opt/kafka/bin/kafka-storage.sh random-uuid) -c /certs/server.properties \
                  && exec /opt/kafka/bin/kafka-server-start.sh /certs/server.properties";
    let mut run = Command::new("docker");
    run.args(["run", "-d", "--rm"])
        .args(["-v", &format!("{}:/certs:ro", dir.display())]);
    match std::env::var("RELAY_DOCKER_NETWORK") {
        Ok(network) => run.args(["--network", &network]),
        Err(_) => run
            .args(["-p", &format!("{publish_ip}:{ssl_port}:{ssl_port}")])
            .args([
                "-p",
                &format!("{publish_ip}:{sasl_ssl_port}:{sasl_ssl_port}"),
            ]),
    };
    let out = run
        .args(["--entrypoint", "/bin/bash", &image, "-c", script])
        .output()
        .expect("docker run");
    assert!(
        out.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let broker = DockerBroker {
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
