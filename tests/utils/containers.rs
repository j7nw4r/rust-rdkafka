use anyhow::Context;
use std::fmt::Debug;
use std::sync::Arc;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::core::ContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;

type KafkaImage = testcontainers_modules::testcontainers::core::ContainerRequest<Kafka>;

pub struct KafkaContext {
    kafka_node: ContainerAsync<Kafka>,
    pub bootstrap_servers: String,
    pub version: String,
}

impl Debug for KafkaContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaContext").finish()
    }
}

impl KafkaContext {
    pub async fn shared() -> anyhow::Result<Arc<Self>> {
        static INSTANCE: OnceCell<Arc<KafkaContext>> = OnceCell::const_new();

        INSTANCE
            .get_or_try_init(init)
            .await
            .context("Failed to initialize Kafka shared instance")
            .map(Arc::clone)
    }

    pub async fn std_out(&self) -> anyhow::Result<String> {
        let std_out_byte_vec = self
            .kafka_node
            .stdout_to_vec()
            .await
            .context("Failed to get stdout")?;
        Ok(String::from_utf8(std_out_byte_vec)?)
    }

    pub async fn std_err(&self) -> anyhow::Result<String> {
        let std_err_byte_vec = self
            .kafka_node
            .stderr_to_vec()
            .await
            .context("Failed to get stderr")?;
        Ok(String::from_utf8(std_err_byte_vec)?)
    }
}

async fn init() -> anyhow::Result<Arc<KafkaContext>> {
    let kafka_tag = resolve_kafka_image_tag();
    let kafka_container: KafkaImage = Kafka::default()
        // The kafka-native image (the crate default) doesn't publish 3.7.x
        // tags, so use the JVM image which covers the full CI matrix range.
        .with_jvm_image()
        .with_tag(&kafka_tag)
        // The single-broker testcontainers image needs replication and ISR
        // overrides; otherwise transactions hang because __transaction_state
        // can't reach its default replication factor of 3.
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

    let kafka_node = kafka_container
        .start()
        .await
        .context("Failed to start Kafka")?;
    let kafka_host = kafka_node
        .get_host()
        .await
        .context("Failed to get Kafka host")?;
    let kafka_port = kafka_node
        .get_host_port_ipv4(ContainerPort::Tcp(9092))
        .await?;

    Ok::<Arc<KafkaContext>, anyhow::Error>(Arc::new(KafkaContext {
        kafka_node,
        bootstrap_servers: format!("{}:{}", kafka_host, kafka_port),
        version: kafka_tag,
    }))
}

// Map the CI matrix's short KAFKA_VERSION (e.g. "3.7") onto a specific
// apache/kafka tag so each matrix row actually exercises a different broker.
// Without this, the crate's hard-coded default tag would make every row run
// the same image. Full tag strings (e.g. "3.9.1") are passed through.
fn resolve_kafka_image_tag() -> String {
    let raw = std::env::var("KAFKA_VERSION").unwrap_or_else(|_| "4.0".into());
    match raw.as_str() {
        "3.7" => "3.7.2".into(),
        "3.8" => "3.8.1".into(),
        "3.9" => "3.9.2".into(),
        "4.0" => "4.0.2".into(),
        _ => raw,
    }
}

#[tokio::test]
pub async fn test_kafka_context_works() {
    let kafka_context_result = KafkaContext::shared().await;
    let Ok(kafka_context) = kafka_context_result else {
        panic!(
            "Failed to get Kafka context: {}",
            kafka_context_result.unwrap_err()
        );
    };

    assert_ne!(
        kafka_context.bootstrap_servers.len(),
        0,
        "Bootstrap servers empty"
    );
}
