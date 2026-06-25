use anyhow::Context;
use rdkafka::config::FromClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;

pub async fn create_producer(bootstrap_servers: &str) -> anyhow::Result<FutureProducer> {
    create_producer_with_overrides(bootstrap_servers, &[]).await
}

pub async fn create_producer_with_overrides(
    bootstrap_servers: &str,
    config_overrides: &[(&str, &str)],
) -> anyhow::Result<FutureProducer> {
    let mut producer_client_config = ClientConfig::default();
    producer_client_config.set("bootstrap.servers", bootstrap_servers);
    for (key, value) in config_overrides {
        producer_client_config.set(*key, *value);
    }
    let future_producer = FutureProducer::from_config(&producer_client_config)
        .context("couldn't create producer client")?;
    Ok(future_producer)
}
