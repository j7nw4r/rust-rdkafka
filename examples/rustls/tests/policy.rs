//! Client creation with and without a process-default rustls provider. No
//! broker is needed.

use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::producer::BaseProducer;
use rdkafka_rustls_example::{provider, PROVIDERS};

// Test-only credentials made for this test (RSA 2048, PKCS#8 key). Loading
// the client key covers the certificate/key match in rd_kafka_new.
const TEST_CA: &str = include_str!("fixtures/ca.crt");
const CLIENT_CERT: &str = include_str!("fixtures/client.crt");
const CLIENT_KEY: &str = include_str!("fixtures/client.key");

fn producer(fips: bool) -> KafkaResult<BaseProducer> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "localhost:9093")
        .set("security.protocol", "ssl")
        .set("ssl.ca.pem", TEST_CA)
        .set("ssl.certificate.pem", CLIENT_CERT)
        .set("ssl.key.pem", CLIENT_KEY);
    if fips {
        config.set("ssl.fips.mode", "required");
    }
    config.create()
}

// One process has one provider registry, so the steps run in order in one
// test. RDKAFKA_TEST_PROVIDER selects the provider; the default is the first
// one in PROVIDERS.
#[test]
fn provider_policy() {
    let err = producer(false).expect_err("a TLS client must be refused without a provider");
    println!("no provider: {}", err);

    let name = std::env::var("RDKAFKA_TEST_PROVIDER")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| PROVIDERS[0].into());
    let provider = provider(&name).expect("the provider is not in this build");
    let fips = provider.fips();
    let empty = provider.cipher_suites.is_empty();
    println!("provider {}: fips() = {}, empty = {}", name, fips, empty);
    provider.install_default().unwrap();

    // cng-fips is empty when Windows does not run in FIPS mode.
    if empty {
        let err = producer(false).expect_err("an empty provider must be refused");
        assert!(err.to_string().contains("has no cipher suites"), "{}", err);
        return;
    }

    producer(false).expect("an ordinary TLS client must be created");
    let required = producer(true);
    println!("required mode: {:?}", required.as_ref().err());
    assert_eq!(required.is_ok(), fips);
    match name.as_str() {
        "ring" => assert!(!fips),
        "aws-lc-rs-fips" => assert!(fips),
        _ => {}
    }
}
