//! Client creation with and without a process-default rustls provider. No
//! broker is needed.

use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::producer::BaseProducer;
use rdkafka_rustls_example::{provider, PROVIDERS};

// A self-signed CA whose key was discarded. The client only parses it.
const TEST_CA: &str = "\
-----BEGIN CERTIFICATE-----
MIIDMzCCAhugAwIBAgIURaIGpyNYJ4fcViNnpNemBHRyT3EwDQYJKoZIhvcNAQEL
BQAwKDEmMCQGA1UEAwwdcmRrYWZrYSBydXN0bHMgcG9saWN5IHRlc3QgQ0EwIBcN
MjYxMTIyMjE1NTMyWhgPMjEyNjEwMjkyMTU1MzJaMCgxJjAkBgNVBAMMHXJka2Fm
a2EgcnVzdGxzIHBvbGljeSB0ZXN0IENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEAw6EdGHXZ5qNaOw8DR4Jq3CswsPadHRGI++288GTVngOWREqjSq3B
kDzzKuBcAzEsquNgy+4YrhyTFVcndG1sl6sfFPiLCWYRMeCEFLJZlJEDsZmqnGQC
x+8XndrqOO3AQoZkf9I/KxNfUEA3PNF1gzUuZrgz5wikfK/KMNLzY5lisQ9Iqtss
4ps79dKZ24fHDRSqf18FK4LwVXYhYnIZ5EwhBF7+q/+yZBXQ8QCPBsF0eLx381Xj
yBHxuv78s95iNMMRBLnqxP7Ilvqz2toQZINHQxg1W3KkRpDhUtlfdQXKG43cPQK/
vVRoNPpedEwFQAS4BuksE4ntHcuRaOQMMwIDAQABo1MwUTAdBgNVHQ4EFgQUZD8/
W+6EMe3FAROrjnh0v3ExKuMwHwYDVR0jBBgwFoAUZD8/W+6EMe3FAROrjnh0v3Ex
KuMwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAVkt7/iXqMirs
861svVpQHoxhXK32bdyfiwpzDO4dZD6P4bbo8hCXVfVCFBAXmcHbrihtOJCuNR28
x+ThK2jyQmYs3yVA79ohrS+VqU8nqIW/pacfk7kzSlYP6Fo2Xu5D5KrG0lG7DDSc
QMEHffjB3CgjhbsIX14NuNV7aZM3b4Xh3b14Hwmi4i3EUVoefjXQpAtiLc5FhC6E
MRKujpGwT7YUONu9fITWiCBfXc+W5zOire/ID7tnrICD/INirX4J4omNJvPtNRry
SvBjDIZsYGq5uOFrpL2taMV11W3vmC+SRyOVylntg+Z8auEhusG8NRn5nAwLHXAU
7Sz+/SpM6w==
-----END CERTIFICATE-----
";

fn producer(fips: bool) -> KafkaResult<BaseProducer> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "localhost:9093")
        .set("security.protocol", "ssl")
        .set("ssl.ca.pem", TEST_CA);
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

    let name = std::env::var("RDKAFKA_TEST_PROVIDER").unwrap_or_else(|_| PROVIDERS[0].into());
    let provider = provider(&name).expect("the provider is not in this build");
    let fips = provider.fips();
    println!("provider {}: fips() = {}", name, fips);
    provider.install_default().unwrap();

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
