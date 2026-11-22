//! Client creation with `ssl.fips.mode=required`. No broker is needed.
#![cfg(feature = "rustls")]

use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::producer::BaseProducer;

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

fn required_mode_producer() -> KafkaResult<BaseProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9093")
        .set("security.protocol", "ssl")
        .set("ssl.fips.mode", "required")
        .set("ssl.ca.pem", TEST_CA)
        .create()
}

#[cfg(feature = "rustls-fips")]
#[test]
fn fips_build_initializes_required_mode() {
    required_mode_producer().expect("required mode must initialize with a FIPS rustls-ffi");
}

#[cfg(not(feature = "rustls-fips"))]
#[test]
fn ordinary_rustls_build_refuses_required_mode() {
    let err = required_mode_producer()
        .expect_err("required mode must be refused without a FIPS rustls-ffi");
    assert!(
        err.to_string().contains("ssl.fips.mode=required"),
        "unexpected error: {}",
        err
    );
}
