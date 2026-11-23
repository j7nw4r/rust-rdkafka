//! Installs a process-default rustls crypto provider, then produces records
//! over TLS or mTLS and consumes them back. Without `--brokers` and
//! `--topic`, it only creates a producer and reports the result.

use std::collections::{BTreeSet, HashMap};
use std::process;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use rdkafka_rustls_example::{provider, PROVIDERS};

const USAGE: &str = "usage: rdkafka-rustls-example --provider NAME|none --ca CA.pem \
    [--cert CERT.pem --key KEY.pem] [--fips] [--brokers HOST:PORT --topic TOPIC [--count N]]";

fn usage() -> ! {
    eprintln!("{}", USAGE);
    eprintln!("providers in this build: {:?}", PROVIDERS);
    process::exit(2)
}

fn parse_args() -> (HashMap<String, String>, bool) {
    let mut options = HashMap::new();
    let mut fips = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--fips" => fips = true,
            "--provider" | "--ca" | "--cert" | "--key" | "--brokers" | "--topic" | "--count" => {
                let value = args.next().unwrap_or_else(|| usage());
                options.insert(arg[2..].to_string(), value);
            }
            _ => usage(),
        }
    }
    (options, fips)
}

#[tokio::main]
async fn main() {
    let (options, fips) = parse_args();
    let option = |name: &str| options.get(name).map(String::as_str);

    let provider_name = option("provider").unwrap_or_else(|| usage());
    if provider_name != "none" {
        let provider = provider(provider_name).unwrap_or_else(|| usage());
        println!("provider {}: fips() = {}", provider_name, provider.fips());
        provider
            .install_default()
            .expect("a default provider is already installed");
    }
    println!("librdkafka version: {}", get_rdkafka_version().1);

    let mut config = ClientConfig::new();
    config
        .set(
            "bootstrap.servers",
            option("brokers").unwrap_or("localhost:9093"),
        )
        .set("message.timeout.ms", "15000")
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", option("ca").unwrap_or_else(|| usage()));
    if let (Some(cert), Some(key)) = (option("cert"), option("key")) {
        config
            .set("ssl.certificate.location", cert)
            .set("ssl.key.location", key);
    }
    if fips {
        config.set("ssl.fips.mode", "required");
    }

    let producer: FutureProducer = match config.create() {
        Ok(producer) => producer,
        Err(err) => {
            eprintln!("client creation refused: {}", err);
            process::exit(1);
        }
    };
    println!("client created");
    let (Some(_), Some(topic)) = (option("brokers"), option("topic")) else {
        return;
    };
    let count: usize = option("count").map_or(10, |n| n.parse().unwrap_or_else(|_| usage()));

    let mut expected = BTreeSet::new();
    for i in 0..count {
        let payload = format!("record {}", i);
        producer
            .send(
                FutureRecord::to(topic)
                    .key(&i.to_string())
                    .payload(&payload),
                Duration::from_secs(30),
            )
            .await
            .map_err(|(err, _)| err)
            .expect("Delivery failed");
        expected.insert(payload);
    }
    println!("Produced {} records to {}", count, topic);

    let consumer: BaseConsumer = config
        .clone()
        .set("group.id", format!("{}-group", topic))
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation error");
    consumer.subscribe(&[topic]).expect("Subscribe failed");

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut received = BTreeSet::new();
    while received.len() < count {
        assert!(Instant::now() < deadline, "Timed out waiting for records");
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(message)) => {
                let payload = message.payload_view::<str>().unwrap().unwrap();
                received.insert(payload.to_owned());
            }
            // Transport errors are reported here but librdkafka retries them.
            Some(Err(err)) => eprintln!("Consumer error: {}", err),
            None => {}
        }
    }
    assert_eq!(expected, received);
    println!("Consumed the same {} records back", count);
}
