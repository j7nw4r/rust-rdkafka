use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use clap::{Arg, ArgAction, Command};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;

#[tokio::main]
async fn main() {
    let matches = Command::new("tls roundtrip example")
        .about("Produces records over TLS or mTLS and consumes them back")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .required(true),
        )
        .arg(Arg::new("topic").short('t').long("topic").required(true))
        .arg(Arg::new("ca").long("ca").required(true).help("CA PEM file"))
        .arg(
            Arg::new("cert")
                .long("cert")
                .help("Client certificate PEM file"),
        )
        .arg(
            Arg::new("key")
                .long("key")
                .help("Client private key PEM file"),
        )
        .arg(
            Arg::new("fips")
                .long("fips")
                .action(ArgAction::SetTrue)
                .help("Set ssl.fips.mode=required"),
        )
        .arg(
            Arg::new("count")
                .long("count")
                .default_value("10")
                .value_parser(clap::value_parser!(usize)),
        )
        .get_matches();

    let (_, version) = get_rdkafka_version();
    println!("librdkafka version: {}", version);

    let topic = matches.get_one::<String>("topic").unwrap();
    let count = *matches.get_one::<usize>("count").unwrap();
    let mut config = ClientConfig::new();
    config
        .set(
            "bootstrap.servers",
            matches.get_one::<String>("brokers").unwrap(),
        )
        .set("message.timeout.ms", "15000")
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", matches.get_one::<String>("ca").unwrap());
    if let Some(cert) = matches.get_one::<String>("cert") {
        config.set("ssl.certificate.location", cert);
    }
    if let Some(key) = matches.get_one::<String>("key") {
        config.set("ssl.key.location", key);
    }
    if matches.get_flag("fips") {
        config.set("ssl.fips.mode", "required");
    }

    let producer: FutureProducer = config.create().expect("Producer creation error");
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
