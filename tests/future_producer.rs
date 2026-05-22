//! Test data production using high level producers.

use std::time::{Duration, Instant};

use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};

use rdkafka::admin::AdminOptions;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::Timestamp;

use crate::utils::admin;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer;
use crate::utils::rand::*;

mod utils;

#[tokio::test]
async fn test_future_producer_send() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_future_producer_send");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let results: FuturesUnordered<_> = (0..10)
        .map(|_| {
            producer.send(
                FutureRecord::to(&topic_name).payload("A").key("B"),
                Duration::from_secs(0),
            )
        })
        .collect();

    let results: Vec<_> = results.collect().await;
    assert!(results.len() == 10);
    for (i, result) in results.into_iter().enumerate() {
        let delivered = result.unwrap();
        assert_eq!(delivered.partition, 1);
        assert_eq!(delivered.offset, i as i64);
        assert!(delivered.timestamp < Timestamp::now());
    }
}

#[tokio::test]
async fn test_future_producer_send_full() {
    // Connect to a nonexistent Kafka broker with a long message timeout and a
    // tiny producer queue, so we can fill up the queue for a while by sending a
    // single message.
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "")
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "1");
    let producer: FutureProducer<DefaultClientContext> =
        config.create().expect("Failed to create producer");
    let producer = &producer;
    let topic_name = &rand_test_topic("test_future_producer_send_full");

    // Fill up the queue.
    producer
        .send_result(FutureRecord::to(topic_name).payload("A").key("B"))
        .unwrap();

    let send_message = |timeout| async move {
        let start = Instant::now();
        let res = producer
            .send(FutureRecord::to(topic_name).payload("A").key("B"), timeout)
            .await;
        match res {
            Ok(_) => panic!("send unexpectedly succeeded"),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => start.elapsed(),
            Err((e, _)) => panic!("got incorrect error: {}", e),
        }
    };

    // Sending a message with no timeout should return a `QueueFull` error
    // approximately immediately.
    let elapsed = send_message(Duration::from_secs(0)).await;
    assert!(elapsed < Duration::from_millis(20));

    // Sending a message with a 1s timeout should return a `QueueFull` error
    // in about 1s.
    let elapsed = send_message(Duration::from_secs(1)).await;
    assert!(elapsed > Duration::from_millis(800));
    assert!(elapsed < Duration::from_millis(1200));

    producer.flush(Timeout::Never).unwrap();
}

#[tokio::test]
async fn test_future_producer_send_fail() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic("test_future_producer_send_fail");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(3)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create Future producer");

    let future = producer.send(
        FutureRecord::to(&topic_name)
            .payload("payload")
            .key("key")
            .partition(100) // Fail
            .headers(
                OwnedHeaders::new()
                    .insert(Header {
                        key: "0",
                        value: Some("A"),
                    })
                    .insert(Header {
                        key: "1",
                        value: Some("B"),
                    })
                    .insert(Header {
                        key: "2",
                        value: Some("C"),
                    }),
            ),
        Duration::from_secs(10),
    );

    match future.await {
        Err((kafka_error, owned_message)) => {
            assert_eq!(
                kafka_error.to_string(),
                "Message production error: UnknownPartition (Local: Unknown partition)"
            );
            assert_eq!(owned_message.topic(), topic_name.as_str());
            let headers = owned_message.headers().unwrap();
            assert_eq!(headers.count(), 3);
            assert_eq!(
                headers.get_as::<str>(0),
                Ok(Header {
                    key: "0",
                    value: Some("A")
                })
            );
            assert_eq!(
                headers.get_as::<str>(1),
                Ok(Header {
                    key: "1",
                    value: Some("B")
                })
            );
            assert_eq!(
                headers.get_as::<str>(2),
                Ok(Header {
                    key: "2",
                    value: Some("C")
                })
            );
        }
        e => {
            panic!("Unexpected return value: {:?}", e);
        }
    }
}

async fn run_compression_round_trip(codec: &str) {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic_name = rand_test_topic(&format!("test_compression_{}", codec));
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let producer = producer::future_producer::create_producer_with_overrides(
        &kafka_context.bootstrap_servers,
        &[("compression.type", codec), ("linger.ms", "20")],
    )
    .await
    .expect("could not create future producer");

    const N: usize = 64;
    let payload = "rust-rdkafka compression round trip ".repeat(8);
    let keys: Vec<String> = (0..N).map(|i| format!("k{}", i)).collect();
    let values: Vec<String> = (0..N).map(|i| format!("{}:{}", i, payload)).collect();
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        futures.push(
            producer.send(
                FutureRecord::to(&topic_name)
                    .partition(0)
                    .key(&keys[i])
                    .payload(&values[i]),
                Duration::from_secs(10),
            ),
        );
    }
    let mut expected = std::collections::HashMap::with_capacity(N);
    for (i, future) in futures.into_iter().enumerate() {
        let delivered = future.await.unwrap_or_else(|(e, _)| {
            panic!("delivery failed for codec {} message {}: {}", codec, i, e)
        });
        expected.insert(delivered.offset, (keys[i].clone(), values[i].clone()));
    }
    producer
        .flush(Timeout::After(Duration::from_secs(10)))
        .unwrap();

    let consumer = utils::consumer::stream_consumer::create_stream_consumer(
        &kafka_context.bootstrap_servers,
        Some(&rand_test_group()),
    )
    .await
    .expect("could not create stream consumer");
    consumer.subscribe(&[topic_name.as_str()]).unwrap();

    let mut seen = 0usize;
    consumer
        .stream()
        .take(N)
        .for_each(|message| {
            let m = message.expect("error receiving message");
            let (expected_key, expected_value) = expected
                .remove(&m.offset())
                .unwrap_or_else(|| panic!("unexpected offset {} for codec {}", m.offset(), codec));
            assert_eq!(m.key_view::<str>().unwrap().unwrap(), expected_key);
            assert_eq!(m.payload_view::<str>().unwrap().unwrap(), expected_value);
            seen += 1;
            future::ready(())
        })
        .await;
    assert_eq!(seen, N, "codec {} did not yield all messages", codec);
    assert!(
        expected.is_empty(),
        "codec {} left {} unmatched offsets",
        codec,
        expected.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_future_producer_compression_gzip() {
    run_compression_round_trip("gzip").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_future_producer_compression_snappy() {
    run_compression_round_trip("snappy").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_future_producer_compression_lz4() {
    run_compression_round_trip("lz4").await;
}

// librdkafka is built with `--disable-zstd` unless the `zstd` Cargo feature is
// enabled (see rdkafka-sys/build.rs), so this test can only run when that
// feature is on. CI exercises it by passing `--features zstd` to the test job.
#[cfg(feature = "zstd")]
#[tokio::test(flavor = "multi_thread")]
async fn test_future_producer_compression_zstd() {
    run_compression_round_trip("zstd").await;
}

#[tokio::test]
async fn test_future_undelivered() {
    let delivery_future = {
        let mut config = ClientConfig::new();
        // There's no server running there
        config
            .set("bootstrap.servers", "localhost:47021")
            .set("message.timeout.ms", "1");
        let producer: FutureProducer = config.create().expect("Failed to create producer");

        producer
            .send_result(
                FutureRecord::to("topic")
                    .payload("payload")
                    .key("key")
                    .partition(100),
            )
            .expect("Failed to queue message")

        // drop producer. This should resolve the future as per purge API (couldn't be delivered)
    };

    match delivery_future.await {
        Ok(Err((kafka_error, owned_message))) => {
            assert_eq!(
                kafka_error.to_string(),
                "Message production error: PurgeQueue (Local: Purged in queue)"
            );
            assert_eq!(owned_message.topic(), "topic");
            assert_eq!(owned_message.key(), Some(b"key" as _));
            assert_eq!(owned_message.payload(), Some(b"payload" as _));
        }
        v => {
            panic!("Unexpected return value: {:?}", v);
        }
    }
}
