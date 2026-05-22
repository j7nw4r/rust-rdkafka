#![allow(dead_code)]

pub mod admin;
pub mod consumer;
pub mod containers;
pub mod logging;
pub mod producer;
pub mod rand;
pub mod topics;

use std::collections::HashMap;

use regex::Regex;

use crate::utils::containers::KafkaContext;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::statistics::Statistics;
use rdkafka::TopicPartitionList;

pub const BROKER_ID: i32 = 1;

pub fn get_broker_version(kafka_context: &KafkaContext) -> KafkaVersion {
    let regex = Regex::new(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:\.(\d+))?$").unwrap();
    match regex.captures(&kafka_context.version) {
        Some(captures) => {
            let extract = |i| {
                captures
                    .get(i)
                    .map(|m| m.as_str().parse().unwrap())
                    .unwrap_or(0)
            };
            KafkaVersion(extract(1), extract(2), extract(3), extract(4))
        }
        None => panic!("KAFKA_VERSION env var was not in expected [n[.n[.n[.n]]]] format"),
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct KafkaVersion(pub u32, pub u32, pub u32, pub u32);

pub async fn produce_messages_with_timestamp(
    producer: &FutureProducer,
    topic_name: &str,
    count: usize,
    partition: i32,
    timestamp: i64,
) -> HashMap<(i32, i64), i32> {
    produce_messages(
        producer,
        topic_name,
        count,
        Some(partition),
        Some(timestamp),
    )
    .await
}

pub async fn produce_messages_to_partition(
    producer: &FutureProducer,
    topic_name: &str,
    count: usize,
    partition: i32,
) -> HashMap<(i32, i64), i32> {
    produce_messages(producer, topic_name, count, Some(partition), None).await
}

pub async fn produce_messages(
    producer: &FutureProducer,
    topic_name: &str,
    count: usize,
    partition: Option<i32>,
    timestamp: Option<i64>,
) -> HashMap<(i32, i64), i32> {
    let mut inflight = Vec::with_capacity(count);

    for idx in 0..count {
        let id = idx as i32;
        let payload = value_fn(id);
        let key = key_fn(id);
        let mut record = FutureRecord::to(topic_name).payload(&payload).key(&key);
        if let Some(partition) = partition {
            record = record.partition(partition);
        }
        if let Some(timestamp) = timestamp {
            record = record.timestamp(timestamp);
        }
        let delivery_future = producer
            .send_result(record)
            .expect("failed to enqueue message");
        inflight.push((id, payload, key, delivery_future));
    }

    let mut message_map = HashMap::new();

    for (id, _payload, _key, delivery_future) in inflight {
        match delivery_future
            .await
            .expect("producer unexpectedly dropped")
        {
            Ok(delivery) => {
                message_map.insert((delivery.partition, delivery.offset), id);
            }
            Err((error, _message)) => panic!("Delivery failed: {}", error),
        };
    }

    message_map
}

pub fn value_fn(id: i32) -> String {
    format!("Message {}", id)
}

pub fn key_fn(id: i32) -> String {
    format!("Key {}", id)
}

pub struct ConsumerTestContext {
    pub _n: i64, // Add data for memory access validation
}

impl ClientContext for ConsumerTestContext {
    // Access stats
    fn stats(&self, stats: Statistics) {
        let stats_str = format!("{:?}", stats);
        println!("Stats received: {} bytes", stats_str.len());
    }
}

impl ConsumerContext for ConsumerTestContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

pub fn consumer_config(
    bootstrap_servers: &str,
    group_id: &str,
    config_overrides: Option<HashMap<&str, &str>>,
) -> ClientConfig {
    let mut config = ClientConfig::new();

    config.set("group.id", group_id);
    config.set("client.id", "rdkafka_integration_test_client");
    config.set("bootstrap.servers", bootstrap_servers);
    config.set("enable.partition.eof", "false");
    config.set("session.timeout.ms", "6000");
    config.set("enable.auto.commit", "false");
    config.set("debug", "all");
    config.set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(key, value);
        }
    }

    config
}
