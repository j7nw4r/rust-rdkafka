//! Test transactions using the base consumer and producer.

use std::error::Error;
use std::time::Duration;

use log::info;

use rdkafka::admin::AdminOptions;
use rdkafka::config::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::utils::admin;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer;
use crate::utils::rand::*;
use crate::utils::*;

mod utils;

async fn create_consumer(
    kafka_context: &KafkaContext,
    config_overrides: Option<&[(&str, &str)]>,
) -> Result<BaseConsumer<ConsumerTestContext>, KafkaError> {
    init_test_logger();
    let group_id = rand_test_group();
    let mut config = ClientConfig::new();
    config
        .set("group.id", &group_id)
        .set("enable.partition.eof", "false")
        .set("client.id", "rdkafka_integration_test_client")
        .set("bootstrap.servers", &kafka_context.bootstrap_servers)
        .set("session.timeout.ms", "6000")
        .set("debug", "all")
        .set("auto.offset.reset", "earliest");

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.set(*key, *value);
        }
    }

    config.create_with_context(ConsumerTestContext { _n: 64 })
}

fn create_producer(kafka_context: &KafkaContext) -> Result<BaseProducer, KafkaError> {
    init_test_logger();
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &kafka_context.bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", rand_test_transactional_id())
        .set("debug", "eos");
    config.set_log_level(RDKafkaLogLevel::Debug);
    config.create()
}

enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

async fn count_records(
    kafka_context: &KafkaContext,
    topic: &str,
    iso: IsolationLevel,
) -> Result<usize, KafkaError> {
    let isolation = match iso {
        IsolationLevel::ReadUncommitted => "read_uncommitted",
        IsolationLevel::ReadCommitted => "read_committed",
    };

    let consumer = create_consumer(
        kafka_context,
        Some(&[
            ("isolation.level", isolation),
            ("enable.partition.eof", "true"),
        ]),
    )
    .await?;

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    consumer.assign(&tpl)?;
    let mut count = 0;
    for message in consumer.iter() {
        match message {
            Ok(_) => count += 1,
            Err(KafkaError::PartitionEOF(_)) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(count)
}

#[tokio::test]
async fn test_transaction_abort() -> Result<(), Box<dyn Error>> {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let consume_topic = rand_test_topic("test_transaction_abort");
    let produce_topic = rand_test_topic("test_transaction_abort");

    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&consume_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create consume topic");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&produce_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create produce topic");

    let future_producer =
        producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
            .await
            .expect("Could not create Future producer");
    let _ = produce_messages_to_partition(&future_producer, &consume_topic, 30, 0).await;

    // Create consumer and subscribe to `consume_topic`.
    let consumer = create_consumer(&kafka_context, None).await?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    // Commit the first 10 messages.
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    let producer = create_producer(&kafka_context)?;
    producer.init_transactions(Timeout::Never)?;
    producer.begin_transaction()?;

    // Tie the commit of offset 20 to the transaction.
    let cgm = consumer.group_metadata().unwrap();
    let mut txn_tpl = TopicPartitionList::new();
    txn_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(20))?;
    producer.send_offsets_to_transaction(&txn_tpl, &cgm, Timeout::Never)?;

    // Produce 10 records in the transaction.
    for _ in 0..10 {
        producer
            .send(
                BaseRecord::to(&produce_topic)
                    .payload("A")
                    .key("B")
                    .partition(0),
            )
            .unwrap();
    }

    // Abort the transaction, but only after producing all messages.
    info!("BEFORE FLUSH");
    producer.flush(Duration::from_secs(20))?;
    info!("AFTER FLUSH");
    producer.abort_transaction(Duration::from_secs(20))?;
    info!("AFTER ABORT");

    // Check that no records were produced in read committed mode, but that
    // the records are visible in read uncommitted mode.
    assert_eq!(
        count_records(
            &kafka_context,
            &produce_topic,
            IsolationLevel::ReadCommitted
        )
        .await?,
        0,
    );
    assert_eq!(
        count_records(
            &kafka_context,
            &produce_topic,
            IsolationLevel::ReadUncommitted
        )
        .await?,
        10,
    );

    // Check that the consumer's committed offset is still 10.
    let committed = consumer.committed(Timeout::Never)?;
    assert_eq!(
        committed
            .find_partition(&consume_topic, 0)
            .unwrap()
            .offset(),
        Offset::Offset(10)
    );

    Ok(())
}

#[tokio::test]
async fn test_transaction_commit() -> Result<(), Box<dyn Error>> {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let consume_topic = rand_test_topic("test_transaction_commit");
    let produce_topic = rand_test_topic("test_transaction_commit");

    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("Could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&consume_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create consume topic");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&produce_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create produce topic");

    let future_producer =
        producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
            .await
            .expect("Could not create Future producer");
    let _ = produce_messages_to_partition(&future_producer, &consume_topic, 30, 0).await;

    // Create consumer and subscribe to `consume_topic`.
    let consumer = create_consumer(&kafka_context, None).await?;
    consumer.subscribe(&[&consume_topic])?;
    consumer.poll(Timeout::Never).unwrap()?;

    // Commit the first 10 messages.
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(10))?;
    consumer.commit(&commit_tpl, CommitMode::Sync).unwrap();

    // Create a producer and start a transaction.
    let producer = create_producer(&kafka_context)?;
    producer.init_transactions(Timeout::Never)?;
    producer.begin_transaction()?;

    // Tie the commit of offset 20 to the transaction.
    let cgm = consumer.group_metadata().unwrap();
    let mut txn_tpl = TopicPartitionList::new();
    txn_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(20))?;
    producer.send_offsets_to_transaction(&txn_tpl, &cgm, Timeout::Never)?;

    // Produce 10 records in the transaction.
    for _ in 0..10 {
        producer
            .send(
                BaseRecord::to(&produce_topic)
                    .payload("A")
                    .key("B")
                    .partition(0),
            )
            .unwrap();
    }

    // Commit the transaction.
    producer.commit_transaction(Timeout::Never)?;

    // Check that 10 records were produced.
    assert_eq!(
        count_records(
            &kafka_context,
            &produce_topic,
            IsolationLevel::ReadUncommitted
        )
        .await?,
        10,
    );
    assert_eq!(
        count_records(
            &kafka_context,
            &produce_topic,
            IsolationLevel::ReadCommitted
        )
        .await?,
        10,
    );

    // Check that the consumer's committed offset is now 20.
    let committed = consumer.committed(Timeout::Never)?;
    assert_eq!(
        committed
            .find_partition(&consume_topic, 0)
            .unwrap()
            .offset(),
        Offset::Offset(20)
    );

    Ok(())
}

fn create_producer_with_txn_id(
    kafka_context: &KafkaContext,
    transactional_id: &str,
) -> Result<BaseProducer, KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &kafka_context.bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("enable.idempotence", "true")
        .set("transactional.id", transactional_id);
    config.create()
}

// When two producers share a `transactional.id`, the broker fences the older
// epoch on the second producer's `init_transactions`. The fenced producer's
// next transactional API call must surface that fact rather than silently
// committing. This test sets up two producers with the same transactional
// id, drives the second through `init_transactions` (which fences the
// first), and asserts the first's `commit_transaction` returns a
// `KafkaError::Transaction` whose underlying code is one of the librdkafka
// fencing codes. A binding regression that hid the fencing error or
// pretended the commit succeeded would surface here.
#[tokio::test]
async fn test_transaction_producer_fenced_by_epoch() -> Result<(), Box<dyn Error>> {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_txn_fencing");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let txn_id = rand_test_transactional_id();

    let first = create_producer_with_txn_id(&kafka_context, &txn_id)?;
    first.init_transactions(Timeout::Never)?;
    first.begin_transaction()?;
    first
        .send(
            BaseRecord::to(&topic)
                .payload("first-pre-fence")
                .key("k")
                .partition(0),
        )
        .map_err(|(e, _)| e)?;
    first.flush(Duration::from_secs(20))?;

    let second = create_producer_with_txn_id(&kafka_context, &txn_id)?;
    second.init_transactions(Timeout::Never)?;
    drop(second);

    let result = first.commit_transaction(Duration::from_secs(20));
    match result {
        Ok(()) => panic!("commit_transaction unexpectedly succeeded after the producer was fenced"),
        Err(KafkaError::Transaction(rd_err)) => {
            let code = rd_err.code();
            assert!(
                matches!(
                    code,
                    RDKafkaErrorCode::Fenced
                        | RDKafkaErrorCode::InvalidProducerEpoch
                        | RDKafkaErrorCode::ProducerFenced
                ),
                "expected a producer-fencing error code, got {:?} ({})",
                code,
                rd_err.string(),
            );
        }
        Err(other) => panic!("unexpected error variant: {:?}", other),
    }

    Ok(())
}

// `test_transaction_abort` and `test_transaction_commit` each cover a single
// producer running a single transaction, but they never interleave committed
// and aborted records on the same topic-partition. This test mixes the two:
// one transactional producer commits 7 records, a second transactional
// producer (different `transactional.id`) aborts 5 records on the same topic.
// A `read_committed` consumer must observe exactly the 7 committed payloads
// (the broker writes a control marker that the consumer-side filter must
// honour); a `read_uncommitted` consumer must observe all 12 payloads. A
// binding regression on the `isolation.level` plumbing, or a librdkafka
// filter break, would show up as the wrong total or the wrong payload set.
#[tokio::test]
async fn test_transaction_isolation_level_filters_aborted() -> Result<(), Box<dyn Error>> {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_txn_isolation_filter");
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    let committed_producer = create_producer(&kafka_context)?;
    committed_producer.init_transactions(Timeout::Never)?;
    committed_producer.begin_transaction()?;
    let committed_payloads: Vec<String> = (0..7).map(|i| format!("committed-{}", i)).collect();
    for payload in &committed_payloads {
        committed_producer
            .send(
                BaseRecord::to(&topic)
                    .payload(payload.as_str())
                    .key("k")
                    .partition(0),
            )
            .map_err(|(e, _)| e)?;
    }
    committed_producer.flush(Duration::from_secs(20))?;
    committed_producer.commit_transaction(Duration::from_secs(20))?;

    let aborted_producer = create_producer(&kafka_context)?;
    aborted_producer.init_transactions(Timeout::Never)?;
    aborted_producer.begin_transaction()?;
    let aborted_payloads: Vec<String> = (0..5).map(|i| format!("aborted-{}", i)).collect();
    for payload in &aborted_payloads {
        aborted_producer
            .send(
                BaseRecord::to(&topic)
                    .payload(payload.as_str())
                    .key("k")
                    .partition(0),
            )
            .map_err(|(e, _)| e)?;
    }
    aborted_producer.flush(Duration::from_secs(20))?;
    aborted_producer.abort_transaction(Duration::from_secs(20))?;

    let collect_payloads = |iso: IsolationLevel| {
        let iso_str = match iso {
            IsolationLevel::ReadCommitted => "read_committed",
            IsolationLevel::ReadUncommitted => "read_uncommitted",
        };
        let kafka_context = kafka_context.clone();
        let topic = topic.clone();
        async move {
            let consumer = create_consumer(
                &kafka_context,
                Some(&[
                    ("isolation.level", iso_str),
                    ("enable.partition.eof", "true"),
                ]),
            )
            .await?;
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&topic, 0);
            consumer.assign(&tpl)?;
            let mut payloads = Vec::new();
            for message in consumer.iter() {
                match message {
                    Ok(m) => payloads.push(m.payload_view::<str>().unwrap().unwrap().to_string()),
                    Err(KafkaError::PartitionEOF(_)) => break,
                    Err(e) => return Err(e),
                }
            }
            Ok::<_, KafkaError>(payloads)
        }
    };

    let committed_view = collect_payloads(IsolationLevel::ReadCommitted).await?;
    assert_eq!(
        committed_view, committed_payloads,
        "read_committed should see only the committed payloads in order"
    );

    let uncommitted_view = collect_payloads(IsolationLevel::ReadUncommitted).await?;
    let mut expected_uncommitted = committed_payloads.clone();
    expected_uncommitted.extend(aborted_payloads.iter().cloned());
    assert_eq!(
        uncommitted_view, expected_uncommitted,
        "read_uncommitted should see committed payloads followed by aborted payloads"
    );

    Ok(())
}

// `test_transaction_commit` already calls `send_offsets_to_transaction`, but
// it produces a fixed "A" payload regardless of what was consumed and only
// looks at offsets at the end. This test does a full consume-transform-produce
// loop in two transactions: each transaction reads a batch from the input
// topic, transforms each payload, produces the transformed payload to the
// output topic, ties the consumer's offset advance to the transaction with
// `send_offsets_to_transaction`, and commits. The assertion shape catches a
// binding regression where the transformed payloads diverge from the input or
// where `send_offsets_to_transaction` fails to advance the consumer-side
// committed offset in step with the transaction.
#[tokio::test]
async fn test_transaction_send_offsets_consume_transform_produce() -> Result<(), Box<dyn Error>> {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let consume_topic = rand_test_topic("test_txn_ctp_in");
    let produce_topic = rand_test_topic("test_txn_ctp_out");

    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&consume_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create consume topic");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&produce_topic, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create produce topic");

    let future_producer =
        producer::future_producer::create_producer(&kafka_context.bootstrap_servers)
            .await
            .expect("could not create future producer");
    const BATCHES: usize = 2;
    const PER_BATCH: usize = 5;
    const TOTAL: usize = BATCHES * PER_BATCH;
    let _ = produce_messages_to_partition(&future_producer, &consume_topic, TOTAL, 0).await;

    let consumer = create_consumer(&kafka_context, None).await?;
    consumer.subscribe(&[&consume_topic])?;

    let producer = create_producer(&kafka_context)?;
    producer.init_transactions(Timeout::Never)?;

    let mut expected_outputs: Vec<String> = Vec::with_capacity(TOTAL);
    let mut consumed = 0usize;
    for batch in 0..BATCHES {
        producer.begin_transaction()?;

        let mut batch_payloads: Vec<String> = Vec::with_capacity(PER_BATCH);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while batch_payloads.len() < PER_BATCH {
            if std::time::Instant::now() > deadline {
                panic!(
                    "consumer poll timed out in batch {} after {} messages",
                    batch,
                    batch_payloads.len()
                );
            }
            let Some(message) = consumer.poll(Duration::from_secs(2)) else {
                continue;
            };
            let message = message?;
            let payload = message.payload_view::<str>().unwrap().unwrap().to_string();
            batch_payloads.push(payload);
            consumed += 1;
        }

        for payload in &batch_payloads {
            let transformed = format!("transformed:{}", payload);
            expected_outputs.push(transformed.clone());
            producer
                .send(
                    BaseRecord::to(&produce_topic)
                        .payload(&transformed)
                        .key("k")
                        .partition(0),
                )
                .map_err(|(e, _)| e)?;
        }

        let cgm = consumer.group_metadata().unwrap();
        let mut txn_tpl = TopicPartitionList::new();
        txn_tpl.add_partition_offset(&consume_topic, 0, Offset::Offset(consumed as i64))?;
        producer.send_offsets_to_transaction(&txn_tpl, &cgm, Timeout::Never)?;

        producer.flush(Duration::from_secs(20))?;
        producer.commit_transaction(Duration::from_secs(20))?;
    }

    let committed = consumer.committed(Timeout::Never)?;
    assert_eq!(
        committed
            .find_partition(&consume_topic, 0)
            .unwrap()
            .offset(),
        Offset::Offset(TOTAL as i64),
        "consumer-side committed offset must equal the number of consumed messages",
    );

    let output_consumer = create_consumer(
        &kafka_context,
        Some(&[
            ("isolation.level", "read_committed"),
            ("enable.partition.eof", "true"),
        ]),
    )
    .await?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(&produce_topic, 0);
    output_consumer.assign(&tpl)?;
    let mut output_payloads = Vec::with_capacity(TOTAL);
    for message in output_consumer.iter() {
        match message {
            Ok(m) => {
                let payload = m.payload_view::<str>().unwrap().unwrap().to_string();
                output_payloads.push(payload);
            }
            Err(KafkaError::PartitionEOF(_)) => break,
            Err(e) => return Err(e.into()),
        }
    }
    assert_eq!(
        output_payloads, expected_outputs,
        "output topic payloads must match the transformed inputs in order",
    );

    Ok(())
}
