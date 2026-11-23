//! Checks a TLS (or SASL) setup against a real broker.
//!
//! It applies every `-X key=value` property to each client, prints
//! `builtin.features`, and then runs one mode:
//!
//! * `roundtrip` (default): creates a new topic, produces `--count` records
//!   with distinct keys, consumes them back and compares every key and value.
//! * `metadata`: fetches cluster metadata only.
//! * `oauth`: waits until the built-in SASL OAUTHBEARER (OIDC) handler fetches
//!   a token and librdkafka accepts it. No broker is needed.
//! * `create`: creates a producer only. No broker is needed.
//! * `features`: prints `builtin.features` only. No broker is needed.
//!
//! Exit codes: 0 success, 2 usage, 3 missing feature, 4 client creation,
//! 5 metadata or topic creation, 6 delivery, 7 consume or mismatch,
//! 8 an `--expect-error` text did not appear, 9 OAUTHBEARER token.

use std::collections::{BTreeMap, BTreeSet};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Arg, ArgAction, Command};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::get_rdkafka_version;
use rdkafka::ClientContext;

/// librdkafka log lines at or below this syslog level are printed.
static LOG_LEVEL: AtomicI32 = AtomicI32::new(RDKafkaLogLevel::Warning as i32);

#[derive(Default)]
struct CheckContext {
    delivered: AtomicUsize,
    failed: AtomicUsize,
    first_failure: Mutex<Option<String>>,
    token_set: AtomicBool,
    token_error: Mutex<Option<String>>,
}

impl ClientContext for CheckContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        // librdkafka logs this (debug=security) after it accepts a token.
        if log_message.contains("after setting OAUTHBEARER token") {
            self.token_set.store(true, Ordering::SeqCst);
        }
        if (level as i32) <= LOG_LEVEL.load(Ordering::Relaxed) {
            eprintln!("librdkafka {:?} {}: {}", level, fac, log_message);
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        eprintln!("librdkafka error: {}: {}", error, reason);
        if reason.contains("OAUTHBEARER token") {
            let mut first = self.token_error.lock().unwrap();
            first.get_or_insert_with(|| reason.to_string());
        }
    }
}

impl ConsumerContext for CheckContext {}

impl ProducerContext for CheckContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
        match result {
            Ok(_) => {
                self.delivered.fetch_add(1, Ordering::SeqCst);
            }
            Err((err, _)) => {
                self.failed.fetch_add(1, Ordering::SeqCst);
                let mut first = self.first_failure.lock().unwrap();
                first.get_or_insert_with(|| err.to_string());
            }
        }
    }
}

struct Check {
    config: ClientConfig,
    timeout: Duration,
    expect_error: Option<String>,
}

impl Check {
    /// Exits with `code`, unless `--expect-error` matches the message.
    fn fail(&self, code: i32, message: String) -> ! {
        if let Some(expected) = &self.expect_error {
            if message.contains(expected.as_str()) {
                println!("PASS: expected error: {}", message);
                process::exit(0);
            }
            eprintln!("FAIL: expected an error with {:?}", expected);
        }
        eprintln!("FAIL: {}", message);
        process::exit(code)
    }

    fn succeeded(&self, what: &str) {
        if let Some(expected) = &self.expect_error {
            eprintln!(
                "FAIL: {} succeeded, expected an error with {:?}",
                what, expected
            );
            process::exit(8);
        }
    }

    fn client<T: rdkafka::config::FromClientConfigAndContext<CheckContext>>(
        &self,
        extra: &[(&str, &str)],
    ) -> T {
        let mut config = self.config.clone();
        for (key, value) in extra {
            config.set(*key, *value);
        }
        // rust-rdkafka otherwise sets the level from the `log` crate, which
        // has no logger here. CheckContext::log filters by --log-level.
        config.set_log_level(RDKafkaLogLevel::Debug);
        config
            .create_with_context(CheckContext::default())
            .unwrap_or_else(|err| self.fail(4, format!("client creation: {}", err)))
    }

    fn metadata(&self) {
        let producer: BaseProducer<CheckContext> = self.client(&[]);
        match producer.client().fetch_metadata(None, self.timeout) {
            Ok(metadata) => {
                println!(
                    "metadata: {} broker(s), {} topic(s)",
                    metadata.brokers().len(),
                    metadata.topics().len()
                );
            }
            Err(err) => {
                // Serve queued error events, which carry the TLS or SASL reason.
                producer.poll(Duration::from_secs(1));
                self.fail(5, format!("metadata: {}", err))
            }
        }
    }

    fn oauth(&self) {
        let debug = self.config.get("debug").unwrap_or("security").to_string();
        let producer: BaseProducer<CheckContext> = self.client(&[("debug", debug.as_str())]);
        let deadline = Instant::now() + self.timeout;
        // Polling serves the built-in token refresh. The refresh runs inside
        // poll and can block it for the whole HTTP request.
        loop {
            let expired = Instant::now() >= deadline;
            producer.poll(Duration::from_millis(200));
            if let Some(reason) = producer.context().token_error.lock().unwrap().clone() {
                self.fail(9, format!("oauth: {}", reason));
            }
            if producer.context().token_set.load(Ordering::SeqCst) {
                println!("OAUTHBEARER token set");
                return;
            }
            if expired {
                self.fail(
                    9,
                    format!("oauth: no OAUTHBEARER token after {:?}", self.timeout),
                );
            }
        }
    }

    fn create_topic(&self, topic: &str) {
        let admin: AdminClient<CheckContext> = self.client(&[]);
        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(-1));
        let options = AdminOptions::new().operation_timeout(Some(self.timeout));
        let results = futures::executor::block_on(admin.create_topics([&new_topic], &options))
            .unwrap_or_else(|err| self.fail(5, format!("topic creation: {}", err)));
        for result in results {
            if let Err((name, code)) = result {
                self.fail(5, format!("topic creation of {}: {}", name, code));
            }
        }
        println!("created topic {}", topic);
    }

    fn produce(&self, topic: &str, records: &BTreeMap<String, String>) {
        let timeout_ms = self.timeout.as_millis().to_string();
        let producer: BaseProducer<CheckContext> =
            self.client(&[("message.timeout.ms", timeout_ms.as_str())]);
        for (key, value) in records {
            let mut record = BaseRecord::to(topic).key(key).payload(value);
            loop {
                match producer.send(record) {
                    Ok(()) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                        producer.poll(Duration::from_millis(100));
                        record = returned;
                    }
                    Err((err, _)) => self.fail(6, format!("produce: {}", err)),
                }
            }
        }
        // message.timeout.ms bounds each record, so this flush ends in time.
        if let Err(err) = producer.flush(self.timeout + Duration::from_secs(5)) {
            self.fail(6, format!("flush: {}", err));
        }
        let context = producer.context();
        let delivered = context.delivered.load(Ordering::SeqCst);
        let failed = context.failed.load(Ordering::SeqCst);
        if delivered != records.len() || failed != 0 {
            let first = context.first_failure.lock().unwrap().clone();
            self.fail(
                6,
                format!(
                    "delivery: {} of {} delivered, {} failed, first failure: {}",
                    delivered,
                    records.len(),
                    failed,
                    first.unwrap_or_else(|| "none".into())
                ),
            );
        }
        println!("produced {} records to {}", delivered, topic);
    }

    fn consume(&self, topic: &str, records: &BTreeMap<String, String>) {
        let group = format!("{}-group", topic);
        let consumer: BaseConsumer<CheckContext> = self.client(&[
            ("group.id", group.as_str()),
            ("enable.auto.commit", "false"),
        ]);
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(topic, 0, Offset::Beginning)
            .unwrap();
        consumer
            .assign(&assignment)
            .unwrap_or_else(|err| self.fail(7, format!("assign: {}", err)));

        let deadline = Instant::now() + self.timeout;
        let mut received = BTreeSet::new();
        while received.len() < records.len() {
            if Instant::now() >= deadline {
                self.fail(
                    7,
                    format!(
                        "consume timed out: {} of {} records received",
                        received.len(),
                        records.len()
                    ),
                );
            }
            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(message)) => {
                    let key = String::from_utf8_lossy(message.key().unwrap_or_default());
                    let value = String::from_utf8_lossy(message.payload().unwrap_or_default());
                    match records.get(key.as_ref()) {
                        Some(expected) if *expected == value => {}
                        Some(_) => self.fail(7, format!("wrong value for key {}", key)),
                        None => self.fail(7, format!("unexpected key {}", key)),
                    }
                    if !received.insert(key.into_owned()) {
                        self.fail(7, "duplicate record".into());
                    }
                }
                // librdkafka retries transport errors itself.
                Some(Err(err)) => eprintln!("consumer error: {}", err),
                None => {}
            }
        }
        println!("consumed and matched {} records", received.len());
    }
}

fn main() {
    let matches = Command::new("tls_check")
        .about("Checks a TLS or SASL setup with a produce/consume roundtrip")
        .arg(
            Arg::new("property")
                .short('X')
                .value_name("KEY=VALUE")
                .action(ArgAction::Append)
                .help("librdkafka property for every client"),
        )
        .arg(
            Arg::new("mode")
                .long("mode")
                .value_parser(["roundtrip", "metadata", "oauth", "create", "features"])
                .default_value("roundtrip"),
        )
        .arg(
            Arg::new("count")
                .long("count")
                .value_parser(clap::value_parser!(usize))
                .default_value("100"),
        )
        .arg(
            Arg::new("topic")
                .long("topic")
                .help("New topic to create (default: a unique name)"),
        )
        .arg(
            Arg::new("timeout")
                .long("timeout")
                .value_parser(clap::value_parser!(u64))
                .default_value("30")
                .help("Seconds for each phase"),
        )
        .arg(
            Arg::new("log-level")
                .long("log-level")
                .value_parser(clap::value_parser!(i32))
                .default_value("4")
                .help("Print librdkafka logs at or below this syslog level (7 is debug)"),
        )
        .arg(
            Arg::new("expect-features")
                .long("expect-features")
                .value_delimiter(',')
                .help("Fail unless builtin.features has each of these"),
        )
        .arg(
            Arg::new("expect-error")
                .long("expect-error")
                .help("Pass only if the mode fails with an error that contains this text"),
        )
        .get_matches();

    LOG_LEVEL.store(
        *matches.get_one::<i32>("log-level").unwrap(),
        Ordering::Relaxed,
    );
    let mut config = ClientConfig::new();
    for property in matches.get_many::<String>("property").unwrap_or_default() {
        let Some((key, value)) = property.split_once('=') else {
            eprintln!("FAIL: -X needs key=value, got {:?}", property);
            process::exit(2);
        };
        config.set(key, value);
    }
    let check = Check {
        config,
        timeout: Duration::from_secs(*matches.get_one::<u64>("timeout").unwrap()),
        expect_error: matches.get_one::<String>("expect-error").cloned(),
    };

    println!("librdkafka version: {}", get_rdkafka_version().1);
    let features = check
        .config
        .create_native_config()
        .and_then(|native| native.get("builtin.features"))
        .unwrap_or_else(|err| check.fail(4, format!("configuration: {}", err)));
    println!("builtin.features: {}", features);
    let have: Vec<&str> = features.split(',').collect();
    for wanted in matches
        .get_many::<String>("expect-features")
        .unwrap_or_default()
    {
        if !have.contains(&wanted.as_str()) {
            eprintln!("FAIL: builtin.features has no {}", wanted);
            process::exit(3);
        }
    }

    match matches.get_one::<String>("mode").unwrap().as_str() {
        "features" => {}
        "create" => {
            let producer: BaseProducer<CheckContext> = check.client(&[]);
            // Logs go to the main queue, so serve the ones from creation.
            producer.poll(Duration::from_millis(500));
            check.succeeded("client creation");
            println!("client created");
        }
        "metadata" => {
            check.metadata();
            check.succeeded("metadata");
        }
        "oauth" => {
            check.oauth();
            check.succeeded("oauth");
        }
        _ => {
            let topic = matches
                .get_one::<String>("topic")
                .cloned()
                .unwrap_or_else(|| {
                    let millis = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    format!("tls-check-{}-{}", millis, process::id())
                });
            let count = *matches.get_one::<usize>("count").unwrap();
            let records: BTreeMap<String, String> = (0..count)
                .map(|i| (format!("key-{:06}", i), format!("value-{}-{}", i, topic)))
                .collect();
            check.metadata();
            check.create_topic(&topic);
            check.produce(&topic, &records);
            check.consume(&topic, &records);
            check.succeeded("roundtrip");
        }
    }
    println!("PASS");
}
