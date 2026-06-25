# Integration test suite

This is a tour of how the integration tests work, aimed at someone new
to the codebase. If you just want to run them, jump to [Running the
tests](#running-the-tests). If you want to add one, read the rest.

## Overview

Each test file under `tests/` is a separate Cargo integration-test binary
(`tests/admin.rs`, `tests/base_producer.rs`, and so on). Every binary
needs a real Kafka broker to talk to.

We start that broker with [testcontainers-rs]. When the first test in a
binary calls `KafkaContext::shared()`, testcontainers pulls and starts
an `apache/kafka` Docker image, waits for it to be ready, and hands back
its bootstrap address (host plus a randomly-allocated port). Every
subsequent call in that same binary reuses the same container.

You don't need to start the broker yourself. You don't need
`docker-compose`. You don't need to set `KAFKA_HOST`. `cargo test` does
the right thing as long as Docker is running.

[testcontainers-rs]: https://github.com/testcontainers/testcontainers-rs

## Prerequisites

- **A Docker daemon** the current user can talk to (Docker Desktop,
  colima, OrbStack, native Docker, ...). Tests fail at
  `KafkaContext::shared()` if Docker isn't reachable.
- **Rust >= 1.85.** This is the MSRV the library targets, and the
  `testcontainers-modules` dep is pinned at `0.12.1` to keep us on it.
- **librdkafka build deps.** `cmake`, a C/C++ toolchain, plus
  `libcurl4-openssl-dev` on Debian/Ubuntu. See the top-level `README.md`
  for the full list.

## Running the tests

From the repository root:

```bash
cargo test
```

That builds every integration-test binary in `tests/` and runs them
sequentially. The first binary to run pays the cost of pulling the
Kafka image (one-time) and starting a container (a few seconds). Within
a binary, individual tests run in parallel against the shared broker.

To run just one file:

```bash
cargo test --test admin
```

To run a single test:

```bash
cargo test --test admin test_topic_create_and_delete
```

To pick a specific Kafka version (defaults to `4.0`):

```bash
KAFKA_VERSION=3.9 cargo test
```

`KAFKA_VERSION` accepts either a short series (`3.7`, `3.8`, `3.9`,
`4.0`) or a full tag (`3.9.2`). The mapping lives in
`tests/utils/containers.rs::resolve_kafka_image_tag`.

To enable librdkafka log output:

```bash
RUST_LOG="librdkafka=trace,rdkafka::client=debug" cargo test
```

## How the broker is managed

`tests/utils/containers.rs` defines a single `KafkaContext`:

```rust
pub struct KafkaContext {
    kafka_node: ContainerAsync<Kafka>,
    pub bootstrap_servers: String,
    pub version: String,
}
```

`KafkaContext::shared()` is the only constructor. It wraps a
`tokio::sync::OnceCell` so that the first caller in a test binary spins
the container up and every other caller gets the same `Arc<KafkaContext>`
back:

```rust
let kafka_context = KafkaContext::shared()
    .await
    .expect("could not create kafka context");
```

A few things to internalise:

- **One broker per test binary.** Cargo runs each `tests/*.rs` file as
  a separate process, so each binary gets its own container. Two tests
  in the same binary share state on the broker. Two tests in different
  binaries do not.
- **Tests in a binary run in parallel.** That's Cargo's default. If your
  test needs an isolated topic or consumer group, use the random-name
  helpers (see below). Don't hardcode names; you will collide with
  another test.
- **No fresh broker between tests.** The container lives for the whole
  binary. State you leave behind (topics, consumer groups, offsets) is
  visible to later tests in the same binary, which is occasionally what
  you want and occasionally a footgun.
- **Random host port.** `kafka_context.bootstrap_servers` is something
  like `127.0.0.1:54731`. Never assume `9092`.

The container is started with two non-default env vars:

```rust
.with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
.with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
```

Kafka defaults to replication factor 3 for `__transaction_state`. We
only have a single broker, so the transactions tests would hang forever
waiting for the internal topic to come up without these overrides. If
you ever see transactions tests hang, this is the first thing to check.

We also call `.with_jvm_image()` because the kafka-native variant of the
`apache/kafka` image doesn't publish `3.7.x` tags, and the CI matrix
needs them.

## Test file layout

```
tests/
  admin.rs                       integration tests, one file per area
  base_consumer.rs
  base_producer.rs
  consumer_groups.rs
  future_producer.rs
  metadata.rs
  producer.rs
  stream_consumers.rs
  topic_partition_lists.rs       (pure unit-style; no broker)
  transactions.rs

  utils/                         shared test helpers
    mod.rs                       message-production helpers,
                                 ConsumerTestContext, KafkaVersion,
                                 BROKER_ID (= 1, the container's hardcoded id)
    containers.rs                KafkaContext + shared() / OnceCell
    admin.rs                     create_admin_client, create_topic, new_topic_vec
    consumer/
      mod.rs                     base-consumer helpers
      stream_consumer.rs         stream-consumer helpers
    producer/
      mod.rs
      base_producer.rs           create_producer, send_record, poll_and_flush
      future_producer.rs         create_producer (FutureProducer)
    topics.rs                    populate_topic_using_future_producer
    rand.rs                      rand_test_topic / rand_test_group /
                                 rand_test_transactional_id
    logging.rs                   init_test_logger (env_logger, one-shot)
```

Each test file lives at the top of `tests/`. Helpers live under
`tests/utils/` and are pulled in with `mod utils;` at the top of each
file. The split lets Cargo treat each integration test as its own
binary while sharing utility code.

## Writing a new integration test

Start with a small example. Drop a new file at `tests/my_feature.rs`:

```rust
use rdkafka::admin::AdminOptions;

use crate::utils::admin;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::producer;
use crate::utils::rand::{rand_test_group, rand_test_topic};

mod utils;

#[tokio::test]
async fn my_feature_works() {
    init_test_logger();

    // 1. Get (or start, if we're the first test) the shared broker.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // 2. Create an admin client and a unique topic for this test.
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    let topic_name = rand_test_topic("my_feature");
    admin_client
        .create_topics(
            &admin::new_topic_vec(&topic_name, Some(1)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    // 3. Do the thing under test. Make assertions.
    let producer = producer::future_producer::create_producer(
        &kafka_context.bootstrap_servers,
    )
    .await
    .expect("could not create producer");

    // ... produce, consume, assert ...

    drop(producer);
}
```

Things to notice:

- `mod utils;` at the top is mandatory; without it the `crate::utils::*`
  paths don't resolve.
- `init_test_logger()` is a one-shot guarded by `Once`, so it's safe to
  call from every test.
- `rand_test_topic("my_feature")` returns `my_feature_aB3xQ...`, a
  test-specific topic. Always do this for topics and consumer groups,
  or you'll see flaky tests when binaries are run in parallel locally.
- Pass `&kafka_context.bootstrap_servers` into every helper that builds
  a client. There is no global "bootstrap servers" anywhere; the
  container's address is only known after `shared()` resolves.

## Helper cheatsheet

Most of what you'll need is in `tests/utils/`:

| You want to ...                  | Use                                                                |
| -------------------------------- | ------------------------------------------------------------------ |
| Get the shared broker            | `containers::KafkaContext::shared().await`                         |
| Make an admin client             | `admin::create_admin_client(bootstrap_servers).await`              |
| Create a topic                   | `admin::create_topic(client, name).await` (one partition, RF 1)    |
| Get a `NewTopic` vec for finer control | `admin::new_topic_vec(name, Some(num_partitions))`           |
| Make a `BaseProducer`            | `producer::base_producer::create_producer(bootstrap_servers).await` |
| Make a `FutureProducer`          | `producer::future_producer::create_producer(bootstrap_servers).await` |
| `FutureProducer` with config overrides | `producer::future_producer::create_producer_with_overrides(bootstrap_servers, &[(key, value)]).await` |
| Make a `BaseConsumer`            | `consumer::create_subscribed_base_consumer(bootstrap_servers, group, topic).await` |
| Make a `StreamConsumer`          | `consumer::stream_consumer::create_stream_consumer(bootstrap_servers, Some(group)).await` |
| Produce N messages               | `produce_messages(producer, topic, n, partition, timestamp).await` (from `utils::*`) |
| Produce N to a partition         | `produce_messages_to_partition(producer, topic, n, partition).await` |
| Populate a topic with a `FutureProducer` | `topics::populate_topic_using_future_producer(producer, topic, n, partition).await` |
| Random topic name                | `rand::rand_test_topic("test_name")`                               |
| Random consumer group            | `rand::rand_test_group()`                                          |
| Random transactional id          | `rand::rand_test_transactional_id()`                               |
| Init `env_logger` once           | `logging::init_test_logger()`                                      |

Constants you'll see:

- `utils::BROKER_ID == 1`. The single-broker testcontainers image
  hardcodes its broker id to 1. Assert against the constant, not a
  magic number.

## Known quirks

A few tests have non-obvious shapes; read these before debugging a
failure you didn't introduce.

- **`tests/base_producer.rs::test_base_producer_timeout`** points the
  producer at `127.0.0.1:1` (a deliberately unreachable address) instead
  of the real broker. The test is exercising the delivery callback when
  `message.timeout.ms` fires. Pointing at the real broker, with
  `auto.create.topics.enable=true`, lets the message get delivered
  inside the 100ms deadline on fast CI hardware and the test flakes.

- **`tests/consumer_groups.rs::test_delete_unknown_group`** accepts
  either `GroupIdNotFound` or `NotCoordinator`. Both mean "the group
  doesn't exist," but which one you get depends on whether any earlier
  test in the binary has caused the consumer-group coordinator to
  initialise. We share a broker across the file, so coordinator state
  is non-deterministic from this test's point of view.

- **`tests/metadata.rs`** uses `BROKER_ID = 1` (the testcontainers
  default) and does not assert on host port. The old test suite
  hardcoded `0` and `9092` for the docker-compose broker; both are wrong
  here.

- **Transactions tests need the replication overrides.** If you ever
  copy `containers.rs::init` and drop the
  `KAFKA_TRANSACTION_STATE_LOG_*` env vars, the transactions tests will
  hang on broker startup waiting for `__transaction_state` to reach RF
  3. They won't fail with a clear error; they'll just sit there until
  the timeout.

- **`tests/base_consumer.rs::test_produce_consume_message_queue_nonempty_callback`**
  asserts wakeup-count deltas against a baseline captured after initial
  setup, not absolute counts. apache/kafka 3.7.x posts an event to the
  split partition queue during the initial position query (the partition
  is assigned at `Offset::Beginning`), which fires the nonempty callback
  once before any messages are produced. 3.8+ doesn't. Comparing deltas
  is portable across the matrix.

## CI

`.github/workflows/ci.yml` runs five jobs:

- **lint**: `cargo fmt --check`, `cargo clippy -- -Dwarnings`,
  `cargo clippy --tests -- -Dwarnings`, `cargo test --doc`. Lint
  failures break the build. Always run these locally before pushing.
- **check**: cross-platform builds on macOS, Windows, and Ubuntu with
  various feature combinations. No tests, just `cargo build` and
  `cargo test` in `rdkafka-sys` (its tests don't need a broker).
- **check-minimal-versions**: makes sure the declared semver
  constraints actually resolve.
- **test**: the integration suite. Fans out across `KAFKA_VERSION =
  3.7, 3.8, 3.9, 4.0`. Each row resolves to a specific
  `apache/kafka:<tag>` via `resolve_kafka_image_tag` and runs
  `cargo test --features zstd`. The `zstd` feature is on so the
  compression round-trip test for zstd in `tests/future_producer.rs`
  actually links the codec (rdkafka-sys passes `--disable-zstd` to
  librdkafka by default). Rows run sequentially (`max-parallel: 1`)
  because they share an Actions runner and each spawns its own Docker
  container.
- **runtime-examples**: smoke-tests `examples/runtime_smol.rs` and
  `examples/runtime_async_std.rs` against a pinned `apache/kafka:4.0.2`
  service container. The integration suite covers the tokio path via
  testcontainers; this job catches breakage in the alternative runtimes
  that `cargo build --all-targets` would miss. Not matrixed: it's a
  runtime-correctness check, not a broker-compatibility check.

## Troubleshooting

**"could not create kafka context" / Docker errors.** The Docker
daemon isn't running, or the current user can't reach it. Start Docker
Desktop / colima / OrbStack and retry.

**Image pull is slow on first run.** Expected. testcontainers pulls
`apache/kafka:<tag>` on first use and caches it in your local Docker
image store. Subsequent runs reuse it.

**Test hangs on the transactions suite.** Almost certainly the
`__transaction_state` topic can't reach its replication factor. Check
that the `KAFKA_TRANSACTION_STATE_LOG_*` env vars are still set in
`containers.rs::init`.

**Port 9092 in use.** Doesn't matter to testcontainers (it allocates a
random host port), but examples under `examples/` default to
`localhost:9092`. If you're running an example against a separate
broker you started by hand, make sure nothing else is bound to that
port.

**A test passes locally but fails in CI on Kafka 3.7.** The broker is a
different version. Run the same matrix locally: `KAFKA_VERSION=3.7
cargo test --test <file>`.

**Clippy complains in CI but not locally.** The lint job runs
`cargo clippy --tests -- -Dwarnings`. Reproduce that flag set locally.
