use std::time::{Duration, Instant};

use crate::utils::consumer;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::rand::{rand_test_group, rand_test_topic};
use rdkafka::admin::{AdminOptions, GroupResult, NewTopic, TopicReplication};
use rdkafka::consumer::Consumer;
use rdkafka_sys::RDKafkaErrorCode;

mod utils;

/// Verify that a valid group can be deleted.
#[tokio::test]
pub async fn test_consumer_groups_deletion() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    // Create consumer_client
    let group_name = rand_test_group();
    let topic_name = rand_test_topic("test_topic");
    let consumer_client = utils::consumer::create_unsubscribed_base_consumer(
        &kafka_context.bootstrap_servers,
        Some(&group_name),
    )
    .await
    .expect("could not create subscribed base consumer");

    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");

    utils::consumer::create_consumer_group_on_topic(&consumer_client, &topic_name)
        .await
        .expect("could not create group");
    let res = admin_client
        .delete_groups(&[&group_name], &AdminOptions::default())
        .await
        .expect("could not delete groups");
    assert_eq!(res, [Ok(group_name.to_string())]);
}

/// Verify that attempting to delete an unknown group returns a "group not
/// found" error.
#[tokio::test]
pub async fn test_delete_unknown_group() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    let unknown_group_name = rand_test_group();
    let res = admin_client
        .delete_groups(&[&unknown_group_name], &AdminOptions::default())
        .await
        .expect("delete_groups call failed");
    // The broker reports GroupIdNotFound once the consumer-group coordinator
    // has been initialised (any prior test in the binary that touched a
    // group is enough), and NotCoordinator on a cold broker. Both indicate
    // the same thing for this test: the group does not exist.
    let group_result: &GroupResult = res.first().expect("expected one result");
    let (returned_name, code) = group_result
        .as_ref()
        .expect_err("expected an error for an unknown group");
    assert_eq!(returned_name, &unknown_group_name);
    assert!(
        matches!(
            code,
            RDKafkaErrorCode::GroupIdNotFound | RDKafkaErrorCode::NotCoordinator
        ),
        "unexpected error code: {:?}",
        code
    );
}

// `delete_groups` cannot remove a group while it still has an active member.
// This test subscribes a consumer to a topic, drives it until it has actually
// joined the group, calls `delete_groups`, and asserts the per-group result
// is `NonEmptyGroup`. It then drops the consumer (which sends LeaveGroup),
// retries `delete_groups`, and asserts the second call succeeds. A binding
// regression that misclassified the per-group error or that lost the
// active-membership signal in the second-call retry would fail one of those
// assertions.
#[tokio::test]
pub async fn test_delete_non_empty_consumer_group() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    let group_name = rand_test_group();
    let topic_name = rand_test_topic("test_delete_non_empty_group");

    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");

    let consumer_client = utils::consumer::create_subscribed_base_consumer(
        &kafka_context.bootstrap_servers,
        Some(&group_name),
        &topic_name,
    )
    .await
    .expect("could not create subscribed consumer");

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        consumer_client.poll(Duration::from_millis(200));
        if consumer_client.assignment().unwrap().count() > 0 {
            break;
        }
        if Instant::now() > deadline {
            panic!("consumer never joined the group");
        }
    }

    let res = admin_client
        .delete_groups(&[&group_name], &AdminOptions::default())
        .await
        .expect("delete_groups call should not itself fail");
    let first: &GroupResult = res.first().expect("expected one result");
    let (returned_name, code) = first
        .as_ref()
        .expect_err("delete_groups on a non-empty group should be an error");
    assert_eq!(returned_name, &group_name);
    assert_eq!(
        *code,
        RDKafkaErrorCode::NonEmptyGroup,
        "expected NonEmptyGroup while the consumer is still active, got {:?}",
        code
    );

    drop(consumer_client);

    let deadline = Instant::now() + Duration::from_secs(30);
    let last_err: RDKafkaErrorCode = loop {
        let res = admin_client
            .delete_groups(&[&group_name], &AdminOptions::default())
            .await
            .expect("delete_groups call should not itself fail");
        match res.first().expect("expected one result") {
            Ok(name) => {
                assert_eq!(name, &group_name);
                return;
            }
            Err((_, code)) => {
                if Instant::now() > deadline {
                    break *code;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    panic!(
        "delete_groups never converged to success after consumer drop (last error: {:?})",
        last_err
    );
}

/// Verify that deleting a valid and invalid group results in a mixed result
/// set.
#[tokio::test]
pub async fn test_consumer_group_action_mixed_results() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    // Create consumer_client
    let group_name = rand_test_group();
    let topic_name = rand_test_topic("test_topic");
    let consumer_client = utils::consumer::create_unsubscribed_base_consumer(
        &kafka_context.bootstrap_servers,
        Some(&group_name),
    )
    .await
    .expect("could not create subscribed base consumer");

    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");

    let unknown_group_name = rand_test_group();
    consumer::create_consumer_group_on_topic(&consumer_client, &topic_name)
        .await
        .expect("could not create group");
    let res = admin_client
        .delete_groups(
            &[&group_name, &unknown_group_name],
            &AdminOptions::default(),
        )
        .await;
    assert_eq!(
        res,
        Ok(vec![
            Ok(group_name.to_string()),
            Err((
                unknown_group_name.to_string(),
                RDKafkaErrorCode::GroupIdNotFound
            ))
        ])
    );
}
