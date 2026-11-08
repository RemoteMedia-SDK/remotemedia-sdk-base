use super::*;
#[tokio::test]
async fn test_inprocess_channel_creation() {
    let registry = InProcessChannelRegistry::global();

    let channel_name = format!("test/channel/create/{}", std::process::id());
    let channel = registry
        .create_channel(&channel_name, 100, true)
        .await
        .unwrap();

    assert_eq!(channel.name, channel_name);
    assert_eq!(channel.capacity, 100);
    assert!(channel.backpressure_enabled);

    registry.destroy_channel(channel).await.unwrap();
}

#[tokio::test]
async fn test_inprocess_publish_subscribe() {
    let registry = InProcessChannelRegistry::global();

    let channel_name = format!(
        "test/channel/pubsub/{}/{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let channel = registry
        .create_channel(&channel_name, 10, false)
        .await
        .unwrap();

    // Create publisher and subscriber
    let publisher = registry.create_publisher(&channel_name).await.unwrap();
    let mut subscriber = registry.create_subscriber(&channel_name).await.unwrap();

    // Publish data
    let data = RuntimeData::Text("Hello, InProcess!".into());
    publisher.publish(&data).unwrap();

    // Small delay
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Receive data
    let received = subscriber.receive().unwrap();
    assert!(received.is_some());

    let received_data = received.unwrap();
    match &received_data {
        RuntimeData::Text(s) => assert_eq!(s.as_str(), "Hello, InProcess!"),
        _ => panic!("Expected Text variant, got {:?}", received_data.data_type()),
    }

    registry.destroy_channel(channel).await.unwrap();
}
