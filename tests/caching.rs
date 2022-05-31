use eth_analysis::config;
use sqlx::postgres::PgListener;

// This test fails sometimes because when run against the actual dev DB many
// notifications fire on the "cache-update" channel. Needs a test DB to work reliably.
#[ignore]
#[tokio::test]
async fn test_publish_cache_update() {
    let mut listener = PgListener::connect(&config::get_db_url()).await.unwrap();
    listener.listen("cache-update").await.unwrap();

    let notification_future = async { listener.recv().await };

    let pool = sqlx::PgPool::connect(&config::get_db_url()).await.unwrap();

    let test_key = "test-key";
    eth_analysis::caching::publish_cache_update(&pool, test_key).await;

    let notification = notification_future.await.unwrap();

    assert_eq!(notification.payload(), test_key)
}
