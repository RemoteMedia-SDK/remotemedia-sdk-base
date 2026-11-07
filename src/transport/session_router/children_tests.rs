use super::children::{supervise, Children};
use std::{sync::Arc, time::Duration};
use tokio::sync::{oneshot, Notify};

#[tokio::test]
async fn registering_a_cancelled_child_wakes_a_suspended_supervisor() {
    let children = Children::default();
    let started = Arc::new(Notify::new());
    let ready = started.clone();
    let owned = children.clone();
    let mut running = tokio::spawn(supervise(owned, async move {
        ready.notify_one();
        std::future::pending().await
    }));
    started.notified().await;
    let child = tokio::spawn(std::future::pending::<()>());
    child.abort();
    children.add("late cancelled child", child);
    let result = tokio::time::timeout(Duration::from_secs(2), &mut running).await;
    if result.is_err() {
        running.abort();
        let _ = running.await;
        panic!("registration did not wake the supervisor");
    }
    let error = result.unwrap().unwrap().unwrap_err().to_string();
    assert!(
        error.contains("late cancelled child") && error.contains("cancelled"),
        "{error}"
    );
}

#[tokio::test]
async fn successful_child_completion_does_not_cancel_the_body() {
    let children = Children::default();
    children.add("finished child", tokio::spawn(async {}));
    let (release, wait) = oneshot::channel();
    let mut running = tokio::spawn(supervise(children, async move {
        wait.await.unwrap();
        Ok(())
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut running)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn body_completion_still_reports_a_finished_child_panic_once() {
    let children = Children::default();
    let child = tokio::spawn(async { panic!("completed child sentinel") });
    while !child.is_finished() {
        tokio::task::yield_now().await;
    }
    children.add("finished panic", child);
    let error = supervise(children, async { Ok(()) })
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        error.matches("completed child sentinel").count(),
        1,
        "{error}"
    );
    assert!(error.contains("finished panic"), "{error}");
}
