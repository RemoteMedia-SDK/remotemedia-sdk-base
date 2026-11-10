use super::children::{supervise, Children};
use std::{sync::Arc, time::Duration};
use tokio::sync::{oneshot, Notify};

#[tokio::test]
async fn unexpected_cancellation_during_drain_remains_a_failure() {
    let children = Children::default();
    let (stop, _stopping) = tokio::sync::watch::channel(false);
    let child = tokio::spawn(std::future::pending::<crate::Result<()>>());
    child.abort();
    children.add_draining("cancelled source", stop, child);
    let failures = children.drain().await;
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("cancelled source") && failures[0].contains("cancelled"));
}

#[tokio::test]
async fn drain_failure_interrupts_another_blocked_draining_child() {
    let children = Children::default();
    let (stop, _stopping) = tokio::sync::watch::channel(false);
    let retained = Arc::new(());
    let owner = retained.clone();
    let (ready, started) = oneshot::channel();
    children.add_draining(
        "held source",
        stop.clone(),
        tokio::spawn(async move {
            let _owner = owner;
            ready.send(()).unwrap();
            std::future::pending().await
        }),
    );
    started.await.unwrap();
    children.add_draining(
        "failed source",
        stop,
        tokio::spawn(async { Err(crate::Error::Execution("drain failure sentinel".into())) }),
    );
    let failures = tokio::time::timeout(Duration::from_secs(2), children.drain())
        .await
        .unwrap();
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("failed source: Execution error: drain failure sentinel"));
    assert_eq!(Arc::strong_count(&retained), 1);
}

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
    children.add_fallible("finished fallible child", tokio::spawn(async { Ok(()) }));
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
async fn returned_child_error_wakes_a_suspended_supervisor() {
    let children = Children::default();
    let (ready, started) = oneshot::channel();
    let mut running = tokio::spawn(supervise(children.clone(), async move {
        ready.send(()).unwrap();
        std::future::pending().await
    }));
    started.await.unwrap();
    children.add_fallible(
        "source",
        tokio::spawn(async { Err(crate::Error::Execution("allocation failed sentinel".into())) }),
    );
    let result = tokio::time::timeout(Duration::from_secs(2), &mut running).await;
    if result.is_err() {
        running.abort();
        let _ = running.await;
        panic!("returned child error did not wake supervisor");
    }
    let error = result.unwrap().unwrap().unwrap_err().to_string();
    assert!(
        error.contains("source") && error.contains("allocation failed sentinel"),
        "{error}"
    );
}

#[tokio::test]
async fn body_completion_preserves_a_finished_child_error_once() {
    let children = Children::default();
    let child =
        tokio::spawn(async { Err(crate::Error::Execution("returned child sentinel".into())) });
    while !child.is_finished() {
        tokio::task::yield_now().await;
    }
    children.add_fallible("finished source", child);
    let error = supervise(children, async { Ok(()) })
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        error.matches("returned child sentinel").count(),
        1,
        "{error}"
    );
    assert!(error.contains("finished source"), "{error}");
}

#[tokio::test]
async fn stopping_a_fallible_child_joins_its_owner_without_reporting_expected_abort() {
    let children = Children::default();
    let retained = Arc::new(());
    let owner = retained.clone();
    let (ready, started) = oneshot::channel();
    children.add_fallible(
        "held source",
        tokio::spawn(async move {
            let _owner = owner;
            ready.send(()).unwrap();
            std::future::pending().await
        }),
    );
    started.await.unwrap();
    assert_eq!(Arc::strong_count(&retained), 2);
    assert!(children.stop().await.is_empty());
    assert_eq!(Arc::strong_count(&retained), 1);
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
