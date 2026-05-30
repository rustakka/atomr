//! FR-9 — transactional outbox: event row and outbox row commit or roll back
//! together, against an in-memory SQLite database.

use atomr_patterns::tx_outbox::{EventRow, OutboxRow, TxOutbox};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;

async fn setup() -> AnyPool {
    sqlx::any::install_default_drivers();
    let url = format!(
        "sqlite:file:txob_{}?mode=memory&cache=shared",
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let pool = AnyPoolOptions::new().max_connections(1).connect(&url).await.unwrap();
    sqlx::query(
        "CREATE TABLE event_journal (persistence_id TEXT NOT NULL, sequence_nr INTEGER NOT NULL, \
         payload BLOB NOT NULL, manifest TEXT NOT NULL DEFAULT '', writer_uuid TEXT NOT NULL DEFAULT '', \
         deleted INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, \
         PRIMARY KEY (persistence_id, sequence_nr))",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL, \
         payload BLOB NOT NULL, created_at INTEGER NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool
}

fn event_row() -> EventRow {
    EventRow {
        persistence_id: "agg-1".into(),
        sequence_nr: 1,
        payload: b"evt".to_vec(),
        manifest: "m".into(),
        writer_uuid: "w".into(),
    }
}

fn outbox_row() -> OutboxRow {
    OutboxRow { topic: "orders".into(), payload: b"msg".to_vec() }
}

async fn counts(pool: &AnyPool) -> (i64, i64) {
    let (ev,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_journal").fetch_one(pool).await.unwrap();
    let (ob,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM outbox").fetch_one(pool).await.unwrap();
    (ev, ob)
}

#[tokio::test]
async fn both_rows_commit_together() {
    let pool = setup().await;
    let mut tx = pool.begin().await.unwrap();
    TxOutbox.persist_with(&mut tx, event_row(), outbox_row()).await.unwrap();
    tx.commit().await.unwrap();

    let (ev, ob) = counts(&pool).await;
    assert_eq!(ev, 1, "event row committed");
    assert_eq!(ob, 1, "outbox row committed");
}

#[tokio::test]
async fn rollback_persists_neither_row() {
    let pool = setup().await;
    let mut tx = pool.begin().await.unwrap();
    TxOutbox.persist_with(&mut tx, event_row(), outbox_row()).await.unwrap();
    // Simulate an error before commit: drop the transaction (implicit rollback).
    drop(tx);

    let (ev, ob) = counts(&pool).await;
    assert_eq!(ev, 0, "event row rolled back");
    assert_eq!(ob, 0, "outbox row rolled back");
}

#[tokio::test]
async fn explicit_rollback_persists_neither_row() {
    let pool = setup().await;
    let mut tx = pool.begin().await.unwrap();
    TxOutbox.persist_with(&mut tx, event_row(), outbox_row()).await.unwrap();
    tx.rollback().await.unwrap();

    let (ev, ob) = counts(&pool).await;
    assert_eq!(ev, 0);
    assert_eq!(ob, 0);
}
