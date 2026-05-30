//! FR-9 (WORM + hash chain) and FR-8 (bitemporal as-of) integration tests
//! against an in-memory SQLite database.

#![cfg(feature = "sqlite")]

use std::sync::Arc;

use atomr_persistence::{Journal, PersistentRepr};
use atomr_persistence_query::ReadJournal;
use atomr_persistence_sql::{ChainProof, IntegrityVerify, SqlConfig, SqlJournal, SqlReadJournal, WormConfig};
use sqlx::any::AnyPoolOptions;

fn repr(pid: &str, seq: u64, payload: &[u8], tags: &[&str]) -> PersistentRepr {
    PersistentRepr {
        persistence_id: pid.into(),
        sequence_nr: seq,
        payload: payload.to_vec(),
        manifest: "m".into(),
        writer_uuid: "w".into(),
        deleted: false,
        tags: tags.iter().map(|s| s.to_string()).collect(),
    }
}

/// Build a journal sharing a single pool/url so we can run raw SQL against
/// the same in-memory db the journal uses.
async fn shared_journal(worm: WormConfig) -> (Arc<SqlJournal>, sqlx::AnyPool) {
    sqlx::any::install_default_drivers();
    // A named shared-cache in-memory db survives across connections in the pool.
    let url = format!("sqlite:file:worm_{}?mode=memory&cache=shared", rand_suffix());
    let pool = AnyPoolOptions::new().max_connections(1).connect(&url).await.expect("pool");
    let cfg = SqlConfig::new(url).with_max_connections(1);
    let j = SqlJournal::from_pool(pool.clone(), cfg).await.expect("journal");
    let j = j.with_worm(worm).await.expect("worm");
    (j, pool)
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

#[tokio::test]
async fn worm_denies_update_and_delete() {
    let (j, pool) = shared_journal(WormConfig::enforced()).await;
    j.write_messages(vec![repr("p", 1, b"hello", &[])]).await.unwrap();

    // A direct UPDATE must be rejected by the WORM trigger.
    let upd = sqlx::query("UPDATE event_journal SET payload = ? WHERE persistence_id = 'p'")
        .bind(b"tampered".to_vec())
        .execute(&pool)
        .await;
    assert!(upd.is_err(), "UPDATE should be denied under WORM");

    // A direct DELETE must also be rejected.
    let del = sqlx::query("DELETE FROM event_journal WHERE persistence_id = 'p'").execute(&pool).await;
    assert!(del.is_err(), "DELETE should be denied under WORM");
}

#[tokio::test]
async fn verify_chain_intact_for_clean_data() {
    let (j, _pool) = shared_journal(WormConfig { hash_chain: true, deny_update_delete: false }).await;
    j.write_messages(vec![repr("c", 1, b"a", &[]), repr("c", 2, b"b", &[])]).await.unwrap();
    j.write_messages(vec![repr("c", 3, b"cc", &[])]).await.unwrap(); // separate batch
    match j.verify_chain("c").await.unwrap() {
        ChainProof::Intact { rows } => assert_eq!(rows, 3),
        other => panic!("expected Intact, got {other:?}"),
    }
}

#[tokio::test]
async fn verify_chain_detects_tampering() {
    // hash_chain on, deny off so we can forcibly mutate a payload to simulate
    // an attacker who bypassed the WORM trigger (e.g. raw file edit).
    let (j, pool) = shared_journal(WormConfig { hash_chain: true, deny_update_delete: false }).await;
    j.write_messages(vec![repr("t", 1, b"one", &[]), repr("t", 2, b"two", &[]), repr("t", 3, b"three", &[])])
        .await
        .unwrap();

    // Forcibly edit sequence 2's payload — leaves stored row_hash stale.
    sqlx::query("UPDATE event_journal SET payload = ? WHERE persistence_id = 't' AND sequence_nr = 2")
        .bind(b"TWO!".to_vec())
        .execute(&pool)
        .await
        .unwrap();

    match j.verify_chain("t").await.unwrap() {
        ChainProof::Tampered { first_bad_sequence_nr } => assert_eq!(first_bad_sequence_nr, 2),
        other => panic!("expected Tampered at 2, got {other:?}"),
    }
}

#[tokio::test]
async fn events_as_of_excludes_later_restatements() {
    // No WORM needed; system_time is assigned on every write.
    let (j, pool) = shared_journal(WormConfig::default()).await;
    j.write_messages(vec![repr("a", 1, b"v1", &[]), repr("a", 2, b"v2", &[])]).await.unwrap();

    // Pin a cutoff between the two writes by reading the actual system_times.
    let times: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT sequence_nr, system_time FROM event_journal WHERE persistence_id='a' ORDER BY sequence_nr",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(times.len(), 2);

    // Force seq 2 to be recorded *later* than our cutoff (a restatement).
    let cutoff = times[0].1; // system_time of seq 1
    sqlx::query("UPDATE event_journal SET system_time = ? WHERE persistence_id='a' AND sequence_nr=2")
        .bind(cutoff + 1_000_000)
        .execute(&pool)
        .await
        .unwrap();

    let rj = SqlReadJournal::new(j);
    // As of the cutoff, only seq 1 was known — seq 2's later restatement is invisible.
    let as_of = rj.events_as_of("a", cutoff as u128).await.unwrap();
    assert_eq!(as_of.len(), 1, "later-recorded restatement must not appear");
    assert_eq!(as_of[0].sequence_nr, 1);

    // As of "now" both are visible.
    let now = (cutoff + 2_000_000) as u128;
    let later = rj.events_as_of("a", now).await.unwrap();
    assert_eq!(later.len(), 2);
}

#[tokio::test]
async fn bitemporal_slice_distinguishes_corrected_value() {
    let (j, _pool) = shared_journal(WormConfig::default()).await;
    // seq 1: originally-known value valid at t=100.
    // seq 2: a correction whose valid_time is also 100 but recorded later.
    // seq 3: a value valid at t=300.
    j.write_messages(vec![
        repr("b", 1, b"orig@100", &["valid_time:100"]),
        repr("b", 2, b"corrected@100", &["valid_time:100"]),
        repr("b", 3, b"new@300", &["valid_time:300"]),
    ])
    .await
    .unwrap();

    let rj = SqlReadJournal::new(j);
    let now = u128::MAX; // known to system "now"

    // Valid-as-of t=200: includes both rows valid at <=200 (the two valid@100
    // entries), but excludes the t=300 value.
    let slice = rj.events_valid_as_of("b", 200, now).await.unwrap();
    assert_eq!(slice.len(), 2, "only valid_time<=200 rows");
    assert!(slice.iter().all(|e| e.sequence_nr <= 2));

    // Valid-as-of t=300: now the later-valid value joins.
    let slice2 = rj.events_valid_as_of("b", 300, now).await.unwrap();
    assert_eq!(slice2.len(), 3);
}
