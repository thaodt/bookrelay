//! State replay and continuity evidence are different claims.
use chrono::Utc;
use engineering_challenge::{
    book::OrderBook,
    exchange::{ExchangeAdapter, kalshi::Kalshi},
    model::BookEvent,
    output::{OrderingEvidence, Record, SnapshotReason, SourceStamp, Writer},
    replay,
};
use serde_json::Value;
use std::path::Path;

fn stamp(evidence: OrderingEvidence, generation: u64, epoch: u64) -> SourceStamp {
    SourceStamp {
        evidence,
        unverified_cut: false,
        replica: 0,
        generation,
        output_epoch: epoch,
        incarnation: 1,
        continuity_epoch: 0,
        clock: (evidence != OrderingEvidence::SnapshotOnly).then_some(10),
        ordinal: None,
        complete_clock: evidence != OrderingEvidence::SnapshotOnly,
    }
}

async fn capture(root: &Path, rows: Vec<(SnapshotReason, SourceStamp)>) {
    let writer = Writer::start(
        &root.join("out.sbe"),
        Some(&root.join("audit.jsonl")),
        8,
        1 << 20,
    )
    .unwrap();
    let frame = Kalshi
        .decode(
            include_str!("fixtures/kalshi/orderbook_snapshot.json"),
            Utc::now(),
        )
        .unwrap();
    let BookEvent::Snapshot(snapshot) = frame.events.into_iter().next().unwrap() else {
        panic!()
    };
    let mut book = OrderBook::new(snapshot.symbol.clone());
    book.replace(snapshot).unwrap();
    for (reason, source) in rows {
        writer
            .output
            .send(Record::SourcedSnapshot {
                data: book.snapshot(Utc::now()).unwrap(),
                reason,
                source,
            })
            .await
            .unwrap();
    }
    writer.finish().await.unwrap();
}

fn rewrite(root: &Path, change: impl FnOnce(&mut Vec<Value>)) {
    let path = root.join("audit.jsonl");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    change(&mut rows);
    std::fs::write(
        path,
        rows.iter().map(|r| format!("{r}\n")).collect::<String>(),
    )
    .unwrap();
}

fn verify(root: &Path) -> anyhow::Result<replay::ReplayReport> {
    replay::verify(&root.join("out.sbe"), Some(&root.join("audit.jsonl")), 10)
}

#[tokio::test]
async fn public_replay_reports_state_checks_and_unverified_source_transitions_separately() {
    let temp = tempfile::tempdir().unwrap();
    let initial = stamp(OrderingEvidence::SnapshotOnly, 1, 1);
    let reconciliation = stamp(OrderingEvidence::SnapshotOnly, 1, 2);
    let mut replacement = stamp(OrderingEvidence::SnapshotOnly, 2, 3);
    replacement.unverified_cut = true;
    capture(
        temp.path(),
        vec![
            (SnapshotReason::Initial, initial),
            (SnapshotReason::Recovery, reconciliation),
            (SnapshotReason::Failover, replacement.clone()),
            (SnapshotReason::Periodic, replacement),
        ],
    )
    .await;
    let report = verify(temp.path()).unwrap();
    assert_eq!(report.source_baselines_verified, 4);
    assert_eq!(report.snapshot_only_baselines, 4);
    assert_eq!(report.comparable_clock_baselines_verified, 0);
    assert_eq!(report.legacy_unspecified_baselines, 0);
    assert_eq!(report.unverified_continuity_transitions, 1);
    assert_eq!(report.periodic_checkpoints_verified, 1);
    rewrite(temp.path(), |rows| {
        rows[2]["source"]["unverified_cut"] = false.into()
    });
    assert!(
        verify(temp.path())
            .unwrap_err()
            .to_string()
            .contains("hid continuity uncertainty")
    );
}

#[tokio::test]
async fn public_evidence_cannot_manufacture_a_clock_or_relabel_a_periodic_cut() {
    let temp = tempfile::tempdir().unwrap();
    let source = stamp(OrderingEvidence::SnapshotOnly, 1, 1);
    capture(
        temp.path(),
        vec![
            (SnapshotReason::Initial, source.clone()),
            (SnapshotReason::Periodic, source),
        ],
    )
    .await;
    rewrite(temp.path(), |rows| rows[0]["source"]["clock"] = 1.into());
    assert!(
        verify(temp.path())
            .unwrap_err()
            .to_string()
            .contains("manufactured a clock")
    );
    rewrite(temp.path(), |rows| {
        rows[0]["source"]["clock"] = Value::Null;
        rows[1]["source"]["unverified_cut"] = true.into();
    });
    assert!(
        verify(temp.path())
            .unwrap_err()
            .to_string()
            .contains("periodic snapshot changed source")
    );
}

#[tokio::test]
async fn native_evidence_requires_a_clock_and_never_claims_an_unverified_cut() {
    let temp = tempfile::tempdir().unwrap();
    capture(
        temp.path(),
        vec![(
            SnapshotReason::Initial,
            stamp(OrderingEvidence::ComparableClock, 1, 1),
        )],
    )
    .await;
    let report = verify(temp.path()).unwrap();
    assert_eq!(report.comparable_clock_baselines_verified, 1);
    assert_eq!(report.snapshot_only_baselines, 0);
    rewrite(temp.path(), |rows| {
        rows[0]["source"]["unverified_cut"] = true.into()
    });
    assert!(
        verify(temp.path())
            .unwrap_err()
            .to_string()
            .contains("invalid comparable-clock evidence")
    );
    rewrite(temp.path(), |rows| {
        rows[0]["source"]["unverified_cut"] = false.into();
        rows[0]["source"]["clock"] = Value::Null;
    });
    assert!(
        verify(temp.path())
            .unwrap_err()
            .to_string()
            .contains("invalid comparable-clock evidence")
    );
}

#[tokio::test]
async fn old_sidecars_remain_readable_without_becoming_verified_native_evidence() {
    let temp = tempfile::tempdir().unwrap();
    capture(
        temp.path(),
        vec![(
            SnapshotReason::Initial,
            stamp(OrderingEvidence::LegacyUnspecified, 1, 1),
        )],
    )
    .await;
    rewrite(temp.path(), |rows| {
        let source = rows[0]["source"].as_object_mut().unwrap();
        source.remove("evidence");
        source.remove("unverified_cut");
    });
    let report = verify(temp.path()).unwrap();
    assert_eq!(report.source_baselines_verified, 1);
    assert_eq!(report.legacy_unspecified_baselines, 1);
    assert_eq!(report.comparable_clock_baselines_verified, 0);
    rewrite(temp.path(), |rows| {
        rows[0].as_object_mut().unwrap().remove("source");
    });
    let report = verify(temp.path()).unwrap();
    assert_eq!(report.source_baselines_verified, 0);
    assert_eq!(report.legacy_unspecified_baselines, 1);
}
