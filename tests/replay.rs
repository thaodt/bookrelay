use chrono::Utc;
use engineering_challenge::{
    book::OrderBook,
    exchange::{ExchangeAdapter, kalshi::Kalshi},
    model::BookEvent,
    output::{CHUNK_SIZE, Record, SnapshotReason, Writer},
    replay,
};

#[tokio::test]
async fn captured_exchange_events_round_trip_and_periodic_snapshots_are_checked() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("capture.sbe");
    let audit = temp.path().join("audit.jsonl");
    let writer = Writer::start(&path, Some(&audit), 4, 1 << 20).unwrap();
    let snapshot = Kalshi
        .decode(
            include_str!("fixtures/kalshi/orderbook_snapshot.json"),
            Utc::now(),
        )
        .unwrap();
    let BookEvent::Snapshot(snapshot) = snapshot.events.into_iter().next().unwrap() else {
        panic!()
    };
    let mut book = OrderBook::new(snapshot.symbol.clone());
    let received_at = snapshot.received_at;
    book.replace(snapshot).unwrap();
    writer
        .output
        .send(Record::Snapshot {
            data: book.snapshot(received_at).unwrap(),
            reason: SnapshotReason::Initial,
        })
        .await
        .unwrap();
    let delta = Kalshi
        .decode(
            include_str!("fixtures/kalshi/orderbook_delta.json"),
            Utc::now(),
        )
        .unwrap();
    let BookEvent::Update(update) = delta.events.into_iter().next().unwrap() else {
        panic!()
    };
    writer
        .output
        .send(Record::Update(book.apply(update).unwrap()))
        .await
        .unwrap();
    writer
        .output
        .send(Record::Snapshot {
            data: book.snapshot(Utc::now()).unwrap(),
            reason: SnapshotReason::Periodic,
        })
        .await
        .unwrap();
    let summary = writer.finish().await.unwrap();
    assert_eq!(summary.records, 3);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), CHUNK_SIZE as u64);
    let report = replay::verify(&path, Some(&audit), 10).unwrap();
    assert_eq!(report.records, 3);
    assert_eq!(report.periodic_checkpoints_verified, 1);
    assert_eq!(report.updates, 1);
}

#[tokio::test]
async fn wrong_periodic_checkpoint_is_rejected_but_explicit_recovery_can_reset() {
    let temp = tempfile::tempdir().unwrap();
    for reason in [SnapshotReason::Periodic, SnapshotReason::Recovery] {
        let path = temp.path().join(format!("{reason:?}.sbe"));
        let audit = temp.path().join(format!("{reason:?}.jsonl"));
        let writer = Writer::start(&path, Some(&audit), 4, 1 << 20).unwrap();
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
        let initial = book.snapshot(Utc::now()).unwrap();
        let mut changed = initial.clone();
        changed.asks[0].quantity += 1;
        writer
            .output
            .send(Record::Snapshot {
                data: initial,
                reason: SnapshotReason::Initial,
            })
            .await
            .unwrap();
        writer
            .output
            .send(Record::Snapshot {
                data: changed,
                reason,
            })
            .await
            .unwrap();
        writer.finish().await.unwrap();
        let result = replay::verify(&path, Some(&audit), 10);
        if reason == SnapshotReason::Periodic {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("periodic checkpoint")
            );
        } else {
            assert_eq!(result.unwrap().explicit_baseline_resets, 2);
        }
        assert_eq!(
            replay::verify(&path, None, 10)
                .unwrap()
                .unclassified_snapshot_changes,
            1
        );
    }
}

#[tokio::test]
async fn writer_failure_and_byte_budget_are_reported() {
    let temp = tempfile::tempdir().unwrap();
    let writer = Writer::start(&temp.path().join("empty.sbe"), None, 1, 1).unwrap();
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
    assert!(
        writer
            .output
            .send(Record::Snapshot {
                data: book.snapshot(Utc::now()).unwrap(),
                reason: SnapshotReason::Initial
            })
            .await
            .is_err()
    );
    writer.finish().await.unwrap();
    let writer = Writer::start(std::path::Path::new("/dev/full"), None, 1, 1 << 20).unwrap();
    writer
        .output
        .send(Record::Snapshot {
            data: book.snapshot(Utc::now()).unwrap(),
            reason: SnapshotReason::Initial,
        })
        .await
        .unwrap();
    assert!(writer.finish().await.is_err());
}

#[test]
fn truncated_capture_is_not_silently_accepted_as_eof() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), [1u8; 8]).unwrap();
    assert!(
        replay::verify(temp.path(), None, 10)
            .unwrap_err()
            .to_string()
            .contains("incomplete chunk")
    );
}
