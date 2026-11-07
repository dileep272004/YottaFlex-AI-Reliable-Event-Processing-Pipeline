import pandas as pd
import pytest

from src.run_pipeline import run_pipeline
from src.pipeline import (
    process_events,
    validate_references_and_refunds,
    detect_out_of_order_events,
)
from src.deduplicate import deduplicate_events
from src.late_arrival import track_late_arrivals
from src.checkpoint import save_checkpoint, load_checkpoint


def make_event(
    event_id="E001",
    event_type="purchase",
    reference_event_id="",
    amount=500,
    event_time="2026-11-06 10:00:00",
    ingestion_time="2026-11-06 10:01:00",
):
    return {
        "event_id": event_id,
        "order_id": "O001",
        "reference_event_id": reference_event_id,
        "customer_id": "C001",
        "product_id": "P001",
        "event_type": event_type,
        "payment_method": "upi",
        "quantity": 1,
        "amount_inr": amount,
        "event_time": event_time,
        "ingestion_time": ingestion_time,
    }


def test_valid_purchase_is_processed():
    df = pd.DataFrame([make_event()])

    processed, rejected, duplicates = process_events(df)

    assert len(processed) == 1
    assert len(rejected) == 0
    assert len(duplicates) == 0


def test_valid_refund_is_processed():
    purchase = make_event()
    refund = make_event(
        event_id="E002",
        event_type="refund",
        reference_event_id="E001",
        amount=200,
    )

    df = pd.DataFrame([purchase, refund])

    processed, rejected, duplicates = process_events(df)

    assert len(processed) == 2
    assert len(rejected) == 0
    assert len(duplicates) == 0


def test_valid_cancellation_is_processed():
    purchase = make_event()
    cancellation = make_event(
        event_id="E002",
        event_type="cancellation",
        reference_event_id="E001",
        amount=0,
    )

    df = pd.DataFrame([purchase, cancellation])

    processed, rejected, duplicates = process_events(df)

    assert len(processed) == 2
    assert len(rejected) == 0


def test_duplicate_event_id_is_detected():
    df = pd.DataFrame([
        make_event("E001"),
        make_event("E001"),
    ])

    unique, duplicates = deduplicate_events(df)

    assert len(unique) == 1
    assert len(duplicates) == 1


def test_negative_purchase_amount_is_rejected():
    df = pd.DataFrame([
        make_event(amount=-100)
    ])

    processed, rejected, duplicates = process_events(df)

    assert len(processed) == 0
    assert len(rejected) == 1


def test_invalid_event_type_is_rejected():
    event = make_event()
    event["event_type"] = "unknown"

    processed, rejected, duplicates = process_events(
        pd.DataFrame([event])
    )

    assert len(processed) == 0
    assert len(rejected) == 1


def test_missing_customer_id_is_rejected():
    event = make_event()
    event["customer_id"] = ""

    processed, rejected, duplicates = process_events(
        pd.DataFrame([event])
    )

    assert len(processed) == 0
    assert len(rejected) == 1


def test_refund_exceeding_purchase_amount_is_rejected():
    purchase = make_event()
    refund1 = make_event(
        event_id="E002",
        event_type="refund",
        reference_event_id="E001",
        amount=300,
    )
    refund2 = make_event(
        event_id="E003",
        event_type="refund",
        reference_event_id="E001",
        amount=300,
    )

    df = pd.DataFrame([purchase, refund1, refund2])

    processed, rejected, duplicates = process_events(df)

    assert len(processed) == 2
    assert len(rejected) == 1
    assert "exceeds" in rejected.iloc[0]["rejection_reason"].lower()


def test_refund_can_reference_purchase_in_later_batch():
    purchase_lookup = {
        "E001": make_event()
    }

    refund = pd.DataFrame([
        make_event(
            event_id="E002",
            event_type="refund",
            reference_event_id="E001",
            amount=200,
        )
    ])

    processed, rejected, duplicates = process_events(
        refund,
        purchase_lookup=purchase_lookup,
    )

    assert len(processed) == 1
    assert len(rejected) == 0


def test_nonexistent_purchase_reference_is_rejected():
    refund = pd.DataFrame([
        make_event(
            event_id="E002",
            event_type="refund",
            reference_event_id="E999",
            amount=100,
        )
    ])

    processed, rejected, duplicates = process_events(refund)

    assert len(processed) == 0
    assert len(rejected) == 1


def test_late_arrival_is_detected():
    df = pd.DataFrame([
        make_event(
            event_time="2026-11-06 10:00:00",
            ingestion_time="2026-11-06 10:30:00",
        )
    ])

    result = track_late_arrivals(df)

    assert bool(result.iloc[0]["is_late"]) is True


def test_out_of_order_event_is_detected():
    df = pd.DataFrame([
        make_event(
            event_id="E001",
            event_time="2026-11-06 10:10:00",
            ingestion_time="2026-11-06 10:11:00",
        ),
        make_event(
            event_id="E002",
            event_time="2026-11-06 10:05:00",
            ingestion_time="2026-11-06 10:12:00",
        ),
    ])

    result = detect_out_of_order_events(df)

    assert bool(result.iloc[1]["is_out_of_order"]) is True


def test_checkpoint_save_and_recovery(tmp_path):
    checkpoint_file = tmp_path / "checkpoint.json"

    save_checkpoint(
        processed_event_ids={"E001", "E002"},
        last_batch_id="batch_2",
        last_offset=2000,
        checkpoint_file=checkpoint_file,
        purchase_state={
            "E001": make_event()
        },
        refunded_amounts={
            "E001": 200
        },
    )

    restored = load_checkpoint(checkpoint_file)

    assert restored["last_offset"] == 2000
    assert "E001" in restored["processed_event_ids"]
    assert "E001" in restored["purchase_state"]
    assert restored["refunded_amounts"]["E001"] == 200


def test_purchase_reference_mismatch_is_rejected():
    purchase = make_event()

    purchase_state = {
        "E001": purchase
    }

    refund = make_event(
        event_id="E002",
        event_type="refund",
        reference_event_id="E001",
        amount=100,
    )
    refund["customer_id"] = "C999"

    valid, rejected = validate_references_and_refunds(
        pd.DataFrame([refund]),
        purchase_state=purchase_state,
    )

    assert len(valid) == 0
    assert len(rejected) == 1


def test_pipeline_recovers_after_crash_without_duplicate_records(tmp_path):
    events = [
        make_event(event_id=f"E{i:03d}", amount=100 + i)
        for i in range(1, 6)
    ]

    input_file = tmp_path / "input.csv"
    output_dir = tmp_path / "output"

    pd.DataFrame(events).to_csv(input_file, index=False)

    # Simulate a crash after committing the first batch.
    with pytest.raises(RuntimeError, match="Simulated crash"):
        run_pipeline(
            input_file=input_file,
            output_dir=output_dir,
            batch_size=2,
            restart=True,
            simulate_crash_after_commit=1,
        )

    # Recover and continue without restarting.
    run_pipeline(
        input_file=input_file,
        output_dir=output_dir,
        batch_size=2,
    )

    # Verify that all records exist exactly once.
    processed_file = output_dir / "processed_events.csv"
    result = pd.read_csv(processed_file)

    assert len(result) == 5
    assert result["event_id"].nunique() == 5
    assert result["event_id"].duplicated().sum() == 0