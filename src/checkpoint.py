import json
import os
from pathlib import Path

import pandas as pd


CHECKPOINT_FILE = Path("data/output/checkpoint.json")


def load_checkpoint(checkpoint_file=CHECKPOINT_FILE):
    checkpoint_file = Path(checkpoint_file)

    if not checkpoint_file.exists():
        return {
            "last_batch_id": None,
            "last_offset": 0,
            "processed_event_ids": [],
            "purchase_state": {},
            "refunded_amounts": {},
            "updated_at": None,
        }

    with checkpoint_file.open("r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    checkpoint.setdefault("processed_event_ids", [])
    checkpoint.setdefault("purchase_state", {})
    checkpoint.setdefault("refunded_amounts", {})
    checkpoint.setdefault("last_offset", 0)

    return checkpoint


def make_json_safe(value):
    """Convert pandas and NumPy values into JSON-safe values."""

    if isinstance(value, dict):
        return {
            str(key): make_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [make_json_safe(item) for item in value]

    if hasattr(value, "item"):
        value = value.item()

    if pd.isna(value):
        return None

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    return str(value)


def save_checkpoint(
    processed_event_ids,
    last_batch_id,
    last_offset,
    checkpoint_file=CHECKPOINT_FILE,
    purchase_state=None,
    refunded_amounts=None,
):
    checkpoint_file = Path(checkpoint_file)
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

    checkpoint = {
        "last_batch_id": last_batch_id,
        "last_offset": int(last_offset),
        "processed_event_ids": sorted(
            set(str(event_id) for event_id in processed_event_ids)
        ),
        "purchase_state": make_json_safe(purchase_state or {}),
        "refunded_amounts": make_json_safe(refunded_amounts or {}),
        "updated_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }

    temporary_file = checkpoint_file.with_suffix(".tmp")

    try:
        with temporary_file.open("w", encoding="utf-8") as file:
            json.dump(checkpoint, file, indent=4, allow_nan=False)
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_file, checkpoint_file)

    finally:
        if temporary_file.exists():
            temporary_file.unlink()

    return checkpoint


def update_checkpoint(
    new_event_ids,
    last_batch_id,
    last_offset,
    checkpoint_file=CHECKPOINT_FILE,
):
    previous = load_checkpoint(checkpoint_file)

    all_event_ids = set(previous["processed_event_ids"])
    all_event_ids.update(str(event_id) for event_id in new_event_ids)

    return save_checkpoint(
        processed_event_ids=all_event_ids,
        last_batch_id=last_batch_id,
        last_offset=last_offset,
        checkpoint_file=checkpoint_file,
        purchase_state=previous["purchase_state"],
        refunded_amounts=previous["refunded_amounts"],
    )