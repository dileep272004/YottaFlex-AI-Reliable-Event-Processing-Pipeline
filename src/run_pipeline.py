import argparse
import json
import shutil
import time
import uuid
from pathlib import Path

import pandas as pd

from src.pipeline import process_events
from src.checkpoint import save_checkpoint


DEFAULT_INPUT = Path("data/input/large_events.csv")

# Use a NEW output folder for the first reliability test.
DEFAULT_OUTPUT = Path("data/output/reliable_run")


def write_json_atomic(data, path):
    """Write JSON safely using a temporary file and atomic replacement."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_suffix(path.suffix + ".tmp")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, allow_nan=False)
        file.flush()

    temporary.replace(path)


def read_committed_batches(batch_dir):
    """Return committed batch directories in batch-number order."""
    if not batch_dir.exists():
        return []

    batches = []

    for folder in batch_dir.iterdir():
        manifest_file = folder / "manifest.json"

        if folder.is_dir() and manifest_file.exists():
            with manifest_file.open("r", encoding="utf-8") as file:
                manifest = json.load(file)

            batches.append((int(manifest["batch_number"]), folder, manifest))

    return sorted(batches, key=lambda item: item[0])


def rebuild_outputs(batch_dir, output_dir):
    """Rebuild final CSV files only from committed batch directories."""
    output_files = {
        "processed": output_dir / "processed_events.csv",
        "rejected": output_dir / "rejected_events.csv",
        "duplicates": output_dir / "duplicate_events.csv",
    }

    batch_files = {
        "processed": "processed.csv",
        "rejected": "rejected.csv",
        "duplicates": "duplicates.csv",
    }

    for key, final_file in output_files.items():
        temporary_file = final_file.with_suffix(".rebuild.tmp")

        if temporary_file.exists():
            temporary_file.unlink()

        wrote_header = False

        with temporary_file.open("w", encoding="utf-8", newline="") as destination:
            for _, folder, _ in read_committed_batches(batch_dir):
                source_file = folder / batch_files[key]

                if not source_file.exists():
                    continue

                frame = pd.read_csv(source_file)

                if frame.empty:
                    if not wrote_header:
                        frame.to_csv(destination, index=False, header=True)
                        wrote_header = True
                    continue

                frame.to_csv(
                    destination,
                    index=False,
                    header=not wrote_header,
                )
                wrote_header = True

        if not wrote_header:
            pd.DataFrame().to_csv(temporary_file, index=False)

        temporary_file.replace(final_file)

    return output_files


def restore_state(input_file, processed_file, offset, batch_size):
    """
    Reconstruct processing state from committed outputs and source input.

    Event IDs are read from the source up to the committed offset.
    Purchases and successful refunds are reconstructed from processed output.
    """
    if offset == 0:
        return set(), {}, {}
    
    existing_ids = set()

    if offset > 0:
        reader = pd.read_csv(input_file, chunksize=batch_size)

        rows_read = 0

        for chunk in reader:
            remaining = offset - rows_read

            if remaining <= 0:
                break

            chunk = chunk.iloc[:remaining]
            rows_read += len(chunk)

            if "event_id" in chunk.columns:
                ids = (
                    chunk["event_id"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                )
                existing_ids.update(value for value in ids if value)

    purchase_state = {}
    refunded_amounts = {}

    if processed_file.exists() and processed_file.stat().st_size > 0:
        processed = pd.read_csv(processed_file)

        if not processed.empty:
            purchases = processed[
                processed["event_type"] == "purchase"
            ]

            for _, event in purchases.iterrows():
                purchase_state[str(event["event_id"])] = event.to_dict()

            refunds = processed[
                processed["event_type"] == "refund"
            ]

            if not refunds.empty:
                totals = refunds.groupby("reference_event_id")[
                    "amount_inr"
                ].sum()

                refunded_amounts = {
                    str(reference): float(amount)
                    for reference, amount in totals.items()
                }

    return existing_ids, purchase_state, refunded_amounts


def build_purchase_lookup(input_file):
    """Index individually valid purchase events for reference validation."""
    from src.validate_data import validate_events

    purchase_lookup = {}

    for chunk in pd.read_csv(input_file, chunksize=10000):
        validation_results = validate_events(chunk)

        for position, errors in enumerate(validation_results):
            if errors:
                continue

            event = chunk.iloc[position]

            if event["event_type"] != "purchase":
                continue

            event_id = str(event["event_id"]).strip()

            if event_id and event_id not in purchase_lookup:
                purchase_lookup[event_id] = event.to_dict()

    return purchase_lookup


def commit_batch(
    batch_number,
    batch_start,
    batch_end,
    processed,
    rejected,
    duplicates,
    batch_dir,
):
    """Prepare a batch and atomically publish its committed directory."""
    batch_name = f"batch_{batch_number:05d}"
    committed_dir = batch_dir / batch_name
    temporary_dir = batch_dir / f".{batch_name}_{uuid.uuid4().hex}.tmp"

    if committed_dir.exists():
        raise FileExistsError(
            f"Batch already committed: {committed_dir}"
        )

    temporary_dir.mkdir(parents=True)

    try:
        processed.to_csv(
            temporary_dir / "processed.csv",
            index=False,
        )
        rejected.to_csv(
            temporary_dir / "rejected.csv",
            index=False,
        )
        duplicates.to_csv(
            temporary_dir / "duplicates.csv",
            index=False,
        )

        manifest = {
            "batch_number": batch_number,
            "batch_id": batch_name,
            "start_offset": batch_start,
            "end_offset": batch_end,
            "processed_count": len(processed),
            "rejected_count": len(rejected),
            "duplicate_count": len(duplicates),
            "committed_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }

        write_json_atomic(manifest, temporary_dir / "manifest.json")

        # Same-filesystem directory rename publishes the completed batch.
        temporary_dir.replace(committed_dir)

    except Exception:
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir, ignore_errors=True)
        raise

    return manifest


def run_pipeline(
    input_file=DEFAULT_INPUT,
    output_dir=DEFAULT_OUTPUT,
    batch_size=1000,
    restart=False,
    simulate_crash_after_commit=None,
):
    input_file = Path(input_file)
    output_dir = Path(output_dir)
    batch_dir = output_dir / "batches"
    checkpoint_file = output_dir / "pipeline_checkpoint.json"

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero")

    output_dir.mkdir(parents=True, exist_ok=True)
    batch_dir.mkdir(parents=True, exist_ok=True)

    if restart:
        shutil.rmtree(batch_dir, ignore_errors=True)

        for filename in [
            "processed_events.csv",
            "rejected_events.csv",
            "duplicate_events.csv",
            "pipeline_checkpoint.json",
        ]:
            file_path = output_dir / filename
            if file_path.exists():
                file_path.unlink()

    print("Building purchase reference index...")
    purchase_lookup = build_purchase_lookup(input_file)
    print("Indexed purchases:", len(purchase_lookup))

    # Discard incomplete temporary directories. They were never committed.
    for temporary_dir in batch_dir.glob(".*.tmp"):
        if temporary_dir.is_dir():
            shutil.rmtree(temporary_dir, ignore_errors=True)

    committed = read_committed_batches(batch_dir)

    # Rebuild outputs from committed batches. This also repairs interrupted
    # publishing where a batch was committed but final CSVs were not updated.
    output_files = rebuild_outputs(batch_dir, output_dir)

    offset = (
        committed[-1][2]["end_offset"]
        if committed else 0
    )

    existing_ids, purchase_state, refunded_amounts = restore_state(
        input_file,
        output_files["processed"],
        offset,
        batch_size,
    )

    if committed:
        latest = committed[-1][2]

        save_checkpoint(
            processed_event_ids=existing_ids,
            last_batch_id=latest["batch_id"],
            last_offset=offset,
            checkpoint_file=checkpoint_file,
            purchase_state=purchase_state,
            refunded_amounts=refunded_amounts,
        )

    print("Committed batches:", len(committed))
    print("Recovered offset:", offset)

    start_time = time.perf_counter()
    total_processed = 0
    total_rejected = 0
    total_duplicates = 0

    batch_number = offset // batch_size + 1

    reader = pd.read_csv(input_file, chunksize=batch_size)

    rows_seen = 0

    for original_chunk in reader:
        chunk_start = rows_seen
        rows_seen += len(original_chunk)
        chunk_end = rows_seen

        if chunk_end <= offset:
            continue

        chunk = original_chunk.copy()

        if chunk_start < offset:
            chunk = chunk.iloc[offset - chunk_start:].copy()

        if chunk.empty:
            continue

        batch_number = (chunk_start // batch_size) + 1
        batch_name = f"batch_{batch_number:05d}"
        committed_dir = batch_dir / batch_name

        if committed_dir.exists():
            continue

        # Work on copies. State becomes recoverable only after batch commit.
        next_existing_ids = set(existing_ids)
        next_purchase_state = dict(purchase_state)
        next_refunded_amounts = dict(refunded_amounts)

        processed, rejected, duplicates = process_events(
            chunk,
            existing_event_ids=next_existing_ids,
            purchase_state=next_purchase_state,
            refunded_amounts=next_refunded_amounts,
            purchase_lookup=purchase_lookup,
        )

        if "event_id" in chunk.columns:
            ids = (
                chunk["event_id"]
                .dropna()
                .astype(str)
                .str.strip()
            )
            next_existing_ids.update(value for value in ids if value)

        manifest = commit_batch(
            batch_number,
            chunk_start,
            chunk_end,
            processed,
            rejected,
            duplicates,
            batch_dir,
        )

        # Simulate a crash after the batch is committed but before the
        # checkpoint and final CSVs are updated.
        if simulate_crash_after_commit == batch_number:
            raise RuntimeError(
                f"Simulated crash after committing {batch_name}"
            )

        existing_ids = next_existing_ids
        purchase_state = next_purchase_state
        refunded_amounts = next_refunded_amounts

        # Rebuild final output files from committed batch directories.
        output_files = rebuild_outputs(batch_dir, output_dir)

        save_checkpoint(
            processed_event_ids=existing_ids,
            last_batch_id=manifest["batch_id"],
            last_offset=chunk_end,
            checkpoint_file=checkpoint_file,
            purchase_state=purchase_state,
            refunded_amounts=refunded_amounts,
        )

        total_processed += len(processed)
        total_rejected += len(rejected)
        total_duplicates += len(duplicates)

        offset = chunk_end

        print(
            f"{batch_name}: "
            f"read={len(chunk)}, "
            f"processed={len(processed)}, "
            f"rejected={len(rejected)}, "
            f"duplicates={len(duplicates)}, "
            f"offset={offset}"
        )

    elapsed = time.perf_counter() - start_time

    print("\n========== PIPELINE SUMMARY ==========")
    print("Final offset:", offset)
    print("Processed this run:", total_processed)
    print("Rejected this run:", total_rejected)
    print("Duplicates this run:", total_duplicates)
    print("Known event IDs:", len(existing_ids))
    print("Remembered purchases:", len(purchase_state))
    print("Indexed purchases:", len(purchase_lookup))
    print("Tracked refund references:", len(refunded_amounts))
    print("Processing time:", round(elapsed, 2), "seconds")
    print("Output directory:", output_dir)
    print("Checkpoint:", checkpoint_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--simulate-crash-after-commit", type=int)

    args = parser.parse_args()

    run_pipeline(
        input_file=args.input,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        restart=args.restart,
        simulate_crash_after_commit=args.simulate_crash_after_commit,
    )