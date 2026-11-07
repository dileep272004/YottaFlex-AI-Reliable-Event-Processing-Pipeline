import pandas as pd

from src.pipeline import process_events


# Read the input dataset
input_file = "data/input/edge_case_events.csv"

df = pd.read_csv(input_file)

print("Total input events:", len(df))


# Run the complete pipeline
processed_events, rejected_events, duplicate_events = process_events(df)


# Display the results
print("\n--- PIPELINE RESULTS ---")

print("Accepted events:", len(processed_events))
print("Validation rejected:", len(rejected_events))
print("Duplicate rejected:", len(duplicate_events))


print("\n--- ACCEPTED EVENTS ---")
print(
    processed_events[
        ["event_id", "amount_inr", "arrival_delay_minutes", "arrival_status"]
    ].to_string(index=False)
)


print("\n--- VALIDATION REJECTIONS ---")
if not rejected_events.empty:
    print(
        rejected_events[
            ["event_id", "rejection_reason"]
        ].to_string(index=False)
    )
else:
    print("No validation rejections")


print("\n--- DUPLICATE REJECTIONS ---")
if not duplicate_events.empty:
    print(
        duplicate_events[
            ["event_id", "amount_inr", "rejection_reason"]
        ].to_string(index=False)
    )
else:
    print("No duplicate events")


# Check whether all records are accounted for
total_accounted = (
    len(processed_events)
    + len(rejected_events)
    + len(duplicate_events)
)

print("\n--- RECONCILIATION ---")
print("Total input:", len(df))
print("Total accounted for:", total_accounted)

if len(df) == total_accounted:
    print("RECONCILIATION PASSED")
else:
    print("RECONCILIATION FAILED")