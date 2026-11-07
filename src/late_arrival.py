import pandas as pd

# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

# An event is considered late if it arrives
# more than 10 minutes after its event time.

LATE_THRESHOLD_MINUTES = 10


# --------------------------------------------------
# 2. TRACK LATE-ARRIVING EVENTS
# --------------------------------------------------

def track_late_arrivals(df):

    # Check whether the required columns exist.
    required_columns = [
        "event_time",
        "ingestion_time"
    ]

    for column in required_columns:
        if column not in df.columns:
            raise ValueError(
                f"Missing required column: {column}"
            )

    # Work on a copy so the original DataFrame
    # is not modified.
    events = df.copy()

    # --------------------------------------------------
    # 3. CONVERT TIMESTAMPS
    # --------------------------------------------------

    events["event_time"] = pd.to_datetime(
        events["event_time"],
        errors="coerce"
    )

    events["ingestion_time"] = pd.to_datetime(
        events["ingestion_time"],
        errors="coerce"
    )

    # --------------------------------------------------
    # 4. CALCULATE ARRIVAL DELAY
    # --------------------------------------------------

    events["arrival_delay_minutes"] = (
        events["ingestion_time"] - events["event_time"]
    ).dt.total_seconds() / 60

    # --------------------------------------------------
    # 5. IDENTIFY INVALID TIMESTAMPS
    # --------------------------------------------------

    invalid_timestamp = (
        events["event_time"].isna()
        | events["ingestion_time"].isna()
    )

    # --------------------------------------------------
    # 6. IDENTIFY LATE EVENTS
    # --------------------------------------------------

    events["is_late"] = (
        events["arrival_delay_minutes"]
        > LATE_THRESHOLD_MINUTES
    )

    # Invalid timestamps must not be classified as late.
    events.loc[invalid_timestamp, "is_late"] = False

    # --------------------------------------------------
    # 7. ASSIGN ARRIVAL STATUS
    # --------------------------------------------------

    events["arrival_status"] = "on_time"

    # Events arriving more than 10 minutes late.
    events.loc[
        events["is_late"],
        "arrival_status"
    ] = "late"

    # Events containing invalid timestamps.
    events.loc[
        invalid_timestamp,
        "arrival_status"
    ] = "invalid_timestamp"

    # --------------------------------------------------
    # 8. RETURN PROCESSED EVENTS
    # --------------------------------------------------

    return events