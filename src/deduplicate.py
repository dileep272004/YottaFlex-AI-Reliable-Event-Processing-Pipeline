import pandas as pd


# --------------------------------------------------
# REMOVE DUPLICATE EVENTS
# --------------------------------------------------

def deduplicate_events(df, existing_event_ids=None):
    """
    Identify duplicate event records.

    Handles:
    1. Duplicates within the current batch.
    2. Event IDs already processed in earlier batches.

    Parameters:
        df: Pandas DataFrame containing incoming events.
        existing_event_ids: Event IDs already stored or processed.

    Returns:
        unique_events: Records eligible for further processing.
        duplicate_events: Duplicate records with rejection reasons.
    """

    # Check whether the required column exists.
    if "event_id" not in df.columns:
        raise ValueError("Input DataFrame must contain event_id column")

    # If no previous event IDs are provided,
    # use an empty set.
    if existing_event_ids is None:
        existing_event_ids = set()
    else:
        existing_event_ids = set(existing_event_ids)

    # Make a copy so that the original DataFrame is not modified.
    events = df.copy()

    # Normalize event IDs for comparison.
    # Missing and blank IDs are left out of duplicate detection.
    event_ids = events["event_id"].astype("string").str.strip()

    valid_id_mask = (
        events["event_id"].notna()
        & event_ids.ne("").fillna(False)
    )

    # --------------------------------------------------
    # 1. CHECK DUPLICATES FROM PREVIOUS BATCHES
    # --------------------------------------------------

    previous_batch_mask = (
        valid_id_mask
        & event_ids.isin(existing_event_ids)
    )

    # --------------------------------------------------
    # 2. CHECK DUPLICATES WITHIN CURRENT BATCH
    # --------------------------------------------------

    current_batch_mask = (
        valid_id_mask
        & event_ids.duplicated(keep="first")
    )

    # --------------------------------------------------
    # 3. COMBINE BOTH DUPLICATE CHECKS
    # --------------------------------------------------

    duplicate_mask = (
        previous_batch_mask
        | current_batch_mask
    )

    # --------------------------------------------------
    # 4. SEPARATE UNIQUE AND DUPLICATE EVENTS
    # --------------------------------------------------

    unique_events = events.loc[~duplicate_mask].copy()

    duplicate_events = events.loc[duplicate_mask].copy()

    # --------------------------------------------------
    # 5. ADD REJECTION REASONS
    # --------------------------------------------------

    duplicate_events["rejection_reason"] = (
        "Duplicate event ID within current batch"
    )

    # Give a separate reason to records already processed earlier.
    duplicate_events.loc[
        previous_batch_mask.loc[duplicate_events.index],
        "rejection_reason"
    ] = "Event ID already processed in an earlier batch"

    # --------------------------------------------------
    # 6. RETURN RESULTS
    # --------------------------------------------------

    return unique_events, duplicate_events