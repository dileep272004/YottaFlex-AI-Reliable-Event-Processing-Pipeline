import pandas as pd

from src.validate_data import validate_events
from src.deduplicate import deduplicate_events
from src.late_arrival import track_late_arrivals


# --------------------------------------------------
# 1. REFERENCE AND REFUND VALIDATION
# --------------------------------------------------

def validate_references_and_refunds(
    df,
    purchase_state=None,
    refunded_amounts=None,
    purchase_lookup=None
):
    """
    Validate purchase references and cumulative refunds.

    purchase_state:
        Purchases encountered during actual batch processing.

    purchase_lookup:
        Valid purchases indexed from the complete input file.
        This supports references to purchases arriving later.

    refunded_amounts:
        Cumulative successful refunds per purchase.
    """

    events = df.copy().reset_index(drop=True)

    if purchase_state is None:
        purchase_state = {}

    if refunded_amounts is None:
        refunded_amounts = {}

    if purchase_lookup is None:
        purchase_lookup = {}

    if events.empty:
        rejected = events.copy()
        rejected["rejection_reason"] = pd.Series(dtype="object")
        return events, rejected

    rejected_reasons = {}

    events["_ingestion_sort"] = pd.to_datetime(
        events["ingestion_time"],
        errors="coerce",
        utc=True
    )

    sorted_events = events.sort_values(
        by="_ingestion_sort",
        kind="mergesort"
    )

    matching_fields = [
        "order_id",
        "customer_id",
        "product_id",
        "payment_method"
    ]

    for index, event in sorted_events.iterrows():

        event_type = event["event_type"]
        event_id = str(event["event_id"])

        # Remember purchases encountered in this batch.
        if event_type == "purchase":
            purchase_state[event_id] = event.to_dict()
            continue

        reference_id = str(event["reference_event_id"])

        # First check previously encountered purchases.
        purchase = purchase_state.get(reference_id)

        # If not encountered yet, check the complete-file index.
        if purchase is None:
            purchase = purchase_lookup.get(reference_id)

        if purchase is None:
            rejected_reasons[index] = (
                "Refund/cancellation references a nonexistent purchase"
            )
            continue

        if purchase["event_type"] != "purchase":
            rejected_reasons[index] = (
                "Reference event must be a purchase"
            )
            continue

        # Verify related event details.
        mismatch = any(
            str(event[field]) != str(purchase[field])
            for field in matching_fields
        )

        if mismatch:
            rejected_reasons[index] = (
                "Referenced purchase details do not match"
            )
            continue

        # Enforce cumulative refund limit.
        if event_type == "refund":

            original_amount = float(purchase["amount_inr"])
            refund_amount = float(event["amount_inr"])

            already_refunded = refunded_amounts.get(
                reference_id, 0.0
            )

            remaining_refundable = (
                original_amount - already_refunded
            )

            if refund_amount > remaining_refundable:
                rejected_reasons[index] = (
                    "Refund exceeds the remaining refundable amount"
                )
                continue

            refunded_amounts[reference_id] = (
                already_refunded + refund_amount
            )

    rejected_indices = list(rejected_reasons.keys())

    rejected_events = events.loc[rejected_indices].copy()

    if not rejected_events.empty:
        rejected_events["rejection_reason"] = [
            rejected_reasons[index]
            for index in rejected_events.index
        ]

    valid_events = events.drop(
        index=rejected_indices
    ).copy()

    valid_events.drop(
        columns=["_ingestion_sort"],
        inplace=True,
        errors="ignore"
    )

    rejected_events.drop(
        columns=["_ingestion_sort"],
        inplace=True,
        errors="ignore"
    )

    return (
        valid_events.reset_index(drop=True),
        rejected_events.reset_index(drop=True)
    )


# --------------------------------------------------
# 2. OUT-OF-ORDER EVENT DETECTION
# --------------------------------------------------

def detect_out_of_order_events(df):

    events = df.copy().reset_index(drop=True)

    events["is_out_of_order"] = False

    if events.empty:
        return events

    events["_ingestion_sort"] = pd.to_datetime(
        events["ingestion_time"],
        errors="coerce",
        utc=True
    )

    events["_event_sort"] = pd.to_datetime(
        events["event_time"],
        errors="coerce",
        utc=True
    )

    sorted_events = events.sort_values(
        by="_ingestion_sort",
        kind="mergesort"
    )

    previous_event_time = sorted_events[
        "_event_sort"
    ].shift(1)

    out_of_order_mask = (
        sorted_events["_event_sort"] < previous_event_time
    ).fillna(False)

    events.loc[
        sorted_events.index,
        "is_out_of_order"
    ] = out_of_order_mask.to_numpy(dtype=bool)

    events.drop(
        columns=["_ingestion_sort", "_event_sort"],
        inplace=True
    )

    return events


# --------------------------------------------------
# 3. MAIN PIPELINE FUNCTION
# --------------------------------------------------

def process_events(
    df,
    existing_event_ids=None,
    purchase_state=None,
    refunded_amounts=None,
    purchase_lookup=None
):
    """
    Process incoming e-commerce events.

    Supports cross-batch state and references to
    purchases arriving later in the input file.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            "Input must be a pandas DataFrame"
        )

    events = df.copy().reset_index(drop=True)

    if purchase_state is None:
        purchase_state = {}

    if refunded_amounts is None:
        refunded_amounts = {}

    if purchase_lookup is None:
        purchase_lookup = {}

    if events.empty:

        processed = events.copy()

        processed["arrival_delay_minutes"] = pd.Series(
            dtype="float64"
        )
        processed["is_late"] = pd.Series(dtype="bool")
        processed["arrival_status"] = pd.Series(dtype="object")
        processed["is_out_of_order"] = pd.Series(dtype="bool")

        rejected = events.copy()
        rejected["rejection_reason"] = pd.Series(dtype="object")

        duplicates = events.copy()
        duplicates["rejection_reason"] = pd.Series(dtype="object")

        return processed, rejected, duplicates

    # STEP 1: INDIVIDUAL VALIDATION

    validation_results = validate_events(events)

    valid_positions = [
        position
        for position, errors in enumerate(validation_results)
        if not errors
    ]

    rejected_positions = [
        position
        for position, errors in enumerate(validation_results)
        if errors
    ]

    valid_df = events.iloc[valid_positions].copy()

    rejected_df = events.iloc[rejected_positions].copy()

    if not rejected_df.empty:
        rejected_df["rejection_reason"] = [
            "; ".join(validation_results[position])
            for position in rejected_positions
        ]

    rejected_df.reset_index(drop=True, inplace=True)

    # STEP 2: DEDUPLICATION

    unique_events, duplicate_events = deduplicate_events(
        valid_df,
        existing_event_ids=existing_event_ids
    )

    unique_events = unique_events.reset_index(drop=True)
    duplicate_events = duplicate_events.reset_index(drop=True)

    # STEP 3: REFERENCE AND REFUND VALIDATION

    (
        reference_valid_events,
        reference_rejected_events
    ) = validate_references_and_refunds(
        unique_events,
        purchase_state=purchase_state,
        refunded_amounts=refunded_amounts,
        purchase_lookup=purchase_lookup
    )

    rejected_df = pd.concat(
        [
            rejected_df,
            reference_rejected_events
        ],
        ignore_index=True
    )

    # STEP 4: LATE ARRIVAL TRACKING

    processed_events = track_late_arrivals(
        reference_valid_events
    )

    # STEP 5: OUT-OF-ORDER DETECTION

    processed_events = detect_out_of_order_events(
        processed_events
    )

    return (
        processed_events,
        rejected_df,
        duplicate_events
    )