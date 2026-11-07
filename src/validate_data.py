import math
import numpy as np
import pandas as pd


# --------------------------------------------------
# 1. ALLOWED VALUES
# --------------------------------------------------

VALID_EVENT_TYPES = {
    "purchase",
    "refund",
    "cancellation"
}

VALID_PAYMENT_METHODS = {
    "upi",
    "credit_card",
    "debit_card",
    "net_banking",
    "wallet"
}

REQUIRED_FIELDS = [
    "event_id",
    "order_id",
    "customer_id",
    "product_id",
    "event_type",
    "payment_method",
    "quantity",
    "amount_inr"
]


# --------------------------------------------------
# 2. HELPER FUNCTION: CHECK MISSING VALUES
# --------------------------------------------------

def is_missing(value):
    """Check whether a value is missing or blank."""

    if pd.isna(value):
        return True

    if isinstance(value, str) and value.strip() == "":
        return True

    return False


# --------------------------------------------------
# 3. VALIDATE ONE EVENT
# --------------------------------------------------

def validate_event(event):
    """
    Validate one event.

    Kept for compatibility with existing pipeline code.
    """

    errors = []

    for field in REQUIRED_FIELDS:
        value = event.get(field)

        if is_missing(value):
            errors.append(f"Missing required field: {field}")

    event_type = event.get("event_type")

    if not is_missing(event_type):
        if event_type not in VALID_EVENT_TYPES:
            errors.append("Invalid event type")

    payment_method = event.get("payment_method")

    if not is_missing(payment_method):
        if payment_method not in VALID_PAYMENT_METHODS:
            errors.append("Invalid payment method")

    try:
        quantity = float(event.get("quantity"))

        if not math.isfinite(quantity):
            errors.append("Quantity must be a valid number")

        elif quantity <= 0 or not quantity.is_integer():
            errors.append("Quantity must be a positive integer")

    except (ValueError, TypeError, OverflowError):
        if not is_missing(event.get("quantity")):
            errors.append("Quantity must be a valid number")

    try:
        amount = float(event.get("amount_inr"))

        if not math.isfinite(amount):
            errors.append("Amount must be a valid number")

        elif event_type == "purchase" and amount <= 0:
            errors.append("Purchase amount must be positive")

        elif event_type == "refund" and amount <= 0:
            errors.append("Refund amount must be positive")

        elif event_type == "cancellation" and amount != 0:
            errors.append("Cancellation amount must be zero")

    except (ValueError, TypeError, OverflowError):
        if not is_missing(event.get("amount_inr")):
            errors.append("Amount must be a valid number")

    reference_event_id = event.get("reference_event_id")

    if not is_missing(event_type):

        if event_type == "purchase":
            if not is_missing(reference_event_id):
                errors.append(
                    "Purchase must not have a reference event ID"
                )

        elif event_type in {"refund", "cancellation"}:
            if is_missing(reference_event_id):
                errors.append("Missing reference event ID")

    event_time = event.get("event_time")

    if is_missing(event_time):
        errors.append("Invalid event timestamp")
        parsed_event_time = None
    else:
        parsed_event_time = pd.to_datetime(
            event_time,
            errors="coerce"
        )

        if pd.isna(parsed_event_time):
            errors.append("Invalid event timestamp")

    ingestion_time = event.get("ingestion_time")

    if is_missing(ingestion_time):
        errors.append("Missing ingestion timestamp")
        parsed_ingestion_time = None
    else:
        parsed_ingestion_time = pd.to_datetime(
            ingestion_time,
            errors="coerce"
        )

        if pd.isna(parsed_ingestion_time):
            errors.append("Invalid ingestion timestamp")

    if (
        parsed_event_time is not None
        and parsed_ingestion_time is not None
        and not pd.isna(parsed_event_time)
        and not pd.isna(parsed_ingestion_time)
    ):
        if parsed_ingestion_time < parsed_event_time:
            errors.append(
                "Ingestion time cannot be earlier than event time"
            )

    return errors


# --------------------------------------------------
# 4. VECTORISED VALIDATION FOR COMPLETE DATASETS
# --------------------------------------------------

def validate_events(df):
    """
    Validate all rows using vectorised Pandas operations.

    Returns:
        A list containing one list of validation errors per input row.

    The output order matches the input DataFrame row order.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a Pandas DataFrame")

    row_count = len(df)

    if row_count == 0:
        return []

    # Missing columns are treated as entirely missing fields.
    def get_column(column_name):
        if column_name in df.columns:
            return df[column_name]
        return pd.Series(
            [None] * row_count,
            index=df.index
        )

    # Detect missing and blank values in a complete column.
    def missing_mask(series):
        return (
            series.isna()
            | series.astype("string").str.strip().eq("").fillna(False)
        ).to_numpy(dtype=bool)

    errors = [[] for _ in range(row_count)]

    # Store masks as positional arrays so duplicate DataFrame
    # indexes do not affect the results.
    missing = {}

    for field in REQUIRED_FIELDS:
        missing[field] = missing_mask(get_column(field))

    event_type = get_column("event_type")
    payment_method = get_column("payment_method")
    reference_id = get_column("reference_event_id")

    event_types = event_type.astype("string").fillna("").to_numpy(dtype=str)

    # --------------------------------------------------
    # A. REQUIRED FIELDS
    # --------------------------------------------------

    for field in REQUIRED_FIELDS:
        for position in np.flatnonzero(missing[field]):
            errors[position].append(
                f"Missing required field: {field}"
            )

    # --------------------------------------------------
    # B. EVENT TYPE
    # --------------------------------------------------

    invalid_event_type = (
        ~missing["event_type"]
        & ~event_type.isin(VALID_EVENT_TYPES).to_numpy()
    )

    for position in np.flatnonzero(invalid_event_type):
        errors[position].append("Invalid event type")

    # --------------------------------------------------
    # C. PAYMENT METHOD
    # --------------------------------------------------

    invalid_payment = (
        ~missing["payment_method"]
        & ~payment_method.isin(VALID_PAYMENT_METHODS).to_numpy()
    )

    for position in np.flatnonzero(invalid_payment):
        errors[position].append("Invalid payment method")

    # --------------------------------------------------
    # D. QUANTITY
    # --------------------------------------------------

    quantity = pd.to_numeric(
        get_column("quantity"),
        errors="coerce"
    ).to_numpy(dtype=float, na_value=np.nan)

    invalid_quantity_number = (
        ~missing["quantity"]
        & (
            ~np.isfinite(quantity)
        )
    )

    invalid_quantity_integer = (
        ~missing["quantity"]
        & np.isfinite(quantity)
        & (
            (quantity <= 0)
            | (quantity % 1 != 0)
        )
    )

    for position in np.flatnonzero(invalid_quantity_number):
        errors[position].append(
            "Quantity must be a valid number"
        )

    for position in np.flatnonzero(invalid_quantity_integer):
        errors[position].append(
            "Quantity must be a positive integer"
        )

    # --------------------------------------------------
    # E. AMOUNT
    # --------------------------------------------------

    amount = pd.to_numeric(
        get_column("amount_inr"),
        errors="coerce"
    ).to_numpy(dtype=float, na_value=np.nan)

    invalid_amount_number = (
        ~missing["amount_inr"]
        & ~np.isfinite(amount)
    )

    for position in np.flatnonzero(invalid_amount_number):
        errors[position].append("Amount must be a valid number")

    valid_amount = (
        ~missing["amount_inr"]
        & np.isfinite(amount)
    )

    purchase_invalid = (
        valid_amount
        & (event_types == "purchase")
        & (amount <= 0)
    )

    refund_invalid = (
        valid_amount
        & (event_types == "refund")
        & (amount <= 0)
    )

    cancellation_invalid = (
        valid_amount
        & (event_types == "cancellation")
        & (amount != 0)
    )

    for position in np.flatnonzero(purchase_invalid):
        errors[position].append(
            "Purchase amount must be positive"
        )

    for position in np.flatnonzero(refund_invalid):
        errors[position].append(
            "Refund amount must be positive"
        )

    for position in np.flatnonzero(cancellation_invalid):
        errors[position].append(
            "Cancellation amount must be zero"
        )

    # --------------------------------------------------
    # F. REFERENCE EVENT ID
    # --------------------------------------------------

    reference_missing = missing_mask(reference_id)

    purchase_without_reference = (
        (event_types == "purchase")
        & ~reference_missing
    )

    refund_or_cancellation_without_reference = (
        np.isin(event_types, ["refund", "cancellation"])
        & reference_missing
        & ~missing["event_type"]
    )

    for position in np.flatnonzero(purchase_without_reference):
        errors[position].append(
            "Purchase must not have a reference event ID"
        )

    for position in np.flatnonzero(
        refund_or_cancellation_without_reference
    ):
        errors[position].append("Missing reference event ID")

    # --------------------------------------------------
    # G. EVENT TIMESTAMP
    # --------------------------------------------------

    event_time = get_column("event_time")

    parsed_event_time = pd.to_datetime(
        event_time,
        errors="coerce",
        format="mixed",
        utc=True
    )

    invalid_event_time = parsed_event_time.isna().to_numpy()

    for position in np.flatnonzero(invalid_event_time):
        errors[position].append("Invalid event timestamp")

    # --------------------------------------------------
    # H. INGESTION TIMESTAMP
    # --------------------------------------------------

    ingestion_time = get_column("ingestion_time")

    parsed_ingestion_time = pd.to_datetime(
        ingestion_time,
        errors="coerce",
        format="mixed",
        utc=True
    )

    invalid_ingestion_time = parsed_ingestion_time.isna().to_numpy()

    missing_ingestion_time = missing_mask(ingestion_time)

    for position in np.flatnonzero(missing_ingestion_time):
        errors[position].append("Missing ingestion timestamp")

    invalid_nonmissing_ingestion = (
        invalid_ingestion_time
        & ~missing_ingestion_time
    )

    for position in np.flatnonzero(invalid_nonmissing_ingestion):
        errors[position].append("Invalid ingestion timestamp")

    # --------------------------------------------------
    # I. TIMESTAMP ORDER
    # --------------------------------------------------

    comparable_timestamps = (
        ~invalid_event_time
        & ~invalid_ingestion_time
    )

    timestamp_order_invalid = np.zeros(
        row_count,
        dtype=bool
    )

    valid_positions = np.flatnonzero(comparable_timestamps)

    if len(valid_positions) > 0:
        timestamp_order_invalid[valid_positions] = (
            parsed_ingestion_time.iloc[valid_positions].reset_index(drop=True)
            < parsed_event_time.iloc[valid_positions].reset_index(drop=True)
        ).to_numpy()

    for position in np.flatnonzero(timestamp_order_invalid):
        errors[position].append(
            "Ingestion time cannot be earlier than event time"
        )

    return errors