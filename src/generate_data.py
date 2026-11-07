import random
import pandas as pd

from datetime import datetime, timedelta
from pathlib import Path


# ==================================================
# 1. CONFIGURATION
# ==================================================

BASE_TIME = datetime(2026, 11, 6, 10, 0, 0)

EVENT_TYPES = ["purchase", "refund", "cancellation"]

EVENT_WEIGHTS = [80, 15, 5]

CUSTOMERS = [f"C{i:03d}" for i in range(1, 101)]

PRODUCTS = [f"P{i:03d}" for i in range(1, 51)]

PAYMENT_METHODS = [
    "upi",
    "credit_card",
    "debit_card",
    "net_banking",
    "wallet"
]

OUTPUT_COLUMNS = [
    "event_id",
    "order_id",
    "reference_event_id",
    "customer_id",
    "product_id",
    "event_type",
    "payment_method",
    "quantity",
    "amount_inr",
    "event_time",
    "ingestion_time"
]


# ==================================================
# 2. GENERATE UNIQUE EVENT ID
# ==================================================

def generate_event_id(number):
    return f"E{number:06d}"


# ==================================================
# 3. GENERATE ONE EVENT
# ==================================================

def generate_event(
    number,
    rng,
    refundable_purchases,
    cancellable_purchases
):

    event_id = generate_event_id(number)

    # Event time increases by one minute.
    event_time = BASE_TIME + timedelta(minutes=number)

    # Ingestion can be delayed.
    ingestion_delay = rng.choice([0, 1, 2, 5, 10, 30, 60])

    ingestion_time = event_time + timedelta(
        minutes=ingestion_delay
    )

    # Select event type using weighted probabilities.
    selected_event_type = rng.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]

    # --------------------------------------------------
    # CASE 1: REFUND EVENT
    # --------------------------------------------------

    if (
        selected_event_type == "refund"
        and refundable_purchases
    ):

        # Select one purchase that can still be refunded.
        purchase_index = rng.randrange(
            len(refundable_purchases)
        )

        original_purchase = refundable_purchases[
            purchase_index
        ]

        remaining_amount = original_purchase[
            "remaining_refund_amount"
        ]

        # If cancelled, refund the full remaining amount.
        # Otherwise, allow a partial or full refund.
        if original_purchase["is_cancelled"]:

            refund_amount = remaining_amount

        else:

            refund_amount = rng.randint(
                1,
                remaining_amount
            )

        # Update the remaining refundable amount.
        original_purchase["remaining_refund_amount"] -= (
            refund_amount
        )

        # Remove fully refunded purchases from the
        # refundable list using efficient list removal.
        if original_purchase["remaining_refund_amount"] == 0:

            last_purchase = refundable_purchases.pop()

            if purchase_index < len(refundable_purchases):

                refundable_purchases[purchase_index] = (
                    last_purchase
                )

        event = {
            "event_id": event_id,
            "order_id": original_purchase["order_id"],
            "reference_event_id": original_purchase["event_id"],
            "customer_id": original_purchase["customer_id"],
            "product_id": original_purchase["product_id"],
            "event_type": "refund",
            "payment_method": original_purchase["payment_method"],
            "quantity": original_purchase["quantity"],
            "amount_inr": refund_amount,
            "event_time": event_time.isoformat(),
            "ingestion_time": ingestion_time.isoformat()
        }

        return event

    # --------------------------------------------------
    # CASE 2: CANCELLATION EVENT
    # --------------------------------------------------

    if (
        selected_event_type == "cancellation"
        and cancellable_purchases
    ):

        # Select a purchase that has not been cancelled.
        purchase_index = rng.randrange(
            len(cancellable_purchases)
        )

        original_purchase = cancellable_purchases.pop(
            purchase_index
        )

        # Mark the original purchase as cancelled.
        original_purchase["is_cancelled"] = True

        event = {
            "event_id": event_id,
            "order_id": original_purchase["order_id"],
            "reference_event_id": original_purchase["event_id"],
            "customer_id": original_purchase["customer_id"],
            "product_id": original_purchase["product_id"],
            "event_type": "cancellation",
            "payment_method": original_purchase["payment_method"],
            "quantity": original_purchase["quantity"],
            "amount_inr": 0.0,
            "event_time": event_time.isoformat(),
            "ingestion_time": ingestion_time.isoformat()
        }

        return event

    # --------------------------------------------------
    # CASE 3: PURCHASE EVENT
    # --------------------------------------------------

    # If no suitable purchase exists for a refund or
    # cancellation, generate a new purchase instead.

    customer_id = rng.choice(CUSTOMERS)

    product_id = rng.choice(PRODUCTS)

    payment_method = rng.choice(PAYMENT_METHODS)

    quantity = rng.randint(1, 5)

    unit_price = rng.randint(100, 5000)

    amount_inr = quantity * unit_price

    order_id = f"O{number:06d}"

    purchase_event = {
        "event_id": event_id,
        "order_id": order_id,
        "reference_event_id": "",
        "customer_id": customer_id,
        "product_id": product_id,
        "event_type": "purchase",
        "payment_method": payment_method,
        "quantity": quantity,
        "amount_inr": amount_inr,
        "event_time": event_time.isoformat(),
        "ingestion_time": ingestion_time.isoformat()
    }

    # Store purchase information for future events.
    purchase_information = {
        "event_id": event_id,
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "payment_method": payment_method,
        "quantity": quantity,
        "original_amount": amount_inr,
        "remaining_refund_amount": amount_inr,
        "is_cancelled": False
    }

    # Add to both eligible-purchase lists.
    refundable_purchases.append(purchase_information)

    cancellable_purchases.append(purchase_information)

    return purchase_event


# ==================================================
# 4. GENERATE COMPLETE DATASET
# ==================================================

def generate_dataset(num_records, output_file, seed=42):

    if num_records < 0:

        raise ValueError(
            "Number of records cannot be negative"
        )

    # Local random generator for reproducibility.
    rng = random.Random(seed)

    generated_events = []

    # These lists allow efficient selection of purchases.
    refundable_purchases = []

    cancellable_purchases = []

    for number in range(1, num_records + 1):

        event = generate_event(
            number,
            rng,
            refundable_purchases,
            cancellable_purchases
        )

        generated_events.append(event)

    # Convert generated records into a DataFrame.
    df = pd.DataFrame(
        generated_events,
        columns=OUTPUT_COLUMNS
    )

    # Create output directory if it does not exist.
    output_path = Path(output_file)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # Save the dataset.
    df.to_csv(output_path, index=False)

    print(f"{num_records} events generated successfully!")

    print(f"Saved file: {output_path}")

    return df


# ==================================================
# 5. RUN GENERATOR DIRECTLY
# ==================================================

if __name__ == "__main__":

    generate_dataset(
        5,
        "data/input/sample_events.csv"
    )