# Data Dictionary

## Project: YottaFlex AI – Reliable Event Processing Pipeline

### 1. Source Dataset

**File:** `data/input/sample_events.csv`

**Description:**

Contains synthetic e-commerce events generated for testing and processing.

The dataset includes purchase, refund, and cancellation events.

### 2. Source Schema

| Column | Data Type | Description |
|---|---|---|
| event_id | String | Unique identifier for each event |
| order_id | String | Identifier of the associated order |
| reference_event_id | String | References the original purchase event for refunds and cancellations |
| customer_id | String | Identifier of the customer |
| product_id | String | Identifier of the product |
| event_type | String | Type of event: purchase, refund, or cancellation |
| payment_method | String | Payment method used |
| quantity | Integer | Number of items associated with the event |
| amount_inr | Numeric | Event amount in Indian Rupees |
| event_time | DateTime | Time when the event occurred |
| ingestion_time | DateTime | Time when the event entered the pipeline |

### 3. Data Quality and Business Rules

1. `event_id` must be unique and not null.
2. Required fields must contain valid values.
3. `event_type` must be purchase, refund, or cancellation.
4. `payment_method` must be a supported payment method.
5. `quantity` must be a positive integer.
6. `event_time` and `ingestion_time` must contain valid timestamps.
7. Purchase events must have a positive amount and must not reference another event.
8. Cancellation events must have an amount of zero and reference the original purchase.
9. Refund events must have a positive amount and reference the original purchase.
10. Refund and cancellation references must match the original purchase's order, customer, product, and payment method.
11. Cumulative refunds must not exceed the original purchase amount.
12. Events arriving more than 10 minutes after their event time are identified as late events.
13. Events received in an order different from their event timestamps are identified as out-of-order events.

### 4. Primary Key

**Primary Key:** `event_id`

Each event must have a unique event identifier. Duplicate event IDs are detected during processing.

### 5. Currency

All transaction amounts use Indian Rupees (INR).

### 6. Timestamp Definitions

**event_time:**

The actual time at which an e-commerce event occurred.

**ingestion_time:**

The time at which the event was received by the processing system.

**Arrival Delay:**

The difference between ingestion time and event time, measured in minutes.

Events with an arrival delay greater than 10 minutes are classified as late events.

**Timestamp ordering:** `ingestion_time` must be greater than or equal to `event_time`. Records where `ingestion_time` is earlier than `event_time` are rejected.

### 7. Dataset Information

| Dataset | Records | Purpose |
|---|---:|---|
| sample_events.csv | 5 | Basic pipeline testing |
| reliability_test.csv | 5 | Reliability and recovery testing |
| edge_case_events.csv | 15 | Edge-case validation |
| large_events.csv | 100,000 | Large-scale processing verification |
| test_events_100.csv | 100 | Additional testing |
| test_events_1000.csv | 1,000 | Additional testing |

**Generation Method:** Python CSV generator

**Random Seed:** 42

### 8. Current Limitations

- The datasets are synthetic and do not represent real customer transactions.
- The pipeline is designed as a local batch-processing system.
- CSV and JSON are used for input, output, and checkpoint storage.
- The implementation does not currently use a distributed streaming platform or cloud-based processing infrastructure.
- Processing performance depends on the local machine and dataset size.