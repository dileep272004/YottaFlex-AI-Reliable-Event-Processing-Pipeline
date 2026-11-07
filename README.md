# YottaFlex AI – Reliable Event Processing Pipeline

## 1. Project Overview

This project implements a reliable, batch-based event processing pipeline for processing order-related events such as purchases, refunds, and cancellations.

The pipeline is designed to handle large volumes of event data while maintaining data quality, preventing duplicate processing, validating business rules, detecting late and out-of-order events, and supporting recovery through checkpoints.

The implementation uses Python and Pandas with CSV-based input and output.

## 2. Key Features

- **Batch Processing:** Reads large CSV datasets in configurable batches.
- **Data Validation:** Checks required fields, data types, timestamps, event types, and amount rules.
- **Deduplication:** Detects duplicate event IDs within a batch and across previously processed batches.
- **Reference Validation:** Validates refund and cancellation references against original purchases.
- **Refund Validation:** Prevents refunds from exceeding the original purchase amount.
- **Late Event Detection:** Identifies events arriving more than 10 minutes after their event time.
- **Out-of-Order Detection:** Identifies events that arrive in an order different from their event timestamps.
- **Checkpointing:** Saves processing progress and state for recovery.
- **Crash Recovery:** Resumes processing from the last committed batch.
- **Atomic Batch Commits:** Uses temporary batch directories and atomic renaming to avoid treating incomplete batches as committed.
- **Output Reconciliation:** Produces processed, rejected, and duplicate event outputs.

## 3. Technology Stack

- Python 3.13
- Pandas 3.1.1
- NumPy 2.5.4
- Pytest 9.2.0
- CSV for input and output data
- JSON for checkpoint storage

## 4. Project Structure

```text
YottaFlex_Project/
│
├── data/
│   ├── input/
│   │   ├── sample_events.csv
│   │   ├── reliability_test.csv
│   │   ├── edge_case_events.csv
│   │   ├── expected_edge_case_results.csv
│   │   ├── large_events.csv
│   │   ├── test_events_100.csv
│   │   └── test_events_1000.csv
│   │
│   └── output/                 # Generated locally; excluded from Git
│
├── src/
│   ├── generate_data.py
│   ├── validate_data.py
│   ├── deduplicate.py
│   ├── late_arrival.py
│   ├── pipeline.py
│   ├── checkpoint.py
│   └── run_pipeline.py
│
├── tests/
│   └── test_pipeline.py
│
├── docs/
│   └── data_dictionary.md
│
├── prompt_history/
│   └── AI_PROMPT_HISTORY.md
│
├── results/
│   ├── large_dataset_summary/
│   │   ├── 100k_run_summary.txt
│   │   └── processed_events_sample.csv
│   │
│   ├── edge_case_results/
│   │   ├── processed_events.csv
│   │   ├── rejected_events.csv
│   │   └── duplicate_events.csv
│   │
│   └── reliability_results/
│       ├── processed_events.csv
│       ├── rejected_events.csv
│       ├── duplicate_events.csv
│       └── reliability_test_summary.txt
│
├── run_pipeline.py
├── requirements.txt
├── README.md
└── .gitignore
```

## 5. Setup and Installation

### Clone the Repository

```bash
git clone https://github.com/dileep272004/YottaFlex-AI-Reliable-Event-Processing-Pipeline.git
cd YottaFlex_Project
```

### Create a Virtual Environment

```bash
python -m venv .venv
```

### Activate the Environment on Windows

```bash
.venv\Scripts\activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## 6. Run the Pipeline

Run the pipeline using the default input file:

```bash
python -m src.run_pipeline
```

Run with a custom input file and output directory:

```bash
python -m src.run_pipeline --input data/input/large_events.csv --output-dir data/output/final_verification --batch-size 5000 --restart
```

The batch size is configurable. The example processes 5,000 records per batch.

## 7. Crash Recovery

The pipeline stores committed batches and checkpoint information.

To simulate a crash after the first committed batch:

```bash
python -m src.run_pipeline --input data/input/reliability_test.csv --output-dir data/output/reliability_test --batch-size 2 --restart --simulate-crash-after-commit 1
```

After the simulated crash, run the same command without the restart or crash-simulation options:

```bash
python -m src.run_pipeline --input data/input/reliability_test.csv --output-dir data/output/reliability_test --batch-size 2
```

The pipeline resumes from the committed offset instead of processing previously committed records again.

## 8. Testing

Run the automated test suite:

```bash
python -m pytest -v
```

The automated test suite contains 15 tests, all of which passed during the previous verification run.

The test suite covers:

- Valid purchase, refund, and cancellation events.
- Duplicate event detection.
- Invalid event types and amounts.
- Missing required fields.
- Refund amount validation.
- Reference validation.
- Late event detection.
- Out-of-order event detection.
- Checkpoint saving and recovery.
- Crash recovery without duplicate records.

The previous test run also produced Pandas deprecation warnings. These warnings did not cause test failures.

## 9. Large Dataset Verification

A 100,000-record dataset was processed using a batch size of 5,000.

### Processing Results

| Metric | Result |
|---|---:|
| Input records | 100,000 |
| Processed records | 100,000 |
| Unique event IDs | 100,000 |
| Duplicate IDs in processed output | 0 |
| Rejected records | 0 |
| Duplicate records | 0 |
| Committed batches | 20 |
| Processing time | 31.88 seconds |

### Revenue Reconciliation

| Metric | Amount (INR) |
|---|---:|
| Gross purchase revenue | ₹610,463,076 |
| Total refunds | ₹56,211,153 |
| Cancellation amount | ₹0 |
| Net revenue | ₹554,251,923 |

The source and processed datasets produced matching event counts and financial totals.

Processing time is an observed result from one local run and may vary depending on hardware and environment.

## 10. Output Files

The pipeline produces the following outputs:

- `processed_events.csv` – Successfully processed events.
- `rejected_events.csv` – Events rejected during validation or business-rule checks.
- `duplicate_events.csv` – Duplicate events identified during processing.
- `pipeline_checkpoint.json` – Processing progress and recovery state.
- `batches/` – Committed batch data and manifests.

Generated pipeline outputs are stored locally under `data/output/` and excluded from Git.

## 11. Verification Evidence

Selected verification outputs are available in the `results/` directory.

### Large Dataset Results

The `large_dataset_summary/` directory contains:

- `100k_run_summary.txt` – Summary of the 100,000-event processing run.
- `processed_events_sample.csv` – Sample of processed event records.

### Edge Case Results

The `edge_case_results/` directory contains:

- `processed_events.csv`
- `rejected_events.csv`
- `duplicate_events.csv`

These files demonstrate the pipeline's handling of invalid events, duplicate records, and business-rule validation.

### Reliability Results

The `reliability_results/` directory contains:

- `processed_events.csv`
- `rejected_events.csv`
- `duplicate_events.csv`
- `reliability_test_summary.txt`

The reliability summary records the separate crash-and-resume verification, in which processing was interrupted and subsequently resumed with five unique event IDs and no duplicate records.

Internal checkpoint JSON files and intermediate batch state are retained locally rather than included in the public evidence directory.

## 12. Design Decisions and Limitations

- CSV is used for input and output to keep the implementation simple and reproducible.
- JSON is used for checkpoint storage.
- Pandas is used for data processing and validation.
- A configurable batch size balances processing efficiency and recovery granularity.
- Atomic batch commits help prevent incomplete batches from being treated as successfully committed.
- The pipeline runs locally and does not require a cloud service or message broker.
- The current implementation is designed as a local batch-processing system rather than a distributed streaming platform.
- Performance measurements depend on the local machine and dataset.

## 13. AI Usage Disclosure

AI assistance was used during development for understanding requirements, debugging, reviewing implementation approaches, improving test coverage, and preparing project documentation.

The implementation, testing, verification, and final project decisions were reviewed by the candidate.

The `prompt_history/` directory contains a summary of the main AI-assisted development activities. 

## 14. Conclusion

This project demonstrates a reliable event-processing pipeline with data validation, deduplication, business-rule enforcement, late and out-of-order event detection, checkpointing, and crash recovery.

The implementation was tested with automated test cases and a 100,000-record dataset, with successful record-count and revenue reconciliation.

The project demonstrates practical approaches to building reliable and recoverable batch-processing systems using Python and Pandas.