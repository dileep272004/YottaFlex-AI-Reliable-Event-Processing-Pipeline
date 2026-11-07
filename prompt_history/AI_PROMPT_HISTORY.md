# YottaFlex AI Project – ChatGPT Prompt History

## 1. AI Tool Used

- **AI tool:** ChatGPT
- **Other AI tools:** None
- **Purpose:** Understanding project requirements, discussing implementation approaches, debugging, testing, and documentation.

## 2. Development Prompt Summaries

The following are summaries of the main topics discussed with ChatGPT during project development. They are not verbatim copies of the original prompts.

### 1. Understanding Project Requirements
- Asked ChatGPT to explain the project requirements and expected deliverables.
- Discussed the objectives and expected functionality of the event-processing pipeline.

### 2. Project Architecture
- Discussed how to organize the Python project into separate modules.
- Explored batch processing, CSV input/output, and JSON checkpoint storage.

### 3. Data Processing and Validation
- Discussed event validation, duplicate detection, purchase and refund references, and cancellation rules.
- Worked through late-arrival and out-of-order event detection.

### 4. Reliability and Recovery
- Discussed checkpoint saving and restoring processing state.
- Worked through batch commits and simulated crash recovery.

### 5. Testing and Debugging
- Requested explanations and debugging assistance for implementation issues.
- Discussed automated tests for validation, references, refunds, duplicates, and recovery.

### 6. Dataset Verification
- Discussed how to verify the 100,000-record processing run.
- Checked unique event IDs, duplicate records, rejected records, committed batches, and financial reconciliation.

### 7. Documentation and Submission
- Requested assistance preparing the README, AI disclosure, and GitHub submission documentation.

## 3. Candidate Verification

The candidate executed the project locally and verified the implementation through automated tests and dataset checks.

- 100,000 records processed.
- 100,000 unique event IDs.
- Zero duplicate records in the processed output.
- Zero rejected records in the large dataset run.
- 20 committed batches.
- 15 automated tests passed.
- Source and processed revenue totals matched.

## 4. AI Usage Disclosure

ChatGPT was used as an assistance tool throughout the development process. The candidate executed the code, reviewed the outputs, conducted tests, verified results, and made final project decisions.