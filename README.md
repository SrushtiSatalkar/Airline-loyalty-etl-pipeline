# SkyPoints Airline Loyalty ETL Pipeline

Data engineering solution for the Incubyte Data Engineer Technical Assessment.

The pipeline ingests daily airline loyalty member profiles and semi-structured redemption data into Snowflake, applies source-contract validation and data quality rules, maintains the latest member state, routes members into country-specific tables, and produces a curated redemption dataset.

## Architecture

![SkyPoints Architecture](docs/SkyPoints%20Member%20and%20Redemption%20Architecture%20diagram.jpeg)

```text
Azure Blob Storage
       │
       ├── Member profile feed
       │
       └── Redemption JSON
       │
       ▼
     RAW
       │
       ▼
   STAGING
   ├── Parsing
   ├── Data quality validation
   ├── Quarantine
   ├── Age / stale-member calculation
   └── JSON flattening
       │
       ▼
    CURATED
   ├── MEMBER_CURRENT
   ├── Country-specific member tables
   └── REDEMPTION
```

The implementation uses:

* **Python** for orchestration, configuration loading, source-contract validation, and Snowflake execution.
* **Snowflake SQL** for set-based transformation, DQ, deduplication, current-state maintenance, JSON flattening, and MERGE operations.
* **Azure Blob Storage** as the landing area for source feeds.
* **YAML** for source-contract and pipeline configuration.
* **Git** for incremental version-controlled development.

## Repository Structure

```text
.
├── config/
│   ├── member_feed_schema.yml
│   └── pipeline.yml
├── data/
│   └── redemption/
│       └── member_redemptions.json
├── docs/
│   ├── architecture.md
│   ├── assumptions.md
│   └── demo.md
├── python/
│   ├── run_pipeline.py
│   └── run_tests.py
├── sql/
│   ├── 00_control.sql
│   ├── 01_raw_member.sql
│   ├── 02_staging_member.sql
│   ├── 03_validate_member_dq.sql
│   ├── 04_load_member_staging.sql
│   ├── 05_member_current.sql
│   ├── 06_country_targets.sql
│   ├── 07_raw_redemption.sql
│   ├── 08_staging_redemption.sql
│   └── 09_redemption_dq_and_curated.sql
├── tests/
│   └── sql/
│       └── test_member_pipeline.sql
├── requirements.txt
└── README.md
```

## Key Design Decisions

### Member identity

`Member_ID` is treated as the business key.

Although the supplied field specification identifies `Member_Name` as a key, names are not stable identifiers and the redemption feed references members using `member_id`. Using `Member_ID` also provides a consistent key for latest-record processing and country movement.

### Latest-record semantics

The pipeline uses the source feed timestamp as the record version.

`Last_Flight_Date` is **not** used to determine which profile is newer because it represents a business attribute rather than a source-system version.

When a newer record for an existing member arrives, it can replace the current state. An older late-arriving record cannot overwrite a newer current record.

### Data quality before latest selection

Records are validated before they are allowed to affect current state.

Invalid records can be quarantined for investigation rather than silently entering the curated layer.

Examples include:

* Missing mandatory Member ID
* Invalid dates
* Missing or unsupported country
* Invalid active-member flag
* Field-count/schema violations
* Invalid field lengths

### Country movement

Country-specific tables represent the **current state** of each member.

If a member moves from one country to another, the current-state table is updated and the member is removed from the previous country table and inserted into the new country table.

RAW and STAGING retain the source history needed to reproduce and audit processing.

### Stale-member calculation

`stale_member` is calculated using the batch feed's as-of date rather than the execution timestamp.

A member is stale when:

```text
days between feed_as_of_date and Last_Flight_Date > 90
```

A missing `Last_Flight_Date` results in a NULL stale-member value.

Using the feed date makes the calculation deterministic and reproducible when a historical batch is rerun.

### Redemption processing

The redemption JSON is flattened using Snowflake's `LATERAL FLATTEN`.

Each redemption transaction is stored as an individual curated record.

A redemption referencing a member that does not exist in the current member state is quarantined rather than silently dropped.

## Idempotency

The pipeline is designed so that rerunning the same batch does not duplicate the curated state.

Examples:

* Snowflake COPY prevents already-loaded files from being loaded again.
* Member current state uses deterministic `MERGE` logic.
* Country tables are rebuilt only for members affected by the current batch.
* Redemption records use transaction-level keys.
* Automated tests verify repeated processing does not increase the curated redemption count.

## Batch Tracking

The pipeline records execution status in:

```text
SKYPOINTS.CONTROL.BATCH_RUN
```

A pipeline execution is tracked as:

```text
RUNNING → SUCCESS
```

or:

```text
RUNNING → FAILED
```

Failures store an error message to support investigation.

## Source Contract

The member feed contract is versioned in:

```text
config/member_feed_schema.yml
```

The pipeline validates:

* Header structure
* Expected field count
* Record type
* Required fields
* Date formats
* Supported countries

This is particularly important because the assessment's field specification and supplied sample feed differ regarding the optional Post Code field. The implementation follows the actual supplied H/D payload shape and treats unexpected structural changes as a contract issue rather than silently changing the parser.

## Configuration

Runtime ingestion configuration is stored in:

```text
config/pipeline.yml
```

For example:

```yaml
sources:
  member:
    stage_path: member
    file_pattern: ".*member_profile_feed\\.csv"

  redemption:
    stage_path: redemption
    file_pattern: ".*redemptions\\.json"
```

The Python orchestrator reads these values instead of hardcoding source paths and file patterns.

## Running the Pipeline

### Prerequisites

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Set the required Snowflake environment variables:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
```

Optional variables:

```text
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_ROLE
```

The default values are:

```text
Warehouse: COMPUTE_WH
Database: SKYPOINTS
Role: ACCOUNTADMIN
```

The Snowflake account must also have access to the configured Azure external stage.

### Execute a batch

```bash
python python/run_pipeline.py --batch-id 20260926
```

The batch ID corresponds to the source feed date.

A successful run reports:

```text
Connection: PASS
Batch tracking: RUNNING
...
Batch tracking: SUCCESS
```

Already-loaded source files are skipped by Snowflake, allowing the same batch to be rerun safely.

## Running Tests

Execute the automated SQL assertions with:

```bash
python python/run_tests.py
```

Expected result:

```text
Running SkyPoints automated tests...
All automated tests passed.
```

The current test suite validates:

1. Member ID uniqueness
2. Country-routing reconciliation
3. Latest-record-wins behavior
4. Country movement
5. Redemption transaction uniqueness
6. Redemption DQ/quarantine behavior
7. Redemption reconciliation
8. Redemption idempotency

## Scale and Production Considerations

The assessment specifies processing at billion-record-per-day scale.

The implementation therefore avoids row-by-row Python processing for transformations. Python orchestrates the pipeline while Snowflake performs set-based processing.

The design supports scale through:

* Incremental batch processing
* Set-based SQL transformations
* Early data-quality filtering
* Deterministic MERGE operations
* Business-key based current-state maintenance
* Source-file based ingestion
* Separation of RAW, STAGING, and CURATED layers

The supplied assessment sample is intentionally small, so the sample execution does **not** claim to benchmark billion-record performance. At production scale, warehouse sizing, query plans, micro-partition pruning, clustering strategy, ingestion parallelism, and workload isolation would need to be evaluated using representative data volumes.

## Live Demo Scenarios

The implementation supports demonstrating the following cases:

| Scenario                  | Expected behavior                                        |
| ------------------------- | -------------------------------------------------------- |
| Valid member              | Loaded through RAW → STAGING → CURATED                   |
| Missing Member ID         | Quarantined                                              |
| Invalid date              | Quarantined                                              |
| Unsupported country       | Quarantined                                              |
| Duplicate/current member  | Latest valid record wins                                 |
| Member country change     | Removed from old country table and routed to new country |
| Missing Last Flight Date  | `stale_member` remains NULL                              |
| Redemption JSON           | Flattened into transaction-level rows                    |
| Unknown redemption member | Quarantined                                              |
| Re-running the same batch | No duplicate curated state                               |
| Pipeline failure          | `BATCH_RUN` records FAILED status and error              |

## AI-Assisted Development

AI tools were used during development for implementation assistance, code review, debugging, test generation, and documentation.

The implementation was validated independently through:

* Source-contract checks
* Snowflake execution
* Intermediate row-count checks
* Data-quality scenarios
* Latest-record and country-movement tests
* Idempotency tests
* Automated SQL assertions
* Successful and intentionally failed pipeline runs

Design decisions and source-contract discrepancies were reviewed against the assessment requirements rather than accepted blindly from generated code.

## Current Implementation Status

The core assessment workflow is implemented and executable:

* RAW member ingestion
* RAW redemption ingestion
* Source-contract validation
* Member staging and DQ
* Quarantine
* Age and stale-member calculation
* Latest member state
* Country-specific routing
* Redemption JSON flattening
* Redemption DQ
* Curated redemption data
* Batch execution tracking
* Idempotent reruns
* Python orchestration
* Automated validation tests
* Configuration-driven ingestion
