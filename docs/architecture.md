# Architecture: SkyPoints Global Loyalty Pipeline

## 1. Overview

SkyPoints receives daily member-profile flat files and redemption JSON.

The pipeline uses:

```text
Azure Blob → Python Orchestrator → Snowflake RAW → STAGING + DQ → CURATED
```

Member profiles are validated, versioned, merged into a canonical current-state table, and routed into country-specific tables.

Redemption JSON is retained in RAW, flattened into transaction-level records, validated, and loaded into the curated transaction table.

---

## 2. Architecture

```text
                 Azure Blob Storage
                        │
                        ▼
                Python Orchestrator
                        │
                        ▼
              ┌─────────────────────┐
              │     Snowflake RAW   │
              │                     │
              │ RAW_MEMBER_FEED     │
              │ RAW_REDEMPTION_FEED │
              └──────────┬──────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       Member STAGING        Redemption STAGING
          + DQ                    + DQ
              │                     │
              ▼                     ▼
       MEMBER_CURRENT        REDEMPTION
              │                 CURATED
              ▼
       Country Tables
```

---

## 3. Key Design Decisions

### Member identity

`Member_ID` is the business key.

Although the assessment source layout identifies `Member_Name` as a key, `Member_ID` is used because names are not stable identifiers, the JSON feed uses `member_id`, and country movement requires a stable key.

### Latest record

`source_feed_timestamp` is treated as the logical profile version.

`Last_Flight_Date` represents business activity and is **not** used to determine which profile is newer.

A newer valid record can replace the current record, while an older late-arriving record cannot.

### DQ before latest-record selection

Records are validated before choosing the latest record.

This prevents a malformed newer record from replacing an older valid profile.

```text
RAW → Parse → DQ → Latest valid record → Current state
```

### Country movement

`MEMBER_CURRENT` is the canonical source of truth.

When a member moves from one country to another, the member is removed from the old country table and inserted into the new one. This prevents two simultaneous current-country records.

### Deterministic derived values

`Age` is calculated against the feed as-of date using a birthday-aware calculation.

`Stale_Member` is `TRUE` when the last flight was more than 90 days before the feed as-of date. A missing flight date produces `NULL`.

---

## 4. RAW Layer

RAW preserves source evidence and lineage before business transformations.

### Member feed

`RAW.RAW_MEMBER_FEED`

The complete H/D source line is preserved in `raw_record` together with:

* batch ID
* source file
* source row number
* source feed timestamp
* ingestion timestamp

The complete line is initially treated as a single field so positional parsing is not applied before source-contract validation.

### Redemption feed

`RAW.RAW_REDEMPTION_FEED`

The original JSON is stored as Snowflake `VARIANT` with equivalent batch and source lineage.

---

## 5. Source Contract and DQ

The member source contract is versioned in:

```text
config/member_feed_schema.yml
```

The pipeline validates the expected header and field count before parsing.

Schema changes are treated as contract changes rather than silently absorbed.

This is particularly important because the assessment specification mentions an optional `Post_Code`, while the supplied H/D feed does not contain that field. The implementation follows the actual supplied payload and preserves it unchanged in RAW.

Invalid records are quarantined with the original source record and a DQ reason.

---

## 6. Curated Layer

### `CURATED.MEMBER_CURRENT`

Canonical current member state.

**Grain:** one row per `Member_ID`.

The table is maintained using a version-aware `MERGE` so older records cannot overwrite newer state.

### Country tables

Current members are synchronized into:

```text
MEMBER_IND
MEMBER_USA
MEMBER_PHIL
MEMBER_CAN
MEMBER_AU
MEMBER_CHN
MEMBER_BRA
MEMBER_GBR
MEMBER_JPN
MEMBER_ARE
MEMBER_FRA
MEMBER_DEU
MEMBER_SGP
```

These represent current state rather than historical snapshots.

### Redemption

JSON `redemptions[]` is flattened using Snowflake `LATERAL FLATTEN`.

**Grain:** one row per `txn_id`.

Invalid transactions are quarantined. The controlled sample includes `RX10093`, which is quarantined because its `member_id` is not present in the current member state.

---

## 7. Incremental Processing and Idempotency

Every execution uses a `batch_id`.

The pipeline is designed for deterministic reruns using:

* batch identity;
* Snowflake COPY file tracking;
* business-key-based `MERGE`;
* source-version comparison;
* transaction-key uniqueness;
* deterministic country synchronization.

Rerunning an already processed batch does not create duplicate current members or transactions.

Pipeline execution is tracked in:

```text
CONTROL.BATCH_RUN
```

with `RUNNING`, `SUCCESS`, or `FAILED` status.

---

## 8. Responsibility Split

| Python                     | Snowflake               |
| -------------------------- | ----------------------- |
| Configuration              | Parsing                 |
| Batch validation           | DQ                      |
| Source-contract validation | JSON `FLATTEN`          |
| File/load orchestration    | Latest-record selection |
| Error handling             | `MERGE` / current state |
| Pipeline status            | Country routing         |
|                            | Transaction processing  |

Python handles orchestration; Snowflake performs the large-scale set-based transformations.

This avoids row-by-row Python processing and keeps the design suitable for the assessment's billion-record/day requirement.

---

## 9. Testing

Automated Snowflake SQL tests are executed through:

```text
python/run_tests.py
```

Current checks cover:

* `Member_ID` uniqueness;
* country-target reconciliation;
* latest-record-wins;
* country movement;
* transaction uniqueness;
* expected redemption quarantine;
* transaction reconciliation;
* redemption idempotency.

The sample pipeline has been rerun successfully to verify idempotent behavior.

---

## 10. Main Assumptions

* `Member_ID` is the stable business key.
* `source_feed_timestamp` represents profile version for this assessment.
* The feed date is the deterministic as-of date for derived values.
* DQ occurs before latest-record selection.
* Country tables represent current state.
* Invalid records are quarantined rather than silently discarded.
* The actual supplied H/D payload takes precedence over the inconsistent `Post_Code` position in the written field specification.

Production implementation would confirm these assumptions with the source owner and add operational components such as scheduling, alerting, secrets management, and CI/CD as required.
