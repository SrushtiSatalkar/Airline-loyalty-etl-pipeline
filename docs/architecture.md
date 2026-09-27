# Architecture: SkyPoints Global Loyalty Pipeline

## 1. Objective
SkyPoints receives daily member-profile flat files and redemption JSON. This design preserves the source feeds, validates and transforms them in Snowflake, and maintains one current member record plus the country-specific tables required by the assessment.

The SkyPoints Member and Redemption Architecture diagram provides the high-level overview of the two flows: member profiles pass through DQ and latest-record selection before country routing; redemptions are flattened and validated independently, then enriched from the current member state.

---

## 2. Requirements and Decisions
* **Member Identity:** Use `Member_ID` as the business key. The source layout marks `Member_Name` as a key, but names can change or be shared, the JSON feed uses `member_id`, and country movement needs a stable identifier. This is an explicit assessment inconsistency, not a silent correction to the source contract.
* **Profile Version:** Use `source_feed_timestamp` as the logical profile version for this assessment. `Last_Flight_Date` is business activity, not a version. This assumption must be confirmed with the source owner in a production implementation; delivery/ingestion time must not be substituted for business versioning unless the source contract guarantees that behavior.
* **Current State:** A newer valid profile may replace an older one. Latest-record selection is performed after DQ validation, and current-state `MERGE` compares the incoming source version with the stored version. Neither an invalid newer record nor a late-arriving older record may overwrite current state.
* **Historical Evidence:** `RAW` retains the original source records, while `STAGING` retains parsed and validated representations with source lineage. `CURATED.MEMBER_CURRENT` and the country targets represent current state. A separate country-change history is not required.

---

## 3. Data Flow and Layer Responsibilities

### RAW — Source Evidence
Python discovers and identifies batches, validates file-level metadata, and orchestrates bulk loading from Azure Blob Storage into Snowflake. RAW is append-oriented: it does not deduplicate, derive member attributes, or flatten JSON.

* `RAW.RAW_MEMBER_FEED` preserves each supplied H/D member-feed line exactly in `raw_record`, together with batch, file, source-row, source-feed-timestamp, and ingestion lineage.

* The member RAW file format intentionally treats the complete source line as a single field during ingestion. This prevents the pipe-delimited business fields from being interpreted before the source contract has been validated.

* `source_field_count` is calculated from `raw_record` during staging and used to detect changes in the source contract. The current supplied H/D feed contains 10 business fields after the record-type indicator. A changed field count must be detected before positional parsing is applied downstream.

* `RAW.RAW_REDEMPTION` retains the original JSON in `raw_payload` with equivalent batch and source-record lineage.

### STAGING — Parsing and Validation
Before positional parsing, the member feed is checked against the expected source contract. A field-count or header mismatch is treated as schema drift rather than being silently interpreted using the existing mapping. The original record remains preserved in RAW so that, after the source contract is confirmed and the parser is updated, the affected batch can be replayed.

`STAGING.MEMBER_STAGING` has one parsed source member record per row. It contains typed source values, `Age`, `Stale_Member`, DQ status and reason, and the lineage needed to investigate or order records. DQ runs before latest-record selection, so a malformed record cannot win merely because it has a later timestamp.

Redemption processing parses the retained JSON and uses Snowflake `FLATTEN` on `redemptions[]`. Transaction validation follows flattening. Invalid member or transaction records go to `DQ_QUARANTINE`, which records what failed, why, its batch and source location, and the source data needed for investigation.

### CURATED — Queryable Current State and Transactions

| Logical Grain and Role | Target Table | Description |
| :--- | :--- | :--- |
| **Canonical Current State** | `CURATED.MEMBER_CURRENT` | One current row per `Member_ID`; canonical current member state and source version. |
| **Country Targets** | `CURATED.MEMBER_IND`, `CURATED.MEMBER_USA`, `CURATED.MEMBER_PHIL`, `CURATED.MEMBER_CAN`, `CURATED.MEMBER_AU` | Physical current-state targets synchronized from `MEMBER_CURRENT`. The table identifies the country, so these targets do not repeat `Country`. |
| **Transaction Fact** | `CURATED.REDEMPTION_FACT` | One row per `txn_id`, containing transaction data, `member_id`, and ingestion/version lineage—not copied member name, tier, or country. |

*Note: The supplied `IND.csv`, `USA.csv`, and `AUS.xlsx` files are separate sample/reference inputs, not additional canonical member feeds. Their smaller, different schemas must be checked and documented during ingestion, without silently merging their fields into the member model. The `AUS.xlsx` example includes an invalid enrollment date.*

---

## 4. Processing Rules

* **Member DQ and Latest-Record Selection:** Validate required values, supported country and active-flag values, date parsing, and applicable numeric values. Invalid records are quarantined, not silently dropped. For each `Member_ID`, select the latest valid profile by `source_feed_timestamp`.
* **Conflict Resolution:** Identical repeats at the same version can be collapsed without changing current state. If same-version records disagree, quarantine the conflict and do not invent a winner from file or row order. Update `MEMBER_CURRENT` only when the incoming version is greater than the stored version; an equal version is not a new update.
* **Deterministic Derived Values:** Derive `Age` with a birthday-aware calculation against the source feed’s as-of date, not the execution date. Set `Stale_Member` when days from `Last_Flight_Date` to that as-of date are greater than 90. If `Last_Flight_Date` is null, `Stale_Member` is null (unknown), rather than false or a fabricated date. A missing DOB likewise yields a null age when the source contract permits it.
* **Country Movement:** `MEMBER_CURRENT` is the source of truth for country routing. For example, if a newer valid record moves member `223457` from `IND` to `USA`, synchronization removes that member from `MEMBER_IND` and places them in `MEMBER_USA`. It must not leave two current country rows. An older `IND` record arriving afterward cannot reverse the move.
* **Redemptions and Member Relationship:** Validate flattened transactions, including `txn_id` presence and uniqueness, before loading `REDEMPTION_FACT`. Conflicting transactions with the same `txn_id` require DQ investigation. Enrich transactions through a `LEFT JOIN` from `REDEMPTION_FACT.member_id` to `MEMBER_CURRENT.Member_ID`. A valid transaction whose profile has not arrived remains in the fact; it is not automatically quarantined or discarded.

---

## 5. Incremental Processing, Reruns, and Recovery
Each load retains batch identity and source location. Transform only relevant new or retried batches rather than rescanning all historical staging. DQ checks, version-based selection, transaction-key checks, and current-state `MERGE` operations make repeated processing safe against duplicate current rows and facts.

The orchestrator records whether raw load, transformations, tests, and country synchronization succeeded. On failure, retain `RAW` evidence and quarantine findings, stop publication of incomplete results, correct the cause, and rerun the identified batch. Country targets must be reconciled with `MEMBER_CURRENT` after a failed or interrupted routing step so a move cannot remain half-applied.

Processing is idempotent: rerunning the same batch produces the same current state and transaction results rather than creating duplicate current records or transactions.
---

## 6. Scale and Responsibility Split
The billion-record-per-day requirement shapes the processing pattern; the supplied samples do not prove that throughput. Use Azure Blob Storage for object-storage landing, Snowflake bulk ingestion, set-based SQL, incremental batch filters, and pruning-friendly access to batch/version data.

* **Python:** File discovery and validation, batch identification, raw-load orchestration, error handling, dbt invocation, and pipeline status.
* **Snowflake/dbt:** Parsing, typing, DQ, latest-record selection, JSON `FLATTEN`, incremental transformations, merges, country routing, tests, and lineage.

---

## 7. Testing Strategy
* **pytest:** Validates filename parsing, batch identification, input validation, orchestration, and error handling.
* **dbt / Snowflake Tests:** Asserts required fields, valid dates and accepted values, current-member and transaction uniqueness, DQ outcomes, and country-target consistency.

---

## 8. Assumptions and Open Questions
1. Confirm that `source_feed_timestamp` is a logical profile version and define the member feed’s authoritative as-of date.
2. The assessment field specification lists an optional `Post_Code` field, but the supplied H/D member feed does not contain that position. The implementation preserves the supplied H/D source shape in RAW and does not insert a positional `Post_Code` field that would shift `DOB` and `Is_Active`. If the source contract later adds `Post_Code`, the schema change must be detected and explicitly approved before updating the parser.
3. Confirm source date formats, particularly ambiguous DOB values, and define the authoritative feed as-of date used for deterministic derived values.
4. Confirm supported country and active-flag values and whether additional country targets are expected.
5. Confirm whether reporting needs member attributes at transaction time (current design uses `LEFT JOIN` for current-state enrichment only).

---

## 9. Implementation Sequence

Architecture → Snowflake schemas → Raw ingestion → Staging transformations and DQ → Latest/current-state merge → Country routing → Redemption JSON → dbt tests → Python orchestration → pytest → CI → Documentation → Final adversarial review.