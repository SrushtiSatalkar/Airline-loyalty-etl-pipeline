# Assumptions

This document records assumptions made where the assessment materials are ambiguous,
inconsistent, or do not explicitly define the required behavior.

## 1. Member Business Key

`Member_ID` is treated as the business key for a member.

The source detail layout marks `Member Name` as the key column, but the assessment
also provides `Member_ID` and the JSON redemption feed identifies members using
`member_id`. The "latest record wins" requirement when a member moves countries also
requires a stable member identifier.

Therefore, `Member_ID` is used to identify a member across profile records,
country-specific target tables, and redemption transactions.

`Member_Name` remains a member attribute and is not used as the business key.

---

## 2. Latest Record Wins

The assessment requires the latest record to win when a member has moved countries,
but the member profile layout does not provide an explicit record update timestamp
or version number.

The assessment does provide a file date/time specification. We therefore assume
that the source feed/batch timestamp represents the logical version of the feed and
can be used to order records for latest-record selection.

The pipeline will retain both the source feed timestamp and the ingestion timestamp
where applicable.

If the source timestamp represents only file delivery time rather than the logical
version of the member data, the source contract would need to be clarified before
production implementation.

`Last_Flight_Date` is not used to determine which member profile record is latest.
It represents business activity, not the version of the profile record.

---

## 3. Country Values

The sample data contains country values such as:

- `USA`
- `IND`
- `PHIL`
- `CAN`
- `AU`

The assessment does not explicitly define these as ISO country codes or provide a complete reference list of supported country values.

Therefore, the pipeline will preserve the country values supplied by the source
and validate them against the configured set of supported source values rather
than silently converting them to another coding standard.

---

## 4. Date Formats

The member profile specification defines date fields with length 8 and data type
`DATE`. The sample profile data represents dates as `YYYYMMDD`, while the DOB
example `03051985` appears to represent `MMDDYYYY`.

The supplied country files use a different display format such as `12/1/1998`
and `6/15/2022`.

Therefore, date parsing will follow the format of each supplied source rather than
assuming that all input files use one universal date format.

Malformed or ambiguous date values will be identified by validation rather than
silently corrected.



## 5. Active Member Flag

The profile sample uses `A` for the Active Member field, but the assessment does
not provide a complete list of permitted values or explicitly define the meaning
of every possible flag.

`A` is therefore treated as the active value based on the supplied sample.
Unexpected values will be surfaced by data-quality validation rather than silently
mapped to another value.

---

## 6. Current Country vs Historical Country

The country-specific target tables are treated as current-state member tables.

When the latest valid profile record for a member has a different country from an
earlier record, the member belongs in the target table corresponding to the latest
record's country.

For example:

    Earlier record:  Member_ID 223457 -> IND
    Latest record:   Member_ID 223457 -> USA

The current-state result is:

    Table_India -> member removed from current state
    Table_USA   -> member present

Historical/source records remain available in the raw/staging layers and are not
silently deleted as part of the country movement.

---

## 7. Stale Member Calculation

`Stale_Member` is a derived field.

It is calculated as whether the number of days between `Last_Flight_Date` and the
source feed's as-of date is greater than 90 days.

Conceptually:

    DATEDIFF(day, Last_Flight_Date, feed_as_of_date) > 90

The value is recalculated when the member record is processed. It is not treated
as a permanent member attribute.

Using the feed's as-of date rather than the execution date keeps processing
deterministic when a historical batch is reprocessed.

If `Last_Flight_Date` is null, the member cannot be classified using the stated
90-day rule and this case will be handled explicitly by the transformation and
data-quality logic.

---

## 8. Supplied Country Files

The supplied `IND.csv`, `USA.csv`, and `AUS.xlsx` files contain a different and
smaller set of columns from the main member profile specification.

They are treated as additional supplied sample/reference files rather than being
assumed to be the exact schema of the daily member profile feed.

Their differences will be considered when designing ingestion and validation logic,
rather than silently assuming that they represent the complete production source
contract.