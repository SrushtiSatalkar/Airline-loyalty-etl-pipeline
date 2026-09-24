# Airline Loyalty ETL Pipeline

Data engineering solution for the Incubyte Technical Assessment.

## Architecture Overiew

![Skyoints Architecture](docs/SkyPoints%20Member%20and%20Redemption%20Architecture%20diagram.jpeg)

This pipeline uses a RAW -> STAGING -> CURATED architecture.
Member profiles undergo schema validation, parsing, DQ, latest-record selection and country routing.
Redemption JSON is flattened into an append-oriented fact table.

## Goal

Build a reliable data pipeline that:

- Processes SkyPoints member profile feeds
- Splits members into country-specific target tables
- Handles member country changes using latest-record semantics
- Processes semi-structured redemption transaction data
- Applies data quality validations
- Supports repeatable and scalable processing

## Planned Stack

- Python
- SQL
- Snowflake
- pytest
- GitHub Actions

## Status

Project initialization.