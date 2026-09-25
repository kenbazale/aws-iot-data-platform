# AWS IoT Data Platform

PySpark data platform for industrial IoT telemetry. Devices publish temperature, humidity, pressure, battery, and location events. The pipeline validates those events, stores them in a medallion lake (Bronze → Silver → Gold), and produces daily device metrics plus latest device status.

You can run the full batch path locally against `./data`. AWS IoT Core, Kinesis Data Firehose, S3, and Glue jobs in `aws/` are the cloud counterpart of the same layers.

## Architecture

```
Devices / simulator
        │
        ▼
  Raw JSON / JSONL          (local: data/raw  | AWS: Firehose → S3)
        │
        ▼
  Bronze (Parquet)          validated + partitioned by event_date, event_hour
        │                   invalid rows → data/dlq
        ▼
  Silver (Parquet)          valid, deduplicated, normalized
        │
        ▼
  Gold                      daily metrics (device_id, event_date)
                            latest status (one row per device)
```

**Event time** drives partitions (`event_date`, `event_hour`), not job run time.

### Local batch path

1. `jobs/generate_iot_data.py` writes simulated JSONL into the raw directory.
2. `jobs/bronze_ingestion.py` picks up **new** files only (manifest in `data/state/processed_files.json`), validates, writes Bronze Parquet and a JSON dead-letter queue.
3. `jobs/silver_transform.py` keeps valid rows, drops duplicate `event_id`s, normalizes identifiers, overwrites Silver.
4. `jobs/gold_aggregation.py` rebuilds Gold partitions for dates present in Silver, plus a full overwrite of latest device status.

### AWS path

IoT topic `iot/devices/+/telemetry` is intended to land in Firehose stream `iot-platform-raw-stream`, then S3. Glue scripts under `aws/glue/` apply the same Bronze / Silver / Gold logic on the lake bucket.

IAM JSON for device connect/publish, IoT rule, Firehose, and S3 lives in `aws/iot/`. Device certificates are not in git (`aws/iot/certs/` is ignored).

## Telemetry contract

Each event is one JSON object:

| Field | Type | Notes |
| --- | --- | --- |
| `event_id` | string | Unique event key (dedupe in Silver) |
| `device_id` | string | Device identifier |
| `device_type` | string | e.g. `industrial_cooler` |
| `event_timestamp` | timestamp | Event time (UTC in Spark) |
| `temperature` | double | Valid range −100 to 150 |
| `humidity` | double | Valid range 0 to 100 |
| `pressure` | double | Must be > 0 |
| `battery_level` | double | Valid range 0 to 100 |
| `latitude` / `longitude` | double | Simulator uses a Malawi bounding box |
| `firmware_version` | string | |

Invalid records are **marked**, not dropped, in Bronze (`is_valid`, `validation_error`) so they can be written to the DLQ.

### Gold outputs

- **Daily metrics** — grain `(device_id, event_date)`: event count, min/avg/max temperature, avg humidity, min/avg battery.
- **Latest status** — grain `device_id`: most recent telemetry by `event_timestamp`, written under `{gold_path}/device_latest_status`.

Gold daily writes use Spark **dynamic partition overwrite** so only affected `event_date` partitions are replaced.

## Requirements

- Python 3.11+
- [Poetry](https://python-poetry.org/)
- Java available for local PySpark (Spark 3.5)

## Setup

```bash
poetry install
```

Configuration for local runs is `configs/dev.yaml`:

- simulator fleet size
- paths for raw / bronze / silver / gold
- Spark app name and shuffle partitions

Generated lake files under `data/raw`, `data/bronze`, `data/silver`, `data/gold`, and `data/dlq` are gitignored. The processed-file manifest is kept under `data/state/`.

## Run locally

From the repo root, with the Poetry environment:

```bash
# 1. Simulate a fleet (default: 10 devices × 5 batches → 50 events)
poetry run python jobs/generate_iot_data.py --output telemetry.jsonl --batches 5

# 2. Ingest new raw files → Bronze + DLQ
poetry run python jobs/bronze_ingestion.py

# 3. Bronze → Silver
poetry run python jobs/silver_transform.py

# 4. Silver → Gold daily metrics + latest status
poetry run python jobs/gold_aggregation.py
```

Re-running bronze ingestion without new files is a no-op: already processed filenames stay in `data/state/processed_files.json`.

## Tests

```bash
poetry run pytest
```

Unit tests cover the simulator, file discovery/manifest, validation-adjacent transforms, Silver/Gold logic, incremental partition helpers, and pipeline metrics. Spark tests use PySpark locally (see `pyproject.toml` `pythonpath`).

Dev tools also include Ruff and mypy:

```bash
poetry run ruff check .
```

## Project layout

```
aws/
  glue/          Glue jobs: raw → bronze, bronze → silver, silver → gold
  iot/           IoT rule, device and Firehose/S3 IAM policies
configs/         Environment YAML (dev today)
jobs/            Local Spark/Python entrypoints
src/iot_platform/
  config/        YAML loader
  ingestion/     New-file discovery + processed-file manifest
  quality/       Telemetry validation
  schemas/       Spark telemetry schema
  simulator/     Device, fleet, event generator
  transforms/    Bronze enrich, Silver, Gold, dedupe, incremental partitions
  utils/         Spark session, logging, run metrics
tests/unit/
```

Library code under `src/iot_platform` is meant to stay I/O-light where possible (transforms take DataFrames). Jobs own Spark sessions, paths, and writes.

## Current scope (v1)

Included:

- Local end-to-end medallion batch pipeline
- Idempotent bronze file ingestion
- Quality routing to DLQ
- Incremental Gold partition rewrite by event date
- Glue job scripts and IoT/Firehose IAM stubs for AWS

Not in this README as a turnkey deploy: Terraform/CDK, Glue job scheduling, catalog/crawlers, and production Spark sizing. Glue scripts currently use a fixed S3 bucket name; treat that as environment-specific when you wire jobs in AWS.
