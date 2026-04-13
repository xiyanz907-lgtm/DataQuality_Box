# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Cactus-Box** is an Apache Airflow 2.10.4-based data quality and governance platform. It validates operational port data by comparing an nGen database (port operations) against a Cactus BI warehouse, applying configurable YAML-defined rules and dispatching severity-graded alerts via email.

The system runs entirely inside Docker (LocalExecutor). Python 3.10 is the runtime. Dependencies are defined in `deploy/requirements.txt`.

## Commands

### Build & Deploy

```bash
# Build the custom Airflow Docker image
cd deploy && bash build.sh

# Deploy (checks prerequisites, creates volumes, starts services)
bash deploy.sh

# Stop services (preserve data)
bash stop.sh

# Stop and delete all data
bash stop.sh -f

# Backfill / rerun a DAG over a date range (edit variables inside the script)
bash rerun.sh
```

After `deploy.sh`, the Airflow UI is available at `http://localhost:8080`.

### Development Workflow

There is no local test runner configured. The platform is developed by modifying DAGs/plugins and deploying into Docker. To iterate:

```bash
# View live Airflow logs
docker compose logs -f airflow

# Restart Airflow only (picks up plugin/DAG changes without rebuild)
docker compose restart airflow
```

### Configuration

Copy `deploy/env.template` to `deploy/.env` and fill in all values before first deploy. Key variables:

| Variable | Purpose |
|---|---|
| `SITE_NAME` | Location identifier used in email subjects (e.g. `HIT`, `AQCT`) |
| `SITE_TIMEZONE` | IANA timezone string (e.g. `Asia/Hong_Kong`) |
| `GOVERNANCE_STORAGE_TYPE` | `local` or `minio` |
| `NGEN_*` / `CACTUS_*` / `DATALOG_*` / `QA_*` | MySQL connection params for each of the 4 databases |
| `SMTP_*` / `ALERT_EMAIL_TO` / `RECON_EMAIL_TO` | Email delivery settings |
| `INFLUX_*` | InfluxDB ground-truth validation |
| `PACKING_SERVICE_*` | Remote asset packing service |

## Architecture

### Data Flow

Raw data from MySQL/InfluxDB/HTTP passes through five operator stages, each storing intermediate results as Parquet files and passing a serialized `GovernanceContext` object via Airflow XCom:

```
UniversalLoaderOperator  →  Raw Parquet
DomainAdapterOperator    →  Entity Parquet   (standardized domain model)
GenericRuleOperator      →  Result Parquet   (per-rule violation records)
ContextAggregatorOperator →  Alerts + Assets  (reduces all rule results)
NotificationDispatcherOperator → Emails (P0/P1/P2 severity routing)
```

### GovernanceContext

`plugins/domain/context.py` — the central carrier object. It holds `DataRef` (Parquet file URIs), `RuleOutputRef` (rule result pointers), a list of `Alert` objects, and `Asset` packing payloads. It is serialized to JSON and stored in XCom as the `return_value` of each operator, then deserialized by the next task.

### BaseGovernanceOperator

`plugins/infra/operators.py` — all custom operators extend this. It implements a template-method pattern:
1. `pre_execute`: deserializes upstream `GovernanceContext` from XCom, injects `IOStrategy`
2. `execute_logic`: subclass business logic (in-place mutation of `ctx`)
3. `post_execute`: logs summary, returns re-serialized `ctx`

### YAML-Driven Rules

Rules live in `plugins/configs/rules/*.yaml`. Each rule specifies:
- `rule_id`, `severity` (P0/P1/P2)
- `filter` or `aggregate` logic operating on the Entity Parquet
- `asset_field_mapping` for downstream packing

`plugins/orchestration/rule_scanner.py` discovers rules at DAG parse time. `plugins/orchestration/dag_factory.py` wires them into operator chains automatically — no Python changes needed to add or modify a rule.

### DAG Topology

| DAG | Trigger | Purpose |
|---|---|---|
| `dynamic_governance_dags.py` | Per source schedule | Auto-generated from `configs/sources/*.yaml`; runs the full operator pipeline |
| `scanner_cactus_monitor.py` | Hourly sensor | Detects Cactus table changes, triggers reconciliation |
| `reconciliation_worker_dag.py` | Triggered | nGen vs Cactus row-level reconciliation; auto-inserts orphaned nGen records |
| `global_alert_reporter.py` | Daily | Sends consolidated alert digest |
| `asset_packing_dag.py` | Dataset event | Fires when `GOVERNANCE_ASSET_DATASET` is written; calls external packing service |
| `manual_intake_dag.py` | Manual | Ad-hoc data intake |
| `maintenance_dag.py` | Daily 02:00 UTC | Prunes XCom (7d), logs (30d), rule results (90d), Parquet cache (7d) |

### Extractor Layer

`plugins/infra/extractors/` — pluggable data sources. `UniversalLoaderOperator` selects the extractor based on `source_type` in the source YAML. Extractors support `depends_on` (upstream `DataRef`) for chained queries where a later query uses results from an earlier one.

### Storage Abstraction

`plugins/infra/io_strategy.py` — `LocalIOStrategy` writes to `/opt/airflow/data/governance/` (Docker-mounted from `../data/`). `MinIOIOStrategy` writes to an S3-compatible bucket. Switched via `GOVERNANCE_STORAGE_TYPE`.

## Extending the System

**Add a data quality rule**: Create `plugins/configs/rules/<rule_id>.yaml` following the filter/aggregate schema. The rule scanner picks it up automatically on next DAG parse.

**Add a data source**: Create `plugins/configs/sources/<name>.yaml`. `dynamic_governance_dags.py` generates a new DAG for it automatically.

**Add an adapter**: Create `plugins/configs/adapters/<name>.yaml` to define raw→entity field mapping transformations.

**Add an extractor**: Subclass `BaseExtractor` in `plugins/infra/extractors/base.py` and register the new `source_type`.
