# Weather Pipeline — Requirements Document

*Version 1.1 — 2026-08-22*
*Status: reflects the system as implemented; known deviations are flagged with a **Gap** note.*

---

## 1. Introduction

### 1.1 Purpose

This document specifies the functional and non-functional requirements of the
Weather Pipeline: an ETL system that collects current weather observations,
validates them, stores them in an analytical warehouse, and serves the results
through a web dashboard.

### 1.2 Scope

The system covers five capabilities:

1. **Extraction** — fetching current weather from the OpenWeather API.
2. **Transformation & quality control** — PySpark-based cleansing, validation,
   quarantine routing, and Parquet output.
3. **Warehousing** — loading validated data into a DuckDB star schema.
4. **Orchestration** — running the pipeline end-to-end via Prefect, on an
   unattended 5-minute schedule.
5. **Serving** — a FastAPI dashboard exposing warehouse analytics, interactive
   weather charts, and live city search.

Out of scope (see Section 7): historical backfills, scheduling/deployment to
remote infrastructure, authentication, and multi-tenant access.

### 1.3 Definitions

| Term | Definition |
|------|------------|
| Bronze / Silver / Gold | Raw JSON layer / validated Parquet layer / DuckDB warehouse |
| Quarantine | Parquet area for records failing validation; never silently dropped |
| Circuit breaker | Job-level guard that aborts the run when data volume/quality is unacceptable |
| Natural key | (`city_id`, `reading_timestamp`) — identifies one reading |
| Tracked city | One of the 10 hardcoded cities monitored on every extraction run |

### 1.4 References

- `PROJECT_GUIDELINES.md` — engineering conventions and quality checks.
- OpenWeather Current Weather API 2.5 (`/data/2.5/weather`).
- `tests/` — executable specification of stage behavior.

---

## 2. Overall Description

### 2.1 Product Perspective

A single-user, locally-run data product on Windows. Data flows one way:

```
OpenWeather API → raw JSON → Parquet (clean + quarantine) → DuckDB → HTTP dashboard
```

The pipeline self-schedules every 5 minutes; the dashboard reads the warehouse
read-only, never mutates pipeline artifacts, and picks up new results on its
own refresh cycle (60 s) — no manual reruns needed.

### 2.2 Users

| User | Use case |
|------|----------|
| Data engineer | Runs/extends the pipeline, inspects logs, quarantine, and warehouse counts |
| Dashboard viewer | Views per-city aggregates and recent readings; searches any city's live weather |

### 2.3 Operating Environment

- Windows, Python 3.11+, virtualenv-local dependencies.
- PySpark 4.x (local mode), DuckDB, Prefect, FastAPI + Uvicorn, requests.
- Secrets provided via `.env` file (`OPENWEATHER_API_KEY`).

### 2.4 Constraints

- OpenWeather free-tier rate limits apply; one request per tracked city per run.
- Console output must be ASCII-only (Windows cp1252 consoles).
- The machine-global `CURL_CA_BUNDLE` may need clearing for pip installs.

### 2.5 Assumptions

- The API key is valid and has quota for 10 requests per run.
- The pipeline runs from the project root so relative `data/` paths resolve.

---

## 3. Functional Requirements

Priority: **M** = Must, **S** = Should, **C** = Could.

### 3.1 Extraction (FR-EXT)

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-EXT-1 | The system shall fetch current weather for all 10 tracked cities (Warsaw, Krakow, Lodz, Wroclaw, Poznan, Gdansk, Szczecin, Bydgoszcz, Lublin, Katowice) using metric units. | M |
| FR-EXT-2 | The system shall refuse to run when `OPENWEATHER_API_KEY` is missing or set to the placeholder value, raising `ValueError` before any HTTP call. | M |
| FR-EXT-3 | Each raw payload shall be stored as UTF-8 JSON under `data/raw/weather/YYYY/MM/DD/<city_slug>_<YYYYMMDD_HHMMSS>.json`, mirroring cloud date partitioning. | M |
| FR-EXT-4 | Raw storage shall be append-only; existing raw files must never be overwritten or mutated. | M |
| FR-EXT-5 | A failure fetching one city shall be logged with city context and must not abort the remaining batch. | M |
| FR-EXT-6 | Every outbound HTTP request shall carry an explicit timeout and ride a retry session: 3 attempts, exponential backoff, retrying transient failures (429/5xx/timeouts) while failing fast on 4xx client errors. | M |
| FR-EXT-7 | If every city fetch in a run fails, extraction shall raise so the scheduled run is marked failed instead of silently aging the warehouse. | M |
| FR-EXT-8 | The tracked city list shall be configurable via `WEATHER_CITIES` (comma-separated), falling back to the 10-city default when unset or blank. | S |

### 3.2 Transformation & Quality Control (FR-TRF)

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-TRF-1 | Raw JSON shall be read with an explicit Spark schema (no inference) using recursive file lookup and multiLine mode. | M |
| FR-TRF-2 | Nested payloads shall be flattened into one row per reading containing: `city_id`, `city_name`, `country`, `latitude`, `longitude`, `reading_timestamp`, `temp_celsius`, `feels_like_celsius`, `humidity_pct`, `pressure_hpa`, `wind_speed_m_s`, `weather_condition`, `weather_description`. | M |
| FR-TRF-3 | Records shall be deduplicated on the natural key (`city_id`, `reading_timestamp`) before validation. | M |
| FR-TRF-4 | Each record shall be evaluated against six row-level DQ rules producing a `validation_errors` array and boolean `is_valid`: <br>• `NULL_CITY_ID` — city_id is null<br>• `NULL_READING_TIMESTAMP` — reading_timestamp is null<br>• `TEMP_OUT_OF_BOUNDS` — temp outside −90…60 °C<br>• `HUMIDITY_OUT_OF_BOUNDS` — humidity outside 0…100 %<br>• `LATITUDE_OUT_OF_BOUNDS` — latitude outside −90…90<br>• `LONGITUDE_OUT_OF_BOUNDS` — longitude outside −180…180 | M |
| FR-TRF-5 | Boundary values (e.g., exactly 0 % humidity) shall pass validation; only strictly out-of-range values fail. | M |
| FR-TRF-6 | The job shall abort with a non-zero exit code if zero records remain after transformation (zero-record circuit breaker). | M |
| FR-TRF-7 | The job shall fail if invalid records exceed 20 % of the batch (error-rate circuit breaker). Threshold changes require documented justification. | M |
| FR-TRF-8 | Valid records shall be written to `data/processed/weather/` in overwrite mode as a single coalesced Parquet file. | M |
| FR-TRF-9 | Invalid records shall be appended to `data/quarantine/weather/` with `validation_errors` serialized to a comma-separated string, and only when at least one invalid record exists. Invalid records never reach the clean output. | M |
| FR-TRF-10 | Each output row shall carry a `processed_at` audit timestamp. | M |
| FR-TRF-11 | Stale Spark `_temporary` directories shall be removed before overwrite writes so reruns do not fail on Windows file locks. | M |

### 3.3 Warehousing (FR-DWH)

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-DWH-1 | The loader shall refuse to build when the processed directory is missing, contains no Parquet files, or contains zero records, raising explicit errors. | M |
| FR-DWH-2 | The warehouse shall follow a star schema: `dim_cities` (one row per `city_id`) and `fact_weather_readings` (one row per reading), plus mart view `vw_city_weather_metrics`. | M |
| FR-DWH-3 | `dim_cities` shall keep the latest reading per `city_id` (dedup by `ROW_NUMBER()` over `city_id` ordered by `reading_timestamp DESC`); duplicate dimension rows must never exist. | M |
| FR-DWH-4 | Surrogate keys (`city_key`, `reading_key`) shall be generated via `ROW_NUMBER()`; joins between fact and dimension use business key `city_id`. | M |
| FR-DWH-5 | Fact↔dim joins must not fan out (each fact matches exactly one dimension row). | M |
| FR-DWH-6 | `vw_city_weather_metrics` shall expose per-city totals and aggregates: `total_readings`, `avg_temp_c`, `max_temp_c`, `min_temp_c`, `avg_humidity_pct`, `avg_wind_speed`, grouped by city name and country. | M |
| FR-DWH-7 | Every load shall end with printed validation output: row counts for both tables, fact↔dim join sample, and mart summary. | M |
| FR-DWH-8 | Database connections shall be closed in a `try/finally` block regardless of success or failure. | M |

### 3.4 Orchestration (FR-ORCH)

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-ORCH-1 | A single command (`python src\orchestrate_weather.py`) shall execute extract → transform → warehouse in strict sequential order. | M |
| FR-ORCH-2 | A downstream task shall not start until the upstream task completes successfully (Prefect `wait_for` dependencies). | M |
| FR-ORCH-3 | Extract task shall retry twice with a 10-second delay; transform shall retry once; warehouse shall retry twice with a 15-second delay (absorbs transient DuckDB file-lock contention with the running dashboard). | M |
| FR-ORCH-4 | Individual stage scripts shall remain independently runnable for debugging. | S |
| FR-ORCH-5 | The pipeline shall execute unattended every 5 minutes (`flow.serve(interval=...)`), performing one immediate run at startup so data exists before the first scheduled interval; the interval is configurable via `SCHEDULE_INTERVAL_SECONDS` (minimum 60 s). | M |
| FR-ORCH-6 | A single-run mode (`--once` flag) shall be available for smoke tests and debugging without starting the scheduler. | M |
| FR-ORCH-7 | Scheduled runs shall accumulate history: raw JSON appends to date partitions, and each run's warehouse rebuild includes all prior readings (deduplicated by natural key), so time-series charts grow over time. | M |

### 3.5 Serving & Dashboard (FR-SRV)

| ID | Requirement | Pri |
|----|-------------|-----|
| FR-SRV-1 | The service shall expose `GET /health` returning `{"status": "ok"}`. | M |
| FR-SRV-2 | `GET /api/city-metrics` shall return all rows of `vw_city_weather_metrics` sorted by city name. | M |
| FR-SRV-3 | `GET /api/readings` shall return the newest readings first, accepting `limit` (1–500, default 25) and optional `city_id` filter; out-of-range `limit` returns 422. | M |
| FR-SRV-4 | `GET /api/search?city=` shall return live OpenWeather conditions for any city merged with up-to-10 latest warehouse history entries for that city. | M |
| FR-SRV-5 | Search error handling: unknown city → 404 with actionable message; missing API key → 503; provider/network failure → 502; blank name → 422. Provider exceptions must never surface as unhandled 500s. | M |
| FR-SRV-6 | When the warehouse does not exist, metrics/readings endpoints return 503 with an actionable message, and search still succeeds with empty `warehouse_history`. | M |
| FR-SRV-7 | All warehouse access shall use short-lived **read-only** DuckDB connections opened per request and closed in `finally`; the API never writes to `data/`. | M |
| FR-SRV-8 | Live search HTTP calls shall set an explicit timeout (currently 10 s). | M |
| FR-SRV-9 | `GET /` shall serve an HTML dashboard showing: a temperature-trend line chart (one line per city over reading time), an average-temp/humidity bar chart per city, the metrics table, and the recent-readings table — auto-refreshing every 60 s so new scheduled-run results appear without manual reloads. | M |
| FR-SRV-10 | SQL parameters shall be bound (DuckDB `$param` style), never string-interpolated from user input. | M |
| FR-SRV-11 | `GET /api/timeseries` shall return per-city chronological points (`ts`, `temp_celsius`, `feels_like_celsius`, `humidity_pct`) for charting, accepting `limit` (1–10000, default 2000). | M |
| FR-SRV-12 | `GET /api/status` shall report warehouse freshness (`warehouse_exists`, `latest_reading_ts`, `latest_processed_at`) and shall return 200 with `warehouse_exists: false` rather than erroring when no warehouse has been built. | M |
| FR-SRV-13 | Numeric values in API responses shall be JSON numbers, never strings (DuckDB DECIMAL results normalized to float before serialization). | M |
| FR-SRV-14 | Dashboard polling shall tolerate transient warehouse unavailability (e.g., during a scheduled rebuild): a failed fetch leaves existing content intact and retries on the next refresh cycle instead of blanking the page. | S |
| FR-SRV-15 | Chart assets shall be served locally (`/static/chart.umd.min.js`) with an automatic CDN fallback, so charts render without internet access. | S |

---

## 4. Data Requirements

### 4.1 Sources

| Source | Type | Notes |
|--------|------|-------|
| OpenWeather `/data/2.5/weather` | REST JSON | Authenticated by API key; metric units; one call per city per run |
| `.env` | Config | `OPENWEATHER_API_KEY` only |

### 4.2 Data Layers & Retention

| Layer | Location | Mutability |
|-------|----------|------------|
| Bronze (raw JSON) | `data/raw/weather/YYYY/MM/DD/` | Append-only |
| Silver (clean Parquet) | `data/processed/weather/` | Overwritten per run |
| Quarantine (rejected Parquet) | `data/quarantine/weather/` | Append-only, written only when rejects exist |
| Gold (warehouse) | `data/weather_dwh.duckdb` | Rebuilt (`CREATE OR REPLACE`) per load |

### 4.3 Key Entities

- **dim_cities**: `city_key` (surrogate PK), `city_id` (business key), `city_name`, `country`, `latitude`, `longitude`.
- **fact_weather_readings**: `reading_key` (surrogate PK), `city_id` (FK), `reading_timestamp`, temp/feels-like/humidity/pressure/wind metrics, condition/description, `processed_at`.
- **vw_city_weather_metrics**: aggregated per-city view over fact ⋈ dim (see FR-DWH-6).

---

## 5. Non-Functional Requirements

| ID | Category | Requirement |
|----|----------|-------------|
| NFR-1 | Reliability | A single city failure degrades extraction, not the batch; transient extract failures are retried automatically (FR-ORCH-3). |
| NFR-2 | Reliability | Corrupt input never reaches analytical tables: DQ rules + circuit breakers guarantee bad batches abort or quarantine (FR-TRF-4…9). |
| NFR-3 | Auditability | Every rejected record is preserved with its failure reason; every load prints row-count validation. |
| NFR-4 | Security | API keys exist only in `.env` (gitignored); keys are never logged; all SQL is parameterized. |
| NFR-5 | Security | The serving layer opens read-only DB sessions only; no write path exists from the API to pipeline data. |
| NFR-6 | Performance | A full local run (10 cities, extract→transform→warehouse) completes in minutes on a laptop-class machine; the test suite completes in ~1 minute. |
| NFR-7 | Freshness | Dashboard content reflects each scheduled run within 60 s of its completion; a freshness indicator shows when the warehouse data was last updated. |
| NFR-8 | Maintainability | Full type hints, small single-purpose functions, constants at module top; every behavior change ships with a test (see guidelines §4, §6). |
| NFR-9 | Portability | Runs on Windows with Python 3.11+; console output ASCII-only; paths handled via `pathlib`. |
| NFR-10 | Operability | Failures raise specific exceptions with actionable messages; the dashboard exposes a liveness probe (`/health`). |
| NFR-11 | Isolation | Warehouse rebuilds are never blocked by the running API (read-only short-lived sessions); scheduler retries absorb residual lock contention. |

---

## 6. Acceptance Criteria (System Level)

A release is accepted when, after a full orchestrated run against a valid API key:

1. Ten fresh raw JSON files exist under today's date partition.
2. Exactly one clean Parquet file exists in the processed layer; its row count equals the warehouse fact count.
3. The transform log reports an error rate ≤ 20 %; quarantine exists only if rejects occurred.
4. `dim_cities` has no duplicate `city_id`; the mart view returns 10 rows.
5. All automated tests pass (`pytest -v`), lint/type checks pass (`ruff`, `mypy src`).
6. With the server running: `/health` returns OK, `/api/city-metrics` mirrors the mart view, `/api/timeseries` returns chronological per-city points, and a live search returns conditions plus any warehouse history.
7. Removing/renaming the warehouse makes `/api/city-metrics` return 503 while `/health` and `/api/status` stay OK.
8. With the scheduler running (`python src\orchestrate_weather.py`), a new run executes every 5 minutes and the dashboard shows updated charts, tables, and freshness within 60 s of each run.

---

## 7. Out of Scope / Future Work

- Historical backfill (OpenWeather history APIs) and time-series retention policies.
- Scheduler deployment to remote infrastructure (Prefect agent/cloud work pools); the dashboard currently binds to localhost only.
- Authentication/authorization on the dashboard and API.
- Configuration-driven DQ thresholds (cities and schedule interval are already env-configurable).
- CI/CD pipeline (ruff + mypy + pytest on push).

---

*Related documents: `PROJECT_GUIDELINES.md` (engineering conventions, quality gates).*
