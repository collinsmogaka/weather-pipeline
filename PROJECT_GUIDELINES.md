# Weather Pipeline — Project Guidelines & Quality Checks

## 1. Project Overview

An ETL pipeline that collects current weather data for a fixed set of **10 Polish
cities**, cleans and validates it with PySpark, loads it into a DuckDB
star-schema warehouse, orchestrates the whole flow with Prefect (**scheduled to
run automatically every 5 minutes**), and serves the results through a FastAPI
dashboard with interactive charts and a live city-search feature.

```
OpenWeather API ──► Raw JSON (bronze) ──► Parquet (silver) ──► DuckDB DWH (gold)
   extract_weather.py    transform_weather.py      warehouse_weather.py

        orchestrate_weather.py  (Prefect flow: extract -> transform -> warehouse,
                                 served on a 5-minute interval schedule)
                                        │
                                 serve_weather.py  (FastAPI dashboard: charts +
                                                   live search, polls warehouse)
```

| Stage        | Script                        | Input             | Output                                  |
|--------------|-------------------------------|-------------------|-----------------------------------------|
| Extract      | `src/extract_weather.py`      | OpenWeather API   | `data/raw/weather/YYYY/MM/DD/*.json`    |
| Transform    | `src/transform_weather.py`    | Raw JSON          | `data/processed/weather/*.parquet` (+ quarantine) |
| Warehouse    | `src/warehouse_weather.py`    | Processed Parquet | `data/weather_dwh.duckdb`               |
| Orchestration| `src/orchestrate_weather.py`  | —                 | Runs the three stages in order, every 5 min (or once with `--once`) |
| Serving      | `src/serve_weather.py`        | DuckDB DWH (+ live API for search) | HTTP dashboard + charts on `127.0.0.1:8000` |

Warehouse model:
- **dim_cities** — one row per `city_id`, deduplicated on latest reading.
- **fact_weather_readings** — one row per reading (`city_id`, metrics, timestamp).
- **vw_city_weather_metrics** — analytical mart view joining fact + dim.

---

## 2. Environment Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install pyspark duckdb requests python-dotenv prefect fastapi uvicorn
pip freeze > requirements.txt   # keep this file committed and current
```

Rules:
1. **Never install packages outside `venv`.**
2. **Any new dependency must be added to `requirements.txt` in the same PR**
   (`requirements.txt` is pinned from `pip freeze`).
3. Requires Python 3.11+ (PySpark 4.x).
4. All commands below are run from the project root (`F:\weather-pipeline`),
   with the venv activated.

Configuration (all optional, see `src/config_weather.py`):
- `WEATHER_CITIES` — comma-separated override of the tracked city list
  (default: the 10 Polish cities).
- `SCHEDULE_INTERVAL_SECONDS` — orchestration interval (default `300`;
  values below 60 are rejected to protect OpenWeather free-tier limits).

Secrets:
- API keys live ONLY in `.env` (`OPENWEATHER_API_KEY`). `.env` must NEVER be
  committed. Maintain a `.gitignore` containing: `venv/`, `.env`,
  `data/`, `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`.
- The serving layer re-reads the key from the environment at request time
  (`_resolve_api_key()`); never bake keys into module-level constants there.

---

## 3. Project Structure Conventions

```
weather-pipeline/
├── src/                    # pipeline stages + API server (production code)
│   ├── config_weather.py   # env-driven configuration with defaults
│   ├── extract_weather.py
│   ├── transform_weather.py
│   ├── warehouse_weather.py
│   ├── orchestrate_weather.py
│   ├── serve_weather.py
│   └── static/             # vendored browser assets (chart.umd.min.js)
├── tests/                  # unit + integration tests (mirror src/ layout)
├── scripts/                # ad-hoc/debug utilities (run from project root)
├── data/
│   ├── raw/weather/        # bronze — immutable, append-only
│   ├── processed/weather/  # silver — overwritten per run
│   ├── quarantine/weather/ # rejected records + validation_errors column
│   └── weather_dwh.duckdb  # gold warehouse
├── pyproject.toml          # ruff / mypy / pytest configuration
├── requirements.txt        # pinned via pip freeze — keep current
├── .env                    # secrets — never commit
└── PROJECT_GUIDELINES.md
```

Rules:
1. New pipeline stages go in `src/` with filename pattern `<verb>_weather.py`.
2. Scratch/debug files do NOT stay in the repo root; move them to `scripts/`
   or delete them.
3. Never hand-edit anything under `data/` except for local experiments;
   the directories are pipeline-owned artifacts.

---

## 4. Coding Standards

Follow the style already established in `src/warehouse_weather.py`:

1. **Type hints are mandatory** on every function signature
   (`def get_parquet_files() -> list[str]:`).
2. **Small single-purpose functions**, orchestrated by a `main()` function
   guarded by `if __name__ == "__main__":`.
3. **Constants at module top** in UPPER_SNAKE_CASE (e.g., `DATABASE_PATH`,
   `PROCESSED_PATH`, `CITIES`).
4. Paths via `pathlib.Path`; use `.as_posix()` when passing paths to
   Spark/DuckDB readers.
5. Resources (DB connections, Spark sessions) closed/stopped in a
   `try/finally` block — see `main()` in `warehouse_weather.py`. The serving
   layer opens short-lived **read-only** DuckDB connections per request and
   closes them in `finally` so the API never blocks a pipeline rebuild.
6. Comments explain **why** (e.g., "Deduplicates strictly on city_id..."),
   not what. No commented-out dead code in commits.
7. Fail loudly: raise specific exceptions with actionable messages
   (`FileNotFoundError`, `RuntimeError`) instead of returning silently.
8. Print statements are acceptable as pipeline progress logs for now; prefix
   sections consistently (`--- Section ---`). Do not log secrets or full API keys.
   Keep console output ASCII-only (`->`, not `→`): Windows cp1252 consoles
   raise `UnicodeEncodeError` on non-ASCII prints and abort mid-run.

---

## 5. Pipeline Development Rules

### Extract
- Validate `API_KEY` presence before making any HTTP call (existing guard in
  `run_extraction()` must not be removed).
- Every `requests` call sets an explicit timeout and goes through the shared
  retry session (`build_http_session()`: 3 attempts, backoff factor 2,
  retries on 429/5xx/timeouts; 4xx client errors fail immediately).
- One city failing must not abort the batch (current behavior is correct);
  failures are printed with city context.
- If ALL cities fail, `run_extraction()` raises — a fully stale batch must be
  visible as a failed run, never as silently aging warehouse data.
- Raw JSON is **append-only**: never overwrite or mutate existing raw files.
- Raw files are date-partitioned (`YYYY/MM/DD`) with
  `<city_slug>_<timestamp>.json` naming — keep this contract stable, tests
  assert it.

### Transform (PySpark)
- Read with an **explicit schema** (`get_raw_weather_schema()`) — never rely
  on schema inference for production runs.
- Deduplicate on natural keys before validation:
  `dropDuplicates(["city_id", "reading_timestamp"])`.
- Row-level quality rules live in one place (the `error_conditions` array).
  Current enforced rules:
  | Rule code                 | Condition                          |
  |---------------------------|------------------------------------|
  | `NULL_CITY_ID`            | `city_id IS NULL`                  |
  | `NULL_READING_TIMESTAMP`  | `reading_timestamp IS NULL`        |
  | `TEMP_OUT_OF_BOUNDS`      | temp outside −90…60 °C             |
  | `HUMIDITY_OUT_OF_BOUNDS`  | humidity outside 0…100 %           |
  | `LATITUDE_OUT_OF_BOUNDS`  | latitude outside −90…90            |
  | `LONGITUDE_OUT_OF_BOUNDS` | longitude outside −180…180         |
- **Zero-record circuit breaker:** abort (exit non-zero) if the transformed
  DataFrame is empty.
- **Error-rate circuit breaker:** fail the job if invalid records exceed
  **20 %** of the batch. Any change to these thresholds requires a documented
  reason in the PR description.
- Rule violations are collected with `array_compact()` over the
  `error_conditions` array. Do NOT switch back to `array_remove(..., NULL)`:
  on Spark 4.x that call returns NULL whenever the search element is NULL,
  which silently neutralized every DQ rule (invalid count always 0, clean
  sink always empty). Regression coverage:
  `tests/test_transform.py::test_dq_rules_fire_quarantine_and_clean_output`.
- Invalid rows go to quarantine with `validation_errors` serialized
  (`concat_ws`); they are never silently dropped and never reach Parquet output.
- Writes: clean data uses `mode("overwrite")` + `coalesce(1)`; quarantine uses
  `mode("append")` and is written only when invalid records exist.
- Keep `clear_stale_temp_dirs()` — leftover Spark `_temporary` dirs block
  `mode("overwrite")` on Windows.

### Warehouse (DuckDB)
- Refuse to load if processed Parquet is missing or contains zero records
  (guards already implemented — keep them).
- Dimensions must be deduplicated on business key before insert (see
  `dim_cities` `ROW_NUMBER() OVER (PARTITION BY city_id ...)` pattern).
- Every load ends with `validate_warehouse()` output reviewed: row counts,
  fact↔dim join sanity, and mart view summary.
- Surrogate keys (`*_key`) are generated by `ROW_NUMBER()`; business keys
  (`city_id`) are used for joins.

### Orchestration (Prefect)
- `src/orchestrate_weather.py` is the single end-to-end entry point. Default
  behavior: one immediate run, then `flow.serve(interval=300)` keeps the
  pipeline executing **every 5 minutes** unattended. Pass `--once` for a
  single run (used by smoke tests and debugging).
- Stages stay strictly sequential: each task receives `wait_for=[...]` on the
  previous task's result. Do not parallelize without redesigning the data
  contract between stages.
- Retry policy: extract `retries=2, retry_delay_seconds=10`; transform
  `retries=1`; warehouse `retries=2, retry_delay_seconds=15` (retries absorb
  transient DuckDB file-lock contention with concurrently open read-only
  dashboard sessions). Changing retry policy is a deliberate decision, note
  it in the PR.
- Tasks import from `src.*`; keep the `sys.path.append(...)` bootstrap at the
  top so direct execution works.
- Scheduled runs accumulate raw JSON under new date partitions; the transform
  stage re-reads ALL history and deduplicates on (`city_id`,
  `reading_timestamp`), so the warehouse keeps growing across runs. Keep this
  property intact — charts depend on multi-run time series.

### Serving (FastAPI)
- `src/serve_weather.py` binds to `127.0.0.1:8000`. It reads the warehouse
  read-only; the API NEVER writes to `data/` or rebuilds tables.
- Endpoints (contract asserted by `tests/test_serve_weather.py`):
  | Endpoint                | Purpose                                              | Errors |
  |-------------------------|------------------------------------------------------|--------|
  | `GET /health`           | liveness probe                                       | — |
  | `GET /api/city-metrics` | rows of `vw_city_weather_metrics`, sorted by name    | 503 if warehouse missing |
  | `GET /api/readings`     | newest-first readings; `limit` 1–500 (default 25), optional `city_id` filter | 503 / 422 |
  | `GET /api/timeseries`   | per-city chronological points for the trend chart; `limit` 1–10000 (default 2000) | 503 / 422 |
  | `GET /api/status`       | warehouse freshness (latest reading/processed timestamps); returns `warehouse_exists: false` instead of erroring when absent | — |
  | `GET /api/search`       | live OpenWeather lookup + warehouse history (latest 10), merged into one payload | 404 unknown city, 503 no API key, 502 upstream/network failure, 422 blank name |
  | `GET /`                 | HTML dashboard: Chart.js trend + bar charts, tables, auto-refresh every 60 s | — |
- Live search must set `timeout=` on `requests.get()` (currently 10 s) and map
  upstream failures to explicit status codes — never let a provider exception
  surface as a 500.
- Warehouse absence must degrade gracefully: metrics/readings/timeseries
  endpoints return 503 with an actionable message; search still returns live
  conditions with empty `warehouse_history`; `/api/status` reports
  `warehouse_exists: false`.
- DuckDB DECIMAL columns arrive as Python `Decimal`; `_query_dicts()` normalizes
  them to float because Pydantic v2 would serialize Decimals as JSON strings.
  Keep that normalization in place.
- Dashboard JS tolerates transient DB-lock errors during scheduled rebuilds:
  table/chart fetches are wrapped in try/catch so one failed poll does not
  blank the page until the next refresh.
- Chart.js is vendored at `src/static/chart.umd.min.js` (v4.5.1) and served at
  `/static/chart.umd.min.js`, with an automatic CDN fallback in the page —
  the dashboard works offline. When upgrading, re-download the asset and
  update the version noted here and in `REQUIREMENTS.md`.

---

## 6. Testing

The suite lives in `tests/` mirroring `src/` (plus `tests/conftest.py` for
import bootstrapping and `tests/fixtures/raw/` for tiny OpenWeather-style
fixture JSONs). Run everything from the project root with the venv active:

```powershell
pip install pytest   # already in venv
pytest -v            # ~1 min; PySpark session startup dominates
```

| Test file                     | Coverage |
|-------------------------------|----------|
| `tests/test_config.py`        | `WEATHER_CITIES` parsing (defaults, trimming, blank filtering); schedule interval default/env override; intervals < 60 s rejected |
| `tests/test_extract.py`       | API-key guard raises on missing AND placeholder key; date-partitioned raw dir + `city_slug_<timestamp>.json` naming; one city failing does not abort the batch; all cities failing raises `RuntimeError`; fetch passes explicit timeout; retry session mounted with configured attempts/backoff |
| `tests/test_transform.py`     | Each of the 6 DQ rules fires and quarantines with the right error code; inclusive boundary values stay valid; natural-key dedup collapses duplicate readings; >20 % error-rate breaker raises `ValueError`; zero-record breaker exits non-zero; quarantine written only when invalid > 0 |
| `tests/test_warehouse.py`     | `dim_cities` dedup yields unique `city_id` with latest reading winning; fact↔dim join has no fan-out; mart view totals correct; missing/empty Parquet guards raise; zero-record quick scan rejected |
| `tests/test_serve_weather.py` | `/health` OK; metrics sorted by city with correct aggregates; readings newest-first with `limit`/`city_id` filtering and 422 on bad `limit`; timeseries grouped per city in chronological order with enforced `limit`; status reports freshness and degrades to `warehouse_exists: false` without warehouse; dashboard HTML served with local Chart.js asset; vendored chart.js served; 503 when warehouse missing; live search merges payload + history; 404/502/503/422 error mapping for search |

Rules:
1. Tests run against the tiny fixture JSON files under `tests/fixtures/raw/` —
   no network calls, no real API key. Serve tests mock `requests.get` with a
   `FakeResponse`.
2. Tests never touch the real `data/` tree: extract/warehouse tests use
   `monkeypatch.chdir(tmp_path)` or repoint `DATABASE_PATH`; transform tests
   repoint the module constants `RAW_PATH` / `PROCESSED_PATH` /
   `QUARANTINE_PATH` to absolute tmp paths — required because the Spark JVM
   resolves relative paths against its own working directory, not Python's cwd.
   Serve tests build a throwaway DuckDB fixture per test via `tmp_path`.
3. Transform tests share one SparkSession per pytest process
   (`getOrCreate()`); never call `spark.stop()` inside a test.
4. New behavior needs a test in this suite before merge.

---

## 7. Quality Checks — Run Before Every Commit/Merge

```powershell
# 1. Formatting & linting (config lives in pyproject.toml)
pip install ruff mypy
ruff format src tests
ruff check src tests
mypy                # reads files = ["src"] from pyproject.toml

# 2. Tests
pytest              # reads testpaths from pyproject.toml

# 3. Full pipeline smoke test (requires valid .env)
python src\orchestrate_weather.py --once    # single end-to-end run
python src\orchestrate_weather.py           # run now + schedule every 5 min

# ...or stage-by-stage:
python src\extract_weather.py
python src\transform_weather.py
python src\warehouse_weather.py

# 4. Dashboard manual check
python src\serve_weather.py                 # then open http://127.0.0.1:8000
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/api/status
```

Post-run acceptance checks (all must hold):

- [ ] `data/raw/weather/**` gained new JSON files for all 10 cities.
- [ ] `data/processed/weather/` contains exactly one fresh Parquet file
      (`overwrite` mode ⇒ old file replaced, not accumulated).
- [ ] Transform log shows `Data Quality Audit:` line with error rate ≤ 20 %.
- [ ] Quarantine only written when `invalid_records > 0`.
- [ ] `validate_warehouse()` prints non-zero row counts for BOTH tables.
- [ ] Fact row count == count of clean Parquet records (no silent loss):
      ```sql
      SELECT COUNT(*) FROM fact_weather_readings;
      ```
- [ ] `SELECT COUNT(*) FROM (SELECT city_id FROM dim_cities GROUP BY city_id HAVING COUNT(*) > 1);`
      returns 0 rows (no dimension duplicates).
- [ ] Mart view returns 10 rows (one per tracked city); `/api/city-metrics`
      returns the same set.
- [ ] `/health` returns `{"status": "ok"}`; dashboard renders both tables and
      both charts; `/api/status` reports a fresh `latest_reading_ts`.
- [ ] With the scheduler running, fact row counts grow after each 5-minute
      interval (raw accumulates across runs).

### Merge checklist (PR author)

- [ ] `requirements.txt` updated if dependencies changed.
- [ ] Type hints on all new/changed functions.
- [ ] No secrets, keys, or absolute local paths (like `F:\...`) in code.
- [ ] Circuit-breaker thresholds unchanged (or justified in PR body).
- [ ] New DQ rules added to the rule table in Section 5.
- [ ] Debug scripts removed from root / moved to `scripts/`.
- [ ] Lint, typecheck, and tests all green locally.

---

## 8. Known Gaps / Roadmap

1. Replace `print` logging with the `logging` module (medium term — the only
   remaining large refactor; keep ASCII-only console output when doing it).
2. Add CI workflow running ruff + mypy + pytest on push.
3. Environment gotcha: the machine-global `CURL_CA_BUNDLE` points at a
   nonexistent PostgreSQL cert path (`C:\Program Files\PostgreSQL\18\ssl\...`),
   which breaks every pip install with a TLS CA-bundle error until it is
   cleared for the session (`$env:CURL_CA_BUNDLE = $null`).

Recently completed (2026-08-22): extract timeout + retry/backoff, all-cities-
failed guard, env-driven cities/schedule config (`src/config_weather.py`),
debug scripts moved to `scripts/`, `.gitignore` extended, `pyproject.toml`
tool configs, vendored Chart.js v4.5.1, `requirements.txt` pinned.

---

*Last updated: 2026-08-22*
