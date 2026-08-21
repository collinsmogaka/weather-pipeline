# Weather Pipeline — Project Guidelines & Quality Checks

## 1. Project Overview

An ETL pipeline that collects current weather data for a fixed set of cities,
cleans and validates it with PySpark, and loads it into a DuckDB star-schema
warehouse.

```
OpenWeather API ──► Raw JSON (bronze) ──► Parquet (silver) ──► DuckDB DWH (gold)
   extract_weather.py    transform_weather.py      warehouse_weather.py
```

| Stage     | Script                     | Input                        | Output                                  |
|-----------|----------------------------|------------------------------|-----------------------------------------|
| Extract   | `src/extract_weather.py`   | OpenWeather API              | `data/raw/weather/YYYY/MM/DD/*.json`    |
| Transform | `src/transform_weather.py` | Raw JSON                     | `data/processed/weather/*.parquet` (+ quarantine) |
| Warehouse | `src/warehouse_weather.py` | Processed Parquet            | `data/weather_dwh.duckdb`               |

Warehouse model:
- **dim_cities** — one row per `city_id`, deduplicated on latest reading.
- **fact_weather_readings** — one row per reading (`city_id`, metrics, timestamp).
- **vw_city_weather_metrics** — analytical mart view joining fact + dim.

---

## 2. Environment Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install pyspark duckdb requests python-dotenv
pip freeze > requirements.txt   # keep this file committed and current
```

Rules:
1. **Never install packages outside `venv`.**
2. **Any new dependency must be added to `requirements.txt` in the same PR.**
3. Requires Python 3.11+ (PySpark 4.x).
4. All commands below are run from the project root (`F:\weather-pipeline`),
   with the venv activated.

Secrets:
- API keys live ONLY in `.env` (`OPENWEATHER_API_KEY`). `.env` must NEVER be
  committed. Maintain a `.gitignore` containing: `venv/`, `.env`,
  `data/`, `__pycache__/`, `.pytest_cache/`.

---

## 3. Project Structure Conventions

```
weather-pipeline/
├── src/                    # pipeline stages (production code)
├── tests/                  # unit + integration tests (mirror src/ layout)
├── data/
│   ├── raw/weather/        # bronze — immutable, append-only
│   ├── processed/weather/  # silver — overwritten per run
│   ├── quarantine/weather/ # rejected records + validation_errors column
│   └── weather_dwh.duckdb  # gold warehouse
├── scripts/                # ad-hoc/debug utilities (move check_weather.py, debug_*.py here)
├── requirements.txt
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
   `try/finally` block — see `main()` in `warehouse_weather.py`.
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
- Always set `timeout=` on `requests.get()` (currently missing — fix required).
- One city failing must not abort the batch (current behavior is correct);
  failures are printed with city context.
- Raw JSON is **append-only**: never overwrite or mutate existing raw files.

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
  `mode("append")`.

### Warehouse (DuckDB)
- Refuse to load if processed Parquet is missing or contains zero records
  (guards already implemented — keep them).
- Dimensions must be deduplicated on business key before insert (see
  `dim_cities` `ROW_NUMBER() OVER (PARTITION BY city_id ...)` pattern).
- Every load ends with `validate_warehouse()` output reviewed: row counts,
  fact↔dim join sanity, and mart view summary.
- Surrogate keys (`*_key`) are generated by `ROW_NUMBER()`; business keys
  (`city_id`) are used for joins.

---

## 6. Testing

The suite lives in `tests/` mirroring `src/` (plus `tests/conftest.py` for
import bootstrapping and `tests/fixtures/raw/` for tiny OpenWeather-style
fixture JSONs). Run everything from the project root with the venv active:

```powershell
pip install pytest   # already in venv
pytest -v            # ~1 min; PySpark session startup dominates
```

| Test file                 | Coverage |
|---------------------------|----------|
| `tests/test_extract.py`   | API-key guard raises on missing AND placeholder key; date-partitioned raw dir + `city_slug_<timestamp>.json` naming; one city failing does not abort the batch |
| `tests/test_transform.py` | Each of the 6 DQ rules fires and quarantines with the right error code; inclusive boundary values stay valid; natural-key dedup collapses duplicate readings; >20 % error-rate breaker raises `ValueError`; zero-record breaker exits non-zero; quarantine written only when invalid > 0 |
| `tests/test_warehouse.py` | `dim_cities` dedup yields unique `city_id` with latest reading winning; fact↔dim join has no fan-out; mart view totals correct; missing/empty Parquet guards raise; zero-record quick scan rejected |

Rules:
1. Tests run against the tiny fixture JSON files under `tests/fixtures/raw/` —
   no network calls, no real API key.
2. Tests never touch the real `data/` tree: extract/warehouse tests use
   `monkeypatch.chdir(tmp_path)`; transform tests repoint the module constants
   `RAW_PATH` / `PROCESSED_PATH` / `QUARANTINE_PATH` to absolute tmp paths —
   required because the Spark JVM resolves relative paths against its own
   working directory, not Python's cwd.
3. Transform tests share one SparkSession per pytest process
   (`getOrCreate()`); never call `spark.stop()` inside a test.
4. New behavior needs a test in this suite before merge.

---

## 7. Quality Checks — Run Before Every Commit/Merge

```powershell
# 1. Formatting & linting (add configs to pyproject.toml)
pip install ruff mypy
ruff format src tests
ruff check src tests --fix
mypy src

# 2. Tests
pytest -v

# 3. Full pipeline smoke test (requires valid .env)
python src\extract_weather.py
python src\transform_weather.py
python src\warehouse_weather.py
```

Post-run acceptance checks (all must hold):

- [ ] `data/raw/weather/**` gained new JSON files for all 6 cities.
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
- [ ] Mart view returns 6 rows (one per city).

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

1. Create `requirements.txt` (immediate).
2. Add `.gitignore` and initialize git (immediate).
3. Add `timeout` + retry/backoff to `fetch_weather_data()` (high priority).
4. Move `check_weather.py`, `debug_duckdb*.py` into `scripts/`.
5. Replace `print` logging with the `logging` module (medium term).
6. Parameterize cities list via config/env rather than hardcoding (medium term).
7. Add CI workflow running ruff + mypy + pytest on push (when repo is hosted).
8. Environment gotcha: the machine-global `CURL_CA_BUNDLE` points at a
   nonexistent PostgreSQL cert path (`C:\Program Files\PostgreSQL\18\ssl\...`),
   which breaks every pip install with a TLS CA-bundle error until it is
   cleared for the session (`$env:CURL_CA_BUNDLE = $null`).

---

*Last updated: 2026-08-21*
