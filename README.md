# Weather Pipeline

A production-style weather data platform that continuously collects live
weather observations, validates them with PySpark, loads them into a DuckDB
star-schema warehouse, and serves interactive dashboards through FastAPI —
all orchestrated by Prefect on an unattended 5-minute schedule.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.x-E25A1C?logo=apache-spark&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-1.5-FFF100)
![Prefect](https://img.shields.io/badge/Prefect-3.8-024DFD?logo=prefect&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.141-009688?logo=fastapi&logoColor=white)
![Tests](https://img.shields.io/badge/tests-43%20passing-brightgreen)

## Highlights

- **Medallion architecture** — bronze (raw JSON) → silver (validated Parquet) → gold (DuckDB warehouse), mirroring cloud lakehouse patterns.
- **Data-quality engineering** — six row-level validation rules, quarantine routing for rejected records, and two circuit breakers (zero-record and >20 % error-rate) that stop bad batches before they reach analytics.
- **Unattended operations** — Prefect flow runs extract → transform → warehouse every 5 minutes with per-stage retries and strict sequential dependencies.
- **Interactive serving layer** — FastAPI dashboard with per-city temperature trends, avg temp/humidity charts (Chart.js), warehouse freshness indicator, and live city search against OpenWeather.
- **Tested end-to-end** — 43 tests covering extraction resilience, every DQ rule, circuit breakers, warehouse integrity, and all API error paths; no network or real API key required.
- **Offline-capable dashboard** — Chart.js is vendored locally with CDN fallback.

## Architecture

```
OpenWeather API ──► Raw JSON (bronze) ──► Parquet (silver) ──► DuckDB DWH (gold)
   extract_weather.py    transform_weather.py      warehouse_weather.py

        orchestrate_weather.py  (Prefect: sequential tasks, retries,
                                 served every 5 minutes)
                                        │
                                 serve_weather.py  (FastAPI dashboard:
                                                   charts + live search)
```

| Stage | Script | Input | Output |
|-------|--------|-------|--------|
| Extract | `src/extract_weather.py` | OpenWeather API | `data/raw/weather/YYYY/MM/DD/*.json` |
| Transform | `src/transform_weather.py` | Raw JSON | `data/processed/weather/*.parquet` (+ quarantine) |
| Warehouse | `src/warehouse_weather.py` | Processed Parquet | `data/weather_dwh.duckdb` |
| Orchestration | `src/orchestrate_weather.py` | — | Runs the stages above on a 5-min schedule |
| Serving | `src/serve_weather.py` | DuckDB (+ live API) | Dashboard at `http://127.0.0.1:8000` |

**Warehouse model:** `dim_cities` (deduplicated per city), 
`fact_weather_readings` (one row per reading), and the analytical mart 
`vw_city_weather_metrics`.

## Data Quality

Every record is validated before it can reach Parquet:

| Rule | Condition |
|------|-----------|
| `NULL_CITY_ID` | city_id is null |
| `NULL_READING_TIMESTAMP` | reading_timestamp is null |
| `TEMP_OUT_OF_BOUNDS` | temp outside −90…60 °C |
| `HUMIDITY_OUT_OF_BOUNDS` | humidity outside 0…100 % |
| `LATITUDE_OUT_OF_BOUNDS` | latitude outside −90…90 |
| `LONGITUDE_OUT_OF_BOUNDS` | longitude outside −180…180 |

Invalid records are quarantined with their failure reasons — never dropped,
never loaded. A batch aborts entirely when empty or when >20 % of records fail.

## Quickstart

Requires Python 3.11+ and an [OpenWeather](https://openweathermap.org/api) API key.

```powershell
# 1. Environment
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Configure (.env in project root)
#    OPENWEATHER_API_KEY=<your key>

# 3. Start the pipeline (runs once immediately, then every 5 minutes)
python src\orchestrate_weather.py

# 4. In a second terminal, start the dashboard
python src\serve_weather.py
# -> open http://127.0.0.1:8000
```

Single pipeline run without the scheduler: `python src\orchestrate_weather.py --once`

### Configuration (optional)

| Variable | Default | Purpose |
|----------|---------|---------|
| `OPENWEATHER_API_KEY` | — (required) | OpenWeather API access |
| `WEATHER_CITIES` | 10 Polish cities | Comma-separated tracked cities |
| `SCHEDULE_INTERVAL_SECONDS` | `300` | Pipeline interval (min 60) |

## API

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness probe |
| `GET /` | HTML dashboard (charts + tables, auto-refresh) |
| `GET /api/city-metrics` | Per-city aggregates from the mart view |
| `GET /api/readings?limit=&city_id=` | Newest readings, filterable |
| `GET /api/timeseries` | Per-city chronological series for charts |
| `GET /api/status` | Warehouse freshness metadata |
| `GET /api/search?city=` | Live weather for any city + warehouse history |

Error handling is explicit: missing warehouse → 503, unknown city → 404,
upstream provider failure → 502, invalid input → 422.

## Screenshots

<!-- Drop captures here: dashboard overview, trend chart, quarantine output -->

## Testing & Quality Gates

```powershell
ruff format src tests
ruff check src tests
mypy          # config in pyproject.toml
pytest        # 43 tests, ~1 min, no network required
```

## Documentation

- [`PROJECT_GUIDELINES.md`](PROJECT_GUIDELINES.md) — engineering conventions, quality gates, roadmap
- [`REQUIREMENTS.md`](REQUIREMENTS.md) — functional & non-functional requirements (v1.1)

## Roadmap

- Structured `logging` to replace print output
- CI workflow (ruff + mypy + pytest on push)
- Containerized deployment with a cloud data warehouse target
