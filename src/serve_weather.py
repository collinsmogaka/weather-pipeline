import sys
from pathlib import Path

import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

# Add project root to path so direct execution works (same as orchestrate_weather.py)
sys.path.append(
    str(Path(__file__).resolve().parents[1])
)

from src.warehouse_weather import DATABASE_PATH  # noqa: E402

HOST = "127.0.0.1"
PORT = 8000
DEFAULT_READINGS_LIMIT = 25

app = FastAPI(
    title="Weather Pipeline Dashboard",
    description="Serves analytical results from the DuckDB weather warehouse.",
)


def create_readonly_connection() -> duckdb.DuckDBPyConnection:
    database_path = Path(DATABASE_PATH).resolve()

    if not database_path.exists():
        raise FileNotFoundError(
            f"Warehouse database not found: {database_path}. "
            "Run 'python src/orchestrate_weather.py' first."
        )

    # Short-lived read-only sessions so the API never blocks a pipeline rebuild
    return duckdb.connect(str(database_path), read_only=True)


def _query_dicts(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: dict | None = None,
) -> list[dict]:
    result = con.execute(sql, params or {})
    columns = [description[0] for description in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def fetch_city_metrics() -> list[dict]:
    con = create_readonly_connection()
    try:
        return _query_dicts(
            con,
            """
            SELECT *
            FROM vw_city_weather_metrics
            ORDER BY city_name
            """,
        )
    finally:
        con.close()


def fetch_recent_readings(
    limit: int = DEFAULT_READINGS_LIMIT,
    city_id: int | None = None,
) -> list[dict]:
    con = create_readonly_connection()
    try:
        return _query_dicts(
            con,
            """
            SELECT
                f.reading_timestamp,
                d.city_name,
                d.country,
                f.temp_celsius,
                f.feels_like_celsius,
                f.humidity_pct,
                f.pressure_hpa,
                f.wind_speed_m_s,
                f.weather_condition,
                f.weather_description
            FROM fact_weather_readings AS f
            INNER JOIN dim_cities AS d
                ON f.city_id = d.city_id
            WHERE ($city_id IS NULL OR f.city_id = $city_id)
            ORDER BY f.reading_timestamp DESC
            LIMIT $limit
            """,
            {"city_id": city_id, "limit": limit},
        )
    finally:
        con.close()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/city-metrics")
def api_city_metrics() -> dict:
    try:
        cities = fetch_city_metrics()
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error

    return {"cities": cities}


@app.get("/api/readings")
def api_readings(
    limit: int = Query(default=DEFAULT_READINGS_LIMIT, ge=1, le=500),
    city_id: int | None = None,
) -> dict:
    try:
        readings = fetch_recent_readings(limit=limit, city_id=city_id)
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error

    return {"readings": readings}


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Weather Pipeline Dashboard</title>
<style>
  body { font-family: Segoe UI, Arial, sans-serif; margin: 2rem;
         background: #f4f6f8; color: #222; }
  h1 { color: #1a5276; }
  .meta { color: #666; font-size: 0.9rem; }
  table { border-collapse: collapse; width: 100%; margin: 1rem 0 2rem;
          background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,.15); }
  th, td { padding: .5rem .75rem; border-bottom: 1px solid #ddd;
           text-align: left; white-space: nowrap; }
  th { background: #1a5276; color: #fff; }
  tr:hover td { background: #eaf2f8; }
  .num { text-align: right; }
</style>
</head>
<body>
<h1>Weather Pipeline Dashboard</h1>
<p class="meta">Auto-refreshes every 60 s &mdash; last updated:
<span id="updated">-</span></p>

<h2>City Metrics (vw_city_weather_metrics)</h2>
<table id="metrics">
<thead><tr><th>City</th><th>Country</th><th>Readings</th>
<th>Avg Temp (&deg;C)</th><th>Max Temp (&deg;C)</th><th>Min Temp (&deg;C)</th>
<th>Avg Humidity (%)</th><th>Avg Wind (m/s)</th></tr></thead>
<tbody></tbody>
</table>

<h2>Recent Readings (fact_weather_readings)</h2>
<table id="readings">
<thead><tr><th>Timestamp</th><th>City</th><th>Temp (&deg;C)</th>
<th>Feels Like (&deg;C)</th><th>Humidity (%)</th><th>Pressure (hPa)</th>
<th>Wind (m/s)</th><th>Condition</th></tr></thead>
<tbody></tbody>
</table>

<script>
function fmt(value, digits) {
  return value == null ? "-" : Number(value).toFixed(digits);
}

async function load() {
  const metrics = await (await fetch("/api/city-metrics")).json();
  document.querySelector("#metrics tbody").innerHTML =
    metrics.cities.map(c => `
      <tr>
        <td>${c.city_name}</td><td>${c.country}</td>
        <td class="num">${c.total_readings}</td>
        <td class="num">${fmt(c.avg_temp_c, 1)}</td>
        <td class="num">${fmt(c.max_temp_c, 1)}</td>
        <td class="num">${fmt(c.min_temp_c, 1)}</td>
        <td class="num">${fmt(c.avg_humidity_pct, 0)}</td>
        <td class="num">${fmt(c.avg_wind_speed, 1)}</td>
      </tr>`).join("");

  const readings = await (await fetch("/api/readings?limit=25")).json();
  document.querySelector("#readings tbody").innerHTML =
    readings.readings.map(r => `
      <tr>
        <td>${r.reading_timestamp.replace("T", " ").slice(0, 19)}</td>
        <td>${r.city_name} (${r.country})</td>
        <td class="num">${fmt(r.temp_celsius, 1)}</td>
        <td class="num">${fmt(r.feels_like_celsius, 1)}</td>
        <td class="num">${r.humidity_pct ?? "-"}</td>
        <td class="num">${r.pressure_hpa ?? "-"}</td>
        <td class="num">${fmt(r.wind_speed_m_s, 1)}</td>
        <td>${r.weather_description ?? r.weather_condition ?? "-"}</td>
      </tr>`).join("") ||
    "<tr><td colspan='8'>No readings found.</td></tr>";

  document.getElementById("updated").textContent =
    new Date().toLocaleTimeString();
}

load();
setInterval(load, 60000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return DASHBOARD_HTML


def main() -> None:
    print(f"--- Starting dashboard on http://{HOST}:{PORT} ---")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
