import decimal
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse

# Add project root to path so direct execution works (same as orchestrate_weather.py)
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.config_weather import (  # noqa: E402
    OPENWEATHER_BASE_URL,
    PLACEHOLDER_API_KEY,
)
from src.warehouse_weather import DATABASE_PATH  # noqa: E402

load_dotenv()

HOST = "127.0.0.1"
PORT = 8000
DEFAULT_READINGS_LIMIT = 25
SEARCH_HISTORY_LIMIT = 10
DEFAULT_TIMESERIES_LIMIT = 2000
MAX_TIMESERIES_LIMIT = 10000
REQUEST_TIMEOUT_SECONDS = 10
CHART_JS_PATH = Path(__file__).resolve().parent / "static" / "chart.umd.min.js"

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

    # Pydantic v2 serializes Decimal as JSON strings; normalize to float so
    # numeric fields stay numbers in API responses.
    return [
        {
            column: float(value) if isinstance(value, decimal.Decimal) else value
            for column, value in zip(columns, row)
        }
        for row in result.fetchall()
    ]


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


def _resolve_api_key() -> str:
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key or api_key == PLACEHOLDER_API_KEY:
        raise RuntimeError(
            "Missing valid OPENWEATHER_API_KEY in .env file; "
            "live city search is disabled."
        )
    return api_key


def fetch_live_weather(city: str) -> dict:
    params = {
        "q": city,
        "appid": _resolve_api_key(),
        "units": "metric",
    }
    response = requests.get(
        OPENWEATHER_BASE_URL,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def map_live_payload(payload: dict) -> dict:
    condition = payload["weather"][0]
    main = payload["main"]

    return {
        "city": {
            "name": payload["name"],
            "country": payload.get("sys", {}).get("country"),
            "latitude": payload["coord"]["lat"],
            "longitude": payload["coord"]["lon"],
        },
        "current": {
            "observed_at_utc": datetime.fromtimestamp(
                payload["dt"], tz=timezone.utc
            ).isoformat(),
            "temp_celsius": main["temp"],
            "feels_like_celsius": main["feels_like"],
            "humidity_pct": main["humidity"],
            "pressure_hpa": main["pressure"],
            "wind_speed_m_s": payload.get("wind", {}).get("speed"),
            "condition": condition.get("main"),
            "description": condition.get("description"),
        },
    }


def fetch_city_history(
    city_name: str,
    limit: int = SEARCH_HISTORY_LIMIT,
) -> list[dict]:
    try:
        con = create_readonly_connection()
    except FileNotFoundError:
        # Live search must still work when the warehouse was never built
        return []

    try:
        return _query_dicts(
            con,
            """
            SELECT
                f.reading_timestamp,
                f.temp_celsius,
                f.humidity_pct,
                f.weather_description
            FROM fact_weather_readings AS f
            INNER JOIN dim_cities AS d
                ON f.city_id = d.city_id
            WHERE lower(d.city_name) = lower($city)
            ORDER BY f.reading_timestamp DESC
            LIMIT $limit
            """,
            {"city": city_name, "limit": limit},
        )
    finally:
        con.close()


def fetch_city_timeseries(limit: int) -> dict:
    """Per-city chronological series for the dashboard charts."""
    con = create_readonly_connection()
    try:
        rows = _query_dicts(
            con,
            """
            SELECT
                f.city_id,
                d.city_name,
                f.reading_timestamp,
                f.temp_celsius,
                f.feels_like_celsius,
                f.humidity_pct
            FROM fact_weather_readings AS f
            INNER JOIN dim_cities AS d
                ON f.city_id = d.city_id
            ORDER BY f.reading_timestamp ASC, d.city_name ASC
            LIMIT $limit
            """,
            {"limit": limit},
        )
    finally:
        con.close()

    series_by_city: dict[int, dict] = {}
    for row in rows:
        city_id = row["city_id"]
        if city_id not in series_by_city:
            series_by_city[city_id] = {
                "city_id": city_id,
                "city_name": row["city_name"],
                "points": [],
            }
        series_by_city[city_id]["points"].append(
            {
                "ts": row["reading_timestamp"],
                "temp_celsius": row["temp_celsius"],
                "feels_like_celsius": row["feels_like_celsius"],
                "humidity_pct": row["humidity_pct"],
            }
        )

    return {"series": list(series_by_city.values())}


def fetch_warehouse_status() -> dict:
    """Freshness metadata; degrades to exists=false when no warehouse yet."""
    database_path = Path(DATABASE_PATH).resolve()
    if not database_path.exists():
        return {"warehouse_exists": False, "latest_reading_ts": None}

    con = create_readonly_connection()
    try:
        result = con.execute(
            """
            SELECT MAX(reading_timestamp), MAX(processed_at)
            FROM fact_weather_readings
            """
        ).fetchone()
    finally:
        con.close()

    latest_reading, latest_processed = result if result else (None, None)

    return {
        "warehouse_exists": True,
        "latest_reading_ts": (
            latest_reading.isoformat() if latest_reading is not None else None
        ),
        "latest_processed_at": (
            latest_processed.isoformat() if latest_processed is not None else None
        ),
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/static/chart.umd.min.js", include_in_schema=False)
def chart_js() -> FileResponse:
    # Vendored so the dashboard works offline; falls back to CDN in the page
    if not CHART_JS_PATH.exists():
        raise HTTPException(status_code=404, detail="Vendored Chart.js not found.")
    return FileResponse(CHART_JS_PATH, media_type="application/javascript")


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


@app.get("/api/search")
def api_search(
    city: str = Query(min_length=1, max_length=100),
) -> dict:
    city = city.strip()
    if not city:
        raise HTTPException(status_code=422, detail="City name must not be blank.")
    try:
        payload = fetch_live_weather(city)
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    except requests.HTTPError as error:
        status = error.response.status_code if error.response is not None else 0
        if status == 404:
            raise HTTPException(
                status_code=404,
                detail=f"City '{city}' not found. Check the spelling and retry.",
            ) from error
        raise HTTPException(
            status_code=502,
            detail=f"OpenWeather API request failed (status {status}).",
        ) from error
    except requests.RequestException as error:
        raise HTTPException(
            status_code=502,
            detail="Could not reach the OpenWeather API. Try again later.",
        ) from error

    return {
        "source": "openweather_live",
        **map_live_payload(payload),
        "warehouse_history": fetch_city_history(payload.get("name", city)),
    }


@app.get("/api/timeseries")
def api_timeseries(
    limit: int = Query(
        default=DEFAULT_TIMESERIES_LIMIT,
        ge=1,
        le=MAX_TIMESERIES_LIMIT,
    ),
) -> dict:
    try:
        return fetch_city_timeseries(limit=limit)
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.get("/api/status")
def api_status() -> dict:
    return fetch_warehouse_status()


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
  .search { margin: 1rem 0; display: flex; gap: .5rem; }
  .search input { flex: 1; padding: .5rem .75rem; font-size: 1rem;
                  border: 1px solid #bbb; border-radius: 4px; }
  .search button { padding: .5rem 1.25rem; font-size: 1rem; cursor: pointer;
                   background: #1a5276; color: #fff; border: none;
                   border-radius: 4px; }
  .search button:hover { background: #154360; }
  #search-result { background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,.15);
                   padding: 1rem 1.25rem; margin-bottom: 2rem;
                   border-left: 4px solid #1a5276; }
  #search-result.error { border-left-color: #c0392b; color: #c0392b; }
  #search-result h3 { margin: 0 0 .5rem; }
  .hidden { display: none; }
  .charts { display: grid; grid-template-columns: 2fr 1fr; gap: 1.5rem;
            margin-bottom: 2rem; }
  .chart-card { background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,.15);
                padding: 1rem 1.25rem; }
  .chart-card h2 { margin-top: 0; font-size: 1.1rem; color: #1a5276; }
  .empty-note { color: #666; padding: 1rem 0; margin: 0; }
  @media (max-width: 900px) { .charts { grid-template-columns: 1fr; } }
</style>
<script src="/static/chart.umd.min.js"></script>
<script>
  // CDN fallback in case the vendored asset is missing
  window.Chart || document.write(
    '<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"><\\/script>'
  );
</script>
</head>
<body>
<h1>Weather Pipeline Dashboard</h1>
<p class="meta">Auto-refreshes every 60 s &mdash; page refreshed:
<span id="updated">-</span> &middot; pipeline data through: <span id="freshness">loading...</span></p>

<h2>Search Any City (live)</h2>
<div class="search">
  <input id="search-input" type="text" maxlength="100"
         placeholder="Type any city, e.g. Nairobi, Paris, Gdansk...">
  <button id="search-btn">Search</button>
</div>
<div id="search-result" class="hidden"></div>

<div class="charts">
  <div class="chart-card">
    <h2>Temperature Trend by City</h2>
    <p class="empty-note hidden" id="trend-empty">No readings yet - the trend
    chart appears after the first pipeline run.</p>
    <canvas id="trend-chart"></canvas>
  </div>
  <div class="chart-card">
    <h2>Average Temp &amp; Humidity</h2>
    <canvas id="avg-chart"></canvas>
  </div>
</div>

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

function tsLabel(iso) {
  return String(iso).replace("T", " ").slice(0, 16);
}

const PALETTE = ["#1a5276", "#e74c3c", "#27ae60", "#f39c12", "#8e44ad",
                 "#16a085", "#d35400", "#2980b9", "#c0392b", "#7f8c8d",
                 "#2c3e50", "#f1c40f"];
let trendChart = null;
let barChart = null;

async function loadStatus() {
  try {
    const status = await (await fetch("/api/status")).json();
    document.getElementById("freshness").textContent =
      status.warehouse_exists && status.latest_reading_ts
        ? tsLabel(status.latest_reading_ts) + " UTC"
        : "no warehouse yet - run orchestrate_weather.py";
  } catch (err) {
    document.getElementById("freshness").textContent = "status unavailable";
  }
}

async function loadTrend() {
  const canvas = document.getElementById("trend-chart");
  const emptyNote = document.getElementById("trend-empty");

  let series = [];
  try {
    const data = await (await fetch("/api/timeseries")).json();
    series = data.series || [];
  } catch (err) { /* handled by empty state below */ }

  if (!series.length) {
    if (trendChart) { trendChart.destroy(); trendChart = null; }
    emptyNote.classList.remove("hidden");
    canvas.classList.add("hidden");
    return;
  }
  emptyNote.classList.add("hidden");
  canvas.classList.remove("hidden");

  // Union of all timestamps across cities keeps gaps visible per line
  const labels = [...new Set(series.flatMap(s => s.points.map(p => p.ts)))].sort();

  series.sort((a, b) => a.city_name.localeCompare(b.city_name));
  const datasets = series.map((s, i) => ({
    label: s.city_name,
    data: labels.map(ts => {
      const point = s.points.find(pt => pt.ts === ts);
      return point ? point.temp_celsius : null;
    }),
    borderColor: PALETTE[i % PALETTE.length],
    backgroundColor: PALETTE[i % PALETTE.length],
    tension: 0.25,
    pointRadius: 2,
    borderWidth: 2,
    spanGaps: true,
  }));

  if (trendChart) trendChart.destroy();
  trendChart = new Chart(canvas, {
    type: "line",
    data: { labels: labels.map(tsLabel), datasets },
    options: {
      responsive: true,
      interaction: { mode: "nearest", axis: "x", intersect: false },
      scales: {
        y: { title: { display: true, text: "Temp (\u00b0C)" } }
      }
    }
  });
}

function loadAvgBars(cities) {
  const canvas = document.getElementById("avg-chart");
  if (!cities.length) {
    canvas.classList.add("hidden");
    return;
  }
  canvas.classList.remove("hidden");

  if (barChart) barChart.destroy();
  barChart = new Chart(canvas, {
    type: "bar",
    data: {
      labels: cities.map(c => c.city_name),
      datasets: [
        {
          label: "Avg Temp (\u00b0C)",
          data: cities.map(c => c.avg_temp_c),
          backgroundColor: "#e74c3c",
          yAxisID: "y"
        },
        {
          label: "Avg Humidity (%)",
          data: cities.map(c => c.avg_humidity_pct),
          backgroundColor: "#2980b9",
          yAxisID: "y1"
        }
      ]
    },
    options: {
      responsive: true,
      scales: {
        y: { position: "left",
             title: { display: true, text: "\u00b0C" } },
        y1: { position: "right", min: 0, max: 100,
              grid: { drawOnChartArea: false },
              title: { display: true, text: "%" } }
      }
    }
  });
}

async function load() {
  let metrics = { cities: [] };
  try {
    metrics = await (await fetch("/api/city-metrics")).json();
  } catch (err) { /* leave tables empty on transient lock */ }

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

  let readings = { readings: [] };
  try {
    readings = await (await fetch("/api/readings?limit=25")).json();
  } catch (err) { /* leave table empty on transient lock */ }

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

  loadStatus();
  loadTrend();
  loadAvgBars(metrics.cities);

  document.getElementById("updated").textContent =
    new Date().toLocaleTimeString();
}

async function searchCity() {
  const query = document.getElementById("search-input").value.trim();
  const box = document.getElementById("search-result");
  box.classList.remove("hidden", "error");

  if (!query) {
    box.classList.add("error");
    box.textContent = "Enter a city name to search.";
    return;
  }

  try {
    const response = await fetch("/api/search?city=" + encodeURIComponent(query));
    const body = await response.json();
    if (!response.ok) {
      throw new Error(body.detail || "Search failed.");
    }

    const c = body.city;
    const w = body.current;
    const history = body.warehouse_history || [];
    const historyRows = history.map(h => `
      <tr>
        <td>${h.reading_timestamp.replace("T", " ").slice(0, 19)}</td>
        <td class="num">${fmt(h.temp_celsius, 1)}</td>
        <td class="num">${h.humidity_pct ?? "-"}</td>
        <td>${h.weather_description ?? "-"}</td>
      </tr>`).join("");

    box.innerHTML = `
      <h3>${c.name}${c.country ? ", " + c.country : ""}
          <span class="meta">(${c.latitude.toFixed(2)}, ${c.longitude.toFixed(2)})</span></h3>
      <p><b>${w.temp_celsius}&deg;C</b> (feels like ${fmt(w.feels_like_celsius, 1)}&deg;C),
         humidity ${w.humidity_pct ?? "-"}%, pressure ${w.pressure_hpa ?? "-"} hPa,
         wind ${fmt(w.wind_speed_m_s, 1)} m/s &mdash; ${w.description ?? w.condition ?? "-"}
         <span class="meta">(observed ${String(w.observed_at_utc).replace("T", " ").slice(0, 19)} UTC)</span></p>
      ${history.length ? `
        <p class="meta">Pipeline history for this city (latest ${history.length}):</p>
        <table><thead><tr><th>Timestamp</th><th>Temp (&deg;C)</th>
        <th>Humidity (%)</th><th>Description</th></tr></thead>
        <tbody>${historyRows}</tbody></table>`
        : "<p class='meta'>No pipeline history for this city yet.</p>"}`;
  } catch (err) {
    box.classList.add("error");
    box.textContent = err.message;
  }
}

document.getElementById("search-btn").addEventListener("click", searchCity);
document.getElementById("search-input").addEventListener("keydown",
  e => { if (e.key === "Enter") searchCity(); });

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
