from datetime import datetime, timezone

import duckdb
import pytest
import requests
from fastapi.testclient import TestClient

import src.serve_weather as serve
import src.warehouse_weather as warehouse

SRC_VIEW_SQL = """
CREATE OR REPLACE TEMP VIEW src_weather AS
SELECT * FROM (VALUES
    (101, 'London', 'GB', 51.5, -0.12, TIMESTAMP '2026-08-20 10:00:00',
     18.0, 17.0, 65, 1013, 3.5, 'Clear', 'clear sky',
     TIMESTAMP '2026-08-21 13:00:00'),
    (101, 'London', 'GB', 51.51, -0.11, TIMESTAMP '2026-08-21 12:00:00',
     21.0, 20.0, 60, 1012, 4.0, 'Clear', 'clear sky',
     TIMESTAMP '2026-08-21 13:00:00'),
    (102, 'Tokyo', 'JP', 35.68, 139.69, TIMESTAMP '2026-08-21 09:00:00',
     30.0, 32.0, 70, 1008, 2.5, 'Clouds', 'few clouds',
     TIMESTAMP '2026-08-21 13:00:00')
) AS t(
    city_id, city_name, country, latitude, longitude, reading_timestamp,
    temp_celsius, feels_like_celsius, humidity_pct, pressure_hpa,
    wind_speed_m_s, weather_condition, weather_description, processed_at
)
"""


@pytest.fixture()
def client(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> TestClient:
    database_path = tmp_path / "weather_dwh.duckdb"
    monkeypatch.setattr(serve, "DATABASE_PATH", str(database_path))

    con = duckdb.connect(str(database_path))
    try:
        con.execute(SRC_VIEW_SQL)
        warehouse.create_dimension_table(con)
        warehouse.create_fact_table(con)
        warehouse.create_analytical_view(con)
    finally:
        con.close()

    return TestClient(serve.app)


class TestHealth:
    def test_health_returns_ok(self, client: TestClient) -> None:
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestCityMetrics:
    def test_returns_one_row_per_city_sorted_by_name(self, client: TestClient) -> None:
        response = client.get("/api/city-metrics")
        body = response.json()

        assert response.status_code == 200
        cities = body["cities"]
        assert [c["city_name"] for c in cities] == ["London", "Tokyo"]

        london = cities[0]
        assert london["total_readings"] == 2
        assert london["avg_temp_c"] == pytest.approx(19.5)


class TestReadings:
    def test_returns_readings_newest_first(self, client: TestClient) -> None:
        response = client.get("/api/readings")
        readings = response.json()["readings"]

        assert response.status_code == 200
        assert len(readings) == 3
        timestamps = [r["reading_timestamp"] for r in readings]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_city_filter_and_limit_are_applied(self, client: TestClient) -> None:
        response = client.get("/api/readings", params={"limit": 1, "city_id": 102})
        readings = response.json()["readings"]

        assert len(readings) == 1
        assert readings[0]["city_name"] == "Tokyo"

    def test_limit_below_one_is_rejected(self, client: TestClient) -> None:
        response = client.get("/api/readings", params={"limit": 0})

        assert response.status_code == 422


class TestTimeseries:
    def test_groups_points_per_city_chronologically(self, client: TestClient) -> None:
        response = client.get("/api/timeseries")
        body = response.json()

        assert response.status_code == 200
        series = {s["city_name"]: s for s in body["series"]}
        assert set(series) == {"London", "Tokyo"}

        london = series["London"]
        assert london["city_id"] == 101
        timestamps = [p["ts"] for p in london["points"]]
        assert timestamps == sorted(timestamps)
        assert len(timestamps) == 2
        assert london["points"][1]["temp_celsius"] == 21.0

        tokyo = series["Tokyo"]
        first_point = tokyo["points"][0]
        assert first_point["temp_celsius"] == 30.0
        assert first_point["humidity_pct"] == 70
        assert first_point["feels_like_celsius"] == 32.0

    def test_limit_caps_total_points_returned(self, client: TestClient) -> None:
        response = client.get("/api/timeseries", params={"limit": 2})
        body = response.json()

        total_points = sum(len(s["points"]) for s in body["series"])
        assert total_points == 2

    def test_limit_below_one_is_rejected(self, client: TestClient) -> None:
        response = client.get("/api/timeseries", params={"limit": 0})

        assert response.status_code == 422


class TestWarehouseStatus:
    def test_reports_freshness_when_warehouse_exists(self, client: TestClient) -> None:
        response = client.get("/api/status")
        body = response.json()

        assert response.status_code == 200
        assert body["warehouse_exists"] is True
        assert body["latest_reading_ts"] == "2026-08-21T12:00:00"
        assert body["latest_processed_at"] == "2026-08-21T13:00:00"

    def test_reports_missing_warehouse_without_erroring(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            serve, "DATABASE_PATH", str(tmp_path / "does_not_exist.duckdb")
        )
        client = TestClient(serve.app)

        response = client.get("/api/status")

        assert response.status_code == 200
        body = response.json()
        assert body["warehouse_exists"] is False
        assert body["latest_reading_ts"] is None


class TestDashboard:
    def test_root_serves_html_dashboard(self, client: TestClient) -> None:
        response = client.get("/")

        assert response.status_code == 200
        assert "Weather Pipeline Dashboard" in response.text

    def test_vendored_chart_js_is_served(self, client: TestClient) -> None:
        response = client.get("/static/chart.umd.min.js")

        assert response.status_code == 200
        assert "Chart.js" in response.text
        # Dashboard must reference the local asset, not the CDN directly
        assert 'src="/static/chart.umd.min.js"' in client.get("/").text


class TestMissingWarehouseGuard:
    def test_api_returns_503_when_warehouse_missing(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            serve, "DATABASE_PATH", str(tmp_path / "does_not_exist.duckdb")
        )
        client = TestClient(serve.app)

        response = client.get("/api/city-metrics")

        assert response.status_code == 503
        assert "not found" in response.json()["detail"]


class FakeResponse:
    def __init__(self, payload: dict | None = None, status_code: int = 200):
        self.payload = payload or {}
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self) -> dict:
        return self.payload


LIVE_PAYLOAD = {
    "name": "London",
    "coord": {"lat": 51.51, "lon": -0.13},
    "sys": {"country": "GB"},
    "dt": 1755772800,
    "weather": [{"main": "Clouds", "description": "scattered clouds"}],
    "main": {
        "temp": 21.5,
        "feels_like": 22.0,
        "humidity": 58,
        "pressure": 1014,
    },
    "wind": {"speed": 3.1},
}


class TestSearch:
    @pytest.fixture()
    def mock_openweather(self, monkeypatch: pytest.MonkeyPatch):
        def _install(response: FakeResponse) -> None:
            monkeypatch.setattr(serve.requests, "get", lambda *a, **k: response)
            monkeypatch.setenv("OPENWEATHER_API_KEY", "test-key-123")

        return _install

    def test_returns_live_conditions_and_warehouse_history(
        self, client: TestClient, mock_openweather
    ) -> None:
        mock_openweather(FakeResponse(LIVE_PAYLOAD))

        response = client.get("/api/search", params={"city": " London "})
        body = response.json()

        assert response.status_code == 200
        assert body["source"] == "openweather_live"
        assert body["city"] == {
            "name": "London",
            "country": "GB",
            "latitude": 51.51,
            "longitude": -0.13,
        }
        current = body["current"]
        assert current["temp_celsius"] == 21.5
        assert current["feels_like_celsius"] == 22.0
        assert current["humidity_pct"] == 58
        assert current["pressure_hpa"] == 1014
        assert current["wind_speed_m_s"] == 3.1
        assert current["condition"] == "Clouds"
        expected_observed = datetime.fromtimestamp(
            LIVE_PAYLOAD["dt"], tz=timezone.utc
        ).isoformat()
        assert current["observed_at_utc"] == expected_observed

        # 'London' exists in the fixture warehouse, so history must be attached
        history = body["warehouse_history"]
        assert len(history) == 2
        timestamps = [h["reading_timestamp"] for h in history]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_unknown_city_returns_404(
        self, client: TestClient, mock_openweather
    ) -> None:
        mock_openweather(FakeResponse({}, status_code=404))

        response = client.get("/api/search", params={"city": "Nowhereville"})

        assert response.status_code == 404
        assert "not found" in response.json()["detail"]

    def test_upstream_error_returns_502(
        self, client: TestClient, mock_openweather
    ) -> None:
        mock_openweather(FakeResponse({}, status_code=500))

        response = client.get("/api/search", params={"city": "London"})

        assert response.status_code == 502

    def test_network_failure_returns_502(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OPENWEATHER_API_KEY", "test-key-123")

        def _raise(*args, **kwargs):
            raise requests.ConnectionError("connection refused")

        monkeypatch.setattr(serve.requests, "get", _raise)

        response = client.get("/api/search", params={"city": "London"})

        assert response.status_code == 502

    def test_missing_api_key_returns_503(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # dotenv ran at import time, so the real key must be actively removed
        monkeypatch.delenv("OPENWEATHER_API_KEY", raising=False)

        response = client.get("/api/search", params={"city": "London"})

        assert response.status_code == 503
        assert "OPENWEATHER_API_KEY" in response.json()["detail"]

    def test_blank_city_is_rejected(self, client: TestClient) -> None:
        response = client.get("/api/search", params={"city": "   "})

        assert response.status_code == 422
        assert "blank" in response.json()["detail"]
