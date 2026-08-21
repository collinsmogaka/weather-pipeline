import duckdb
import pytest
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
    def test_returns_one_row_per_city_sorted_by_name(
        self, client: TestClient
    ) -> None:
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


class TestDashboard:
    def test_root_serves_html_dashboard(self, client: TestClient) -> None:
        response = client.get("/")

        assert response.status_code == 200
        assert "Weather Pipeline Dashboard" in response.text


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
