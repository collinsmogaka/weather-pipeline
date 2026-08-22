import duckdb
import pytest

import src.warehouse_weather as warehouse

SRC_VIEW_SQL = """
CREATE OR REPLACE TEMP VIEW src_weather AS
SELECT * FROM (VALUES
    {rows}
) AS t(
    city_id, city_name, country, latitude, longitude, reading_timestamp,
    temp_celsius, feels_like_celsius, humidity_pct, pressure_hpa,
    wind_speed_m_s, weather_condition, weather_description, processed_at
)
"""


def _row(
    city_id: int,
    name: str,
    country: str,
    lat: float,
    lon: float,
    ts: str,
    temp: float = 20.0,
) -> str:
    return (
        f"({city_id}, '{name}', '{country}', {lat}, {lon}, "
        f"TIMESTAMP '{ts}', {temp}, {temp - 1.0}, 65, 1013, 3.5, "
        f"'Clear', 'clear sky', TIMESTAMP '2026-08-21 13:00:00')"
    )


def _build_star_schema(con: duckdb.DuckDBPyConnection, row_sqls: list[str]) -> None:
    con.execute(SRC_VIEW_SQL.format(rows=",\n    ".join(row_sqls)))
    warehouse.create_dimension_table(con)
    warehouse.create_fact_table(con)
    warehouse.create_analytical_view(con)


class TestDimDedup:
    def test_dim_cities_deduplicates_to_unique_city_ids(self) -> None:
        rows = [
            _row(101, "LondonOld", "GB", 51.5, -0.12, "2026-08-20 10:00:00"),
            _row(101, "London", "GB", 51.51, -0.11, "2026-08-21 12:00:00"),
            _row(102, "Tokyo", "JP", 35.68, 139.69, "2026-08-21 09:00:00"),
        ]
        con = duckdb.connect(":memory:")
        try:
            _build_star_schema(con, rows)

            duplicates = con.execute(
                "SELECT COUNT(*) FROM ("
                "  SELECT city_id FROM dim_cities"
                "  GROUP BY city_id HAVING COUNT(*) > 1"
                ")"
            ).fetchone()[0]
            assert duplicates == 0

            names = dict(
                con.execute("SELECT city_id, city_name FROM dim_cities").fetchall()
            )
            # The latest reading per city_id must win
            assert names == {101: "London", 102: "Tokyo"}
        finally:
            con.close()


class TestFactDimJoin:
    def test_fact_joins_dim_without_fan_out(self) -> None:
        rows = [
            _row(101, "London", "GB", 51.5, -0.12, "2026-08-20 10:00:00"),
            _row(101, "London", "GB", 51.5, -0.12, "2026-08-21 12:00:00"),
            _row(102, "Tokyo", "JP", 35.68, 139.69, "2026-08-21 09:00:00"),
        ]
        con = duckdb.connect(":memory:")
        try:
            _build_star_schema(con, rows)

            fact_count = con.execute(
                "SELECT COUNT(*) FROM fact_weather_readings"
            ).fetchone()[0]
            joined_count = con.execute(
                "SELECT COUNT(*) FROM fact_weather_readings f "
                "JOIN dim_cities d ON f.city_id = d.city_id"
            ).fetchone()[0]

            assert fact_count == 3
            assert joined_count == fact_count
            assert joined_count > 0

            summary = dict(
                con.execute(
                    "SELECT city_name, total_readings FROM vw_city_weather_metrics"
                ).fetchall()
            )
            assert summary == {"London": 2, "Tokyo": 1}
        finally:
            con.close()


class TestLoadGuards:
    def test_get_parquet_files_raises_when_processed_dir_missing(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError):
            warehouse.get_parquet_files()

    def test_get_parquet_files_raises_when_no_parquet_files(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / warehouse.PROCESSED_PATH).mkdir(parents=True)
        with pytest.raises(FileNotFoundError):
            warehouse.get_parquet_files()

    def test_print_quick_scan_rejects_empty_source(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        empty_parquet = tmp_path / "empty.parquet"
        con = duckdb.connect(":memory:")
        try:
            con.execute(
                f"""
                COPY (
                    SELECT
                        CAST(NULL AS BIGINT) AS city_id,
                        CAST(NULL AS VARCHAR) AS city_name,
                        CAST(NULL AS VARCHAR) AS country,
                        CAST(NULL AS DOUBLE) AS latitude,
                        CAST(NULL AS DOUBLE) AS longitude,
                        CAST(NULL AS TIMESTAMP) AS reading_timestamp,
                        CAST(NULL AS DOUBLE) AS temp_celsius,
                        CAST(NULL AS DOUBLE) AS feels_like_celsius,
                        CAST(NULL AS BIGINT) AS humidity_pct,
                        CAST(NULL AS BIGINT) AS pressure_hpa,
                        CAST(NULL AS DOUBLE) AS wind_speed_m_s,
                        CAST(NULL AS VARCHAR) AS weather_condition,
                        CAST(NULL AS VARCHAR) AS weather_description,
                        CAST(NULL AS TIMESTAMP) AS processed_at
                    WHERE false
                ) TO '{empty_parquet.as_posix()}' (FORMAT PARQUET)
                """
            )
            warehouse.create_parquet_view(con, [empty_parquet.as_posix()])
            with pytest.raises(RuntimeError, match="zero records"):
                warehouse.print_quick_scan(con)
        finally:
            con.close()
