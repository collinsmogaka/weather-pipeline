from pathlib import Path

import duckdb


DATABASE_PATH = "data/weather_dwh.duckdb"
PROCESSED_PATH = Path("data/processed/weather")


def get_parquet_files() -> list[str]:
    project_root = Path.cwd()
    processed_dir = (project_root / PROCESSED_PATH).resolve()

    if not processed_dir.exists():
        raise FileNotFoundError(
            f"Processed directory does not exist: {processed_dir}"
        )

    parquet_files = sorted(
        path
        for path in processed_dir.rglob("*.parquet")
        if path.is_file()
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found under: {processed_dir}"
        )

    print("\n--- Parquet Files Found ---")
    for path in parquet_files:
        print(f"  {path}")

    return [
        path.as_posix()
        for path in parquet_files
    ]


def create_connection() -> duckdb.DuckDBPyConnection:
    database_path = Path(DATABASE_PATH).resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to DuckDB database at: {database_path}")

    return duckdb.connect(str(database_path))


def create_parquet_view(
    con: duckdb.DuckDBPyConnection,
    parquet_files: list[str],
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW src_weather AS
        SELECT *
        FROM read_parquet({parquet_files})
        """
    )


def print_quick_scan(con: duckdb.DuckDBPyConnection) -> int:
    print("\n--- Quick Scan of Processed Parquet Data ---")

    result = con.execute(
        """
        SELECT COUNT(*) AS total_clean_records
        FROM src_weather
        """
    ).fetchone()

    total_records = int(result[0])
    print(f"Total clean records: {total_records}")

    print("\n--- Source Schema ---")
    # Native DuckDB call (no pandas/numpy required)
    con.sql("DESCRIBE src_weather").show()

    print("\n--- Source Sample ---")
    con.sql("SELECT * FROM src_weather LIMIT 5").show()

    if total_records == 0:
        raise RuntimeError(
            "The processed Parquet files contain zero records. "
            "Check PySpark execution logs to see why records were filtered or quarantined."
        )

    return total_records


def create_dimension_table(
    con: duckdb.DuckDBPyConnection,
) -> None:
    print("\nCreating Dimension Table: dim_cities...")

    # Fixed: Deduplicates strictly on city_id to avoid fan-out errors in fact joins
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_cities AS
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY city_id
            ) AS city_key,
            city_id,
            city_name,
            country,
            latitude,
            longitude
        FROM (
            SELECT
                city_id,
                city_name,
                country,
                latitude,
                longitude,
                ROW_NUMBER() OVER (
                    PARTITION BY city_id 
                    ORDER BY reading_timestamp DESC
                ) AS rn
            FROM src_weather
        ) AS cities
        WHERE rn = 1
        """
    )


def create_fact_table(
    con: duckdb.DuckDBPyConnection,
) -> None:
    print("Creating Fact Table: fact_weather_readings...")

    con.execute(
        """
        CREATE OR REPLACE TABLE fact_weather_readings AS
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY reading_timestamp, city_id
            ) AS reading_key,
            city_id,
            reading_timestamp,
            temp_celsius,
            feels_like_celsius,
            humidity_pct,
            pressure_hpa,
            wind_speed_m_s,
            weather_condition,
            weather_description,
            processed_at
        FROM src_weather
        """
    )


def create_analytical_view(
    con: duckdb.DuckDBPyConnection,
) -> None:
    print(
        "Creating Analytical Data Mart View: "
        "vw_city_weather_metrics..."
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW vw_city_weather_metrics AS
        SELECT
            d.city_name,
            d.country,
            COUNT(f.reading_key) AS total_readings,
            AVG(f.temp_celsius) AS avg_temp_c,
            MAX(f.temp_celsius) AS max_temp_c,
            MIN(f.temp_celsius) AS min_temp_c,
            AVG(f.humidity_pct) AS avg_humidity_pct,
            AVG(f.wind_speed_m_s) AS avg_wind_speed
        FROM fact_weather_readings AS f
        INNER JOIN dim_cities AS d
            ON f.city_id = d.city_id
        GROUP BY
            d.city_name,
            d.country
        """
    )


def validate_warehouse(
    con: duckdb.DuckDBPyConnection,
) -> None:
    print("\n--- Analytical View: City Weather Summary ---")
    con.sql(
        """
        SELECT *
        FROM vw_city_weather_metrics
        ORDER BY city_name
        """
    ).show()

    print("\n--- Star Schema Validation (Fact JOIN Dim) ---")
    con.sql(
        """
        SELECT
            f.reading_timestamp,
            d.city_name,
            d.country,
            f.temp_celsius,
            f.weather_condition
        FROM fact_weather_readings AS f
        INNER JOIN dim_cities AS d
            ON f.city_id = d.city_id
        ORDER BY f.reading_timestamp, d.city_name
        LIMIT 10
        """
    ).show()

    print("\n--- Warehouse Row Counts ---")
    con.sql(
        """
        SELECT 'dim_cities' AS table_name, COUNT(*) AS row_count
        FROM dim_cities

        UNION ALL

        SELECT 'fact_weather_readings', COUNT(*)
        FROM fact_weather_readings
        """
    ).show()


def build_data_warehouse() -> None:
    """Loads processed Parquet into DuckDB and builds dim/fact/view."""
    parquet_files = get_parquet_files()
    con = create_connection()

    try:
        create_parquet_view(con, parquet_files)
        print_quick_scan(con)
        create_dimension_table(con)
        create_fact_table(con)
        create_analytical_view(con)
        validate_warehouse(con)

        print("\nDuckDB warehouse setup completed successfully.")

    finally:
        con.close()


def main() -> None:
    build_data_warehouse()


if __name__ == "__main__":
    main()