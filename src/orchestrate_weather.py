import os
import sys

from prefect import flow, task

# Add src to path so task imports work cleanly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config_weather import get_schedule_interval_seconds
from src.extract_weather import run_extraction
from src.transform_weather import transform_and_validate_weather
from src.warehouse_weather import build_data_warehouse

SCHEDULE_INTERVAL_SECONDS = get_schedule_interval_seconds()


@task(name="Extract Weather Data", retries=2, retry_delay_seconds=10)
def task_extract():
    """Fetches raw weather API payloads and saves partitioned JSON."""
    print("--- Executing Task: Extract Weather Data ---")
    run_extraction()


@task(name="Transform & Validate Weather Data", retries=1)
def task_transform():
    """Runs PySpark cleanup, validation checks, and Parquet writes."""
    print("--- Executing Task: Transform & Validate Weather Data ---")
    transform_and_validate_weather()


@task(name="Build Warehouse & Data Mart", retries=2, retry_delay_seconds=15)
def task_warehouse():
    """Loads Parquet into DuckDB DWH and builds analytical views."""
    print("--- Executing Task: Build Warehouse & Data Mart ---")
    build_data_warehouse()


@flow(name="End-To-End Weather Data Pipeline")
def weather_pipeline_flow():
    """Main orchestration flow mapping task execution order."""
    # Enforce strict sequential dependencies
    extract_result = task_extract()
    transform_result = task_transform(wait_for=[extract_result])
    task_warehouse(wait_for=[transform_result])


def main() -> None:
    if "--once" in sys.argv:
        weather_pipeline_flow()
        return

    # One immediate run so fresh data exists before the schedule takes over
    print(
        f"--- Running pipeline once, then serving every {SCHEDULE_INTERVAL_SECONDS} seconds ---"
    )
    weather_pipeline_flow()
    weather_pipeline_flow.serve(
        name="weather-pipeline-every-5-min",
        interval=SCHEDULE_INTERVAL_SECONDS,
    )


if __name__ == "__main__":
    main()
