import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add project root to path so direct execution works (same as orchestrate_weather.py)
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.config_weather import (  # noqa: E402
    DEFAULT_API_TIMEOUT_SECONDS,
    OPENWEATHER_BASE_URL,
    PLACEHOLDER_API_KEY,
    get_cities,
)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = OPENWEATHER_BASE_URL

CITIES = get_cities()
REQUEST_TIMEOUT_SECONDS = DEFAULT_API_TIMEOUT_SECONDS
MAX_ATTEMPTS_PER_CITY = 3
RETRY_BACKOFF_FACTOR = 2


def build_http_session() -> requests.Session:
    """Session with retry/backoff for transient API failures (429/5xx, timeouts)."""
    retry_policy = Retry(
        total=MAX_ATTEMPTS_PER_CITY - 1,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_policy))
    return session


def fetch_weather_data(city: str, session: requests.Session | None = None) -> dict:
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    http = session if session is not None else requests
    response = http.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def save_raw_json(data: dict, city: str):
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")

    # Mirroring cloud object storage date partitioning
    dir_path = os.path.join("data", "raw", "weather", date_path)
    os.makedirs(dir_path, exist_ok=True)

    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{city.lower().replace(' ', '_')}_{timestamp}.json"
    file_path = os.path.join(dir_path, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved: {file_path}")


def run_extraction():
    if not API_KEY or API_KEY == PLACEHOLDER_API_KEY:
        raise ValueError("Missing valid OPENWEATHER_API_KEY in .env file")

    succeeded = 0
    # Broad by design: any per-city failure (API, disk, parse) must be logged
    # and skipped so the rest of the batch still completes.
    with build_http_session() as session:
        for city in CITIES:
            try:
                data = fetch_weather_data(city, session)
                save_raw_json(data, city)
                succeeded += 1
            except Exception as e:  # noqa: BLE001
                print(f"Failed to fetch data for {city}: {e}")

    if succeeded == 0:
        # Silent total failure would let stale raw history keep feeding the
        # warehouse on scheduled runs, masking the outage.
        raise RuntimeError(
            f"All {len(CITIES)} city fetches failed; check API key/quota/network."
        )


if __name__ == "__main__":
    run_extraction()
