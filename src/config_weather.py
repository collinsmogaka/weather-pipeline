"""Central configuration sourced from environment variables with safe defaults."""

import os

from dotenv import load_dotenv

load_dotenv()

DEFAULT_CITIES = [
    "Warsaw",
    "Krakow",
    "Lodz",
    "Wroclaw",
    "Poznan",
    "Gdansk",
    "Szczecin",
    "Bydgoszcz",
    "Lublin",
    "Katowice",
]
DEFAULT_SCHEDULE_INTERVAL_SECONDS = 300
DEFAULT_API_TIMEOUT_SECONDS = 10
PLACEHOLDER_API_KEY = "your_actual_api_key_here"
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def parse_cities(raw: str | None) -> list[str]:
    """Parses a comma-separated WEATHER_CITIES value; empty/None -> default set."""
    if not raw:
        return list(DEFAULT_CITIES)

    cities = [city.strip() for city in raw.split(",") if city.strip()]
    return cities or list(DEFAULT_CITIES)


def get_cities() -> list[str]:
    return parse_cities(os.getenv("WEATHER_CITIES"))


def get_schedule_interval_seconds() -> int:
    raw = os.getenv("SCHEDULE_INTERVAL_SECONDS", str(DEFAULT_SCHEDULE_INTERVAL_SECONDS))
    interval = int(raw)

    if interval < 60:
        raise ValueError(
            f"SCHEDULE_INTERVAL_SECONDS must be >= 60 (got {interval}); "
            "OpenWeather free-tier rate limits make faster schedules unsafe."
        )
    return interval
