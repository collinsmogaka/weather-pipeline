import json
import re
from datetime import datetime, timezone

import pytest
import requests

import src.extract_weather as extract


class TestApiKeyGuard:
    def test_missing_api_key_raises_value_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(extract, "API_KEY", None)
        with pytest.raises(ValueError, match="OPENWEATHER_API_KEY"):
            extract.run_extraction()

    def test_placeholder_api_key_raises_value_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(extract, "API_KEY", "your_actual_api_key_here")
        with pytest.raises(ValueError, match="OPENWEATHER_API_KEY"):
            extract.run_extraction()


class TestRawFileNaming:
    def test_save_raw_json_uses_date_partition_and_city_slug(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        payload = {"main": {"temp": 21.3}}

        extract.save_raw_json(payload, "New York")

        now = datetime.now(timezone.utc)
        expected_dir = (
            tmp_path
            / "data"
            / "raw"
            / "weather"
            / f"{now:%Y}"
            / f"{now:%m}"
            / f"{now:%d}"
        )
        files = list(expected_dir.glob("new_york_*.json"))
        assert len(files) == 1
        assert re.fullmatch(r"new_york_\d{8}_\d{6}\.json", files[0].name) is not None
        assert json.loads(files[0].read_text(encoding="utf-8")) == payload


class TestBatchResilience:
    def test_one_city_failure_does_not_abort_batch(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(extract, "API_KEY", "test-key")
        monkeypatch.setattr(extract, "CITIES", ["Failing City", "Working City"])

        def fake_fetch(city: str, session: object = None) -> dict:
            if city == "Failing City":
                raise requests.RequestException("connection refused")
            return {"city": city}

        monkeypatch.setattr(extract, "fetch_weather_data", fake_fetch)

        extract.run_extraction()

        saved_files = list((tmp_path / "data" / "raw" / "weather").rglob("*.json"))
        assert len(saved_files) == 1
        assert json.loads(saved_files[0].read_text(encoding="utf-8")) == {
            "city": "Working City"
        }

    def test_all_cities_failing_raises_runtime_error(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(extract, "API_KEY", "test-key")
        monkeypatch.setattr(extract, "CITIES", ["City A", "City B"])

        def fake_fetch(city: str, session: object = None) -> dict:
            raise requests.ConnectionError(f"{city} unreachable")

        monkeypatch.setattr(extract, "fetch_weather_data", fake_fetch)

        with pytest.raises(RuntimeError, match="All 2 city fetches failed"):
            extract.run_extraction()


class FakeResponse:
    def __init__(self, payload: dict | None = None):
        self.payload = payload or {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class TestFetchHardening:
    def test_fetch_sets_explicit_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured_kwargs: dict = {}

        def fake_get(url: str, params: dict, **kwargs):
            captured_kwargs.update(kwargs)
            return FakeResponse({"ok": True})

        monkeypatch.setattr(extract.requests, "get", fake_get)

        extract.fetch_weather_data("Warsaw")

        assert (
            captured_kwargs["timeout"] == extract.REQUEST_TIMEOUT_SECONDS
            and extract.REQUEST_TIMEOUT_SECONDS > 0
        )

    def test_session_mounts_retry_adapter(self) -> None:
        session = extract.build_http_session()

        adapter = session.get_adapter("https://api.openweathermap.org")
        assert adapter.max_retries.total == extract.MAX_ATTEMPTS_PER_CITY - 1
        assert adapter.max_retries.backoff_factor == extract.RETRY_BACKOFF_FACTOR
