import json
import re
from datetime import datetime, timezone

import pytest
import requests

import src.extract_weather as extract


class TestApiKeyGuard:
    def test_missing_api_key_raises_value_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(extract, "API_KEY", None)
        with pytest.raises(ValueError, match="OPENWEATHER_API_KEY"):
            extract.run_extraction()

    def test_placeholder_api_key_raises_value_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

        def fake_fetch(city: str) -> dict:
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
