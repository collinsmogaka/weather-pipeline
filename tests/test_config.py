import pytest

import src.config_weather as config


class TestParseCities:
    def test_none_and_empty_fall_back_to_default_set(self) -> None:
        assert config.parse_cities(None) == config.DEFAULT_CITIES
        assert config.parse_cities("") == config.DEFAULT_CITIES
        assert config.parse_cities("   ") == config.DEFAULT_CITIES

    def test_comma_separated_values_are_trimmed_and_filtered(self) -> None:
        cities = config.parse_cities(" Paris , London,,Tokyo ")

        assert cities == ["Paris", "London", "Tokyo"]


class TestScheduleInterval:
    def test_default_interval_is_five_minutes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SCHEDULE_INTERVAL_SECONDS", raising=False)

        assert config.get_schedule_interval_seconds() == 300

    def test_env_override_is_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SCHEDULE_INTERVAL_SECONDS", "600")

        assert config.get_schedule_interval_seconds() == 600

    def test_intervals_below_one_minute_are_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SCHEDULE_INTERVAL_SECONDS", "30")

        with pytest.raises(ValueError, match="rate limits"):
            config.get_schedule_interval_seconds()
