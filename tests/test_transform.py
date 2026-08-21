import json
from pathlib import Path

import pytest

import src.transform_weather as transform

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "raw"

# city_id -> expected serialized validation_errors in quarantine
EXPECTED_QUARANTINE_ERRORS = {
    None: "NULL_CITY_ID",
    9000002: "NULL_READING_TIMESTAMP",
    9000003: "TEMP_OUT_OF_BOUNDS",
    9000004: "HUMIDITY_OUT_OF_BOUNDS",
    9000005: "LATITUDE_OUT_OF_BOUNDS",
    9000006: "LONGITUDE_OUT_OF_BOUNDS",
}


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"))


def _filler(index: int) -> dict:
    # Unique natural keys keep fillers from being collapsed by dropDuplicates
    payload = _fixture("valid_london")
    payload["id"] = 10000 + index
    payload["dt"] = 1755774000 + index * 60
    return payload


def _write_batch(raw_dir: Path, payloads: list[dict]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    for index, payload in enumerate(payloads):
        target = raw_dir / f"record_{index:03d}.json"
        target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_output(spark, output_dir: Path):
    return spark.read.parquet(output_dir.as_posix())


def _patch_paths(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Absolute paths are required because the Spark JVM does not follow os.chdir
    monkeypatch.setattr(
        transform, "RAW_PATH", (tmp_path / "data" / "raw" / "weather").as_posix()
    )
    monkeypatch.setattr(
        transform,
        "PROCESSED_PATH",
        (tmp_path / "data" / "processed" / "weather").as_posix(),
    )
    monkeypatch.setattr(
        transform,
        "QUARANTINE_PATH",
        (tmp_path / "data" / "quarantine" / "weather").as_posix(),
    )


def test_dq_rules_fire_quarantine_and_clean_output(tmp_path, monkeypatch) -> None:
    _patch_paths(tmp_path, monkeypatch)

    payloads = [_filler(i) for i in range(24)]
    payloads += [
        _fixture("valid_london"),
        _fixture("dup_reading"),  # collapses onto london via natural-key dedup
        _fixture("boundary_valid"),
        _fixture("invalid_city_id"),
        _fixture("invalid_timestamp"),
        _fixture("invalid_temp"),
        _fixture("invalid_humidity"),
        _fixture("invalid_lat"),
        _fixture("invalid_lon"),
    ]
    _write_batch(tmp_path / "data" / "raw" / "weather", payloads)

    # 32 rows total, 6 invalid => 18.75% error rate, below the 20% breaker
    transform.transform_and_validate_weather()

    spark = transform.build_spark_session()
    clean_df = _read_output(spark, tmp_path / "data" / "processed" / "weather")
    quarantine_rows = _read_output(
        spark, tmp_path / "data" / "quarantine" / "weather"
    ).collect()

    assert clean_df.count() == 26
    assert {"validation_errors", "is_valid"}.isdisjoint(clean_df.columns)

    errors_by_city = {
        row["city_id"]: row["validation_errors"] for row in quarantine_rows
    }
    assert errors_by_city == EXPECTED_QUARANTINE_ERRORS

    # Duplicate raw file removed by (city_id, reading_timestamp) dedup
    assert clean_df.filter("city_id = 2643743").count() == 1
    # Boundary values sit exactly on inclusive limits and must stay valid
    assert clean_df.filter("city_id = 9000007").count() == 1


def test_error_rate_circuit_breaker_trips(tmp_path, monkeypatch) -> None:
    _patch_paths(tmp_path, monkeypatch)

    payloads = [
        _fixture("valid_london"),
        _filler(0),
        _filler(1),
        _fixture("invalid_temp"),
        _fixture("invalid_humidity"),
    ]
    _write_batch(tmp_path / "data" / "raw" / "weather", payloads)

    with pytest.raises(ValueError, match="Circuit Breaker"):
        transform.transform_and_validate_weather()


def test_zero_record_circuit_breaker_stops_pipeline(tmp_path, monkeypatch) -> None:
    _patch_paths(tmp_path, monkeypatch)
    (tmp_path / "data" / "raw" / "weather").mkdir(parents=True)

    with pytest.raises(SystemExit) as excinfo:
        transform.transform_and_validate_weather()

    assert excinfo.value.code == 1


def test_no_quarantine_written_when_all_records_valid(tmp_path, monkeypatch) -> None:
    _patch_paths(tmp_path, monkeypatch)

    payloads = [
        _fixture("valid_london"),
        _fixture("boundary_valid"),
        _filler(0),
        _filler(1),
        _filler(2),
    ]
    _write_batch(tmp_path / "data" / "raw" / "weather", payloads)

    transform.transform_and_validate_weather()

    assert (tmp_path / "data" / "processed" / "weather").exists()
    assert not (tmp_path / "data" / "quarantine" / "weather").exists()


class TestClearStaleTempDirs:
    def test_removes_stale_spark_temp_dir(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(transform, "PROCESSED_PATH", str(tmp_path / "processed"))
        stale = tmp_path / "processed" / "_temporary"
        stale.mkdir(parents=True)
        (stale / "junk.txt").write_text("stale", encoding="utf-8")

        transform.clear_stale_temp_dirs()

        assert not stale.exists()

    def test_noop_when_temp_dir_absent(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setattr(transform, "PROCESSED_PATH", str(tmp_path / "missing"))

        transform.clear_stale_temp_dirs()

    def test_raises_actionable_error_when_locked(
        self, tmp_path, monkeypatch
    ) -> None:
        monkeypatch.setattr(transform, "PROCESSED_PATH", str(tmp_path / "processed"))
        stale = tmp_path / "processed" / "_temporary"
        stale.mkdir(parents=True)

        def refuse(path, *args, **kwargs):
            raise PermissionError(13, "Access is denied", str(path))

        monkeypatch.setattr(transform.shutil, "rmtree", refuse)

        with pytest.raises(RuntimeError, match="reboot or delete it"):
            transform.clear_stale_temp_dirs()
