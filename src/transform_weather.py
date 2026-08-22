import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    array_compact,
    col,
    concat_ws,
    current_timestamp,
    from_unixtime,
    lit,
    size,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

RAW_PATH = "data/raw/weather"
PROCESSED_PATH = "data/processed/weather"
QUARANTINE_PATH = "data/quarantine/weather"


def clear_stale_temp_dirs() -> None:
    """Removes leftover Spark _temporary dirs that block mode('overwrite') on Windows."""
    stale_temp = Path(PROCESSED_PATH) / "_temporary"

    if not stale_temp.exists():
        return

    try:
        shutil.rmtree(stale_temp)
        print(f"Removed stale Spark temp dir: {stale_temp}")
    except OSError as error:
        raise RuntimeError(
            f"Cannot clear locked Spark temp dir: {stale_temp} ({error}). "
            "A previous run crashed while holding handles; reboot or delete it "
            "with admin rights, then retry."
        ) from error


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("WeatherDataTransformation")
        .master("local[*]")
        .getOrCreate()
    )


def get_raw_weather_schema() -> StructType:
    return StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("dt", LongType(), True),
            StructField(
                "coord",
                StructType(
                    [
                        StructField("lon", DoubleType(), True),
                        StructField("lat", DoubleType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "sys", StructType([StructField("country", StringType(), True)]), True
            ),
            StructField(
                "main",
                StructType(
                    [
                        StructField("temp", DoubleType(), True),
                        StructField("feels_like", DoubleType(), True),
                        StructField("pressure", LongType(), True),
                        StructField("humidity", LongType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "wind",
                StructType(
                    [
                        StructField("speed", DoubleType(), True),
                        StructField("deg", LongType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "weather",
                ArrayType(
                    StructType(
                        [
                            StructField("main", StringType(), True),
                            StructField("description", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )


def transform_and_validate_weather():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading from: {RAW_PATH}")

    raw_df = (
        spark.read.schema(get_raw_weather_schema())
        .option("recursiveFileLookup", "true")
        .option("multiLine", "true")
        .json(RAW_PATH)
    )

    print(f"DEBUG -> Raw records read: {raw_df.count()}")
    print("DEBUG -> Sample of raw data:")
    raw_df.show(3, truncate=False)

    # 1. Base Flattening Transformation
    transformed_df = raw_df.select(
        col("id").alias("city_id"),
        col("name").alias("city_name"),
        col("sys.country").alias("country"),
        col("coord.lat").alias("latitude"),
        col("coord.lon").alias("longitude"),
        from_unixtime(col("dt")).cast("timestamp").alias("reading_timestamp"),
        col("main.temp").alias("temp_celsius"),
        col("main.feels_like").alias("feels_like_celsius"),
        col("main.humidity").alias("humidity_pct"),
        col("main.pressure").alias("pressure_hpa"),
        col("wind.speed").alias("wind_speed_m_s"),
        col("weather")[0]["main"].alias("weather_condition"),
        col("weather")[0]["description"].alias("weather_description"),
    ).dropDuplicates(["city_id", "reading_timestamp"])

    print(f"DEBUG -> Records after transformation: {transformed_df.count()}")
    print("DEBUG -> Sample of transformed data:")
    transformed_df.show(3, truncate=False)

    # 2. Add Row-Level Data Quality Rules
    error_conditions = array(
        when(col("city_id").isNull(), lit("NULL_CITY_ID")),
        when(col("reading_timestamp").isNull(), lit("NULL_READING_TIMESTAMP")),
        when(
            (col("temp_celsius") < -90) | (col("temp_celsius") > 60),
            lit("TEMP_OUT_OF_BOUNDS"),
        ),
        when(
            (col("humidity_pct") < 0) | (col("humidity_pct") > 100),
            lit("HUMIDITY_OUT_OF_BOUNDS"),
        ),
        when(
            (col("latitude") < -90) | (col("latitude") > 90),
            lit("LATITUDE_OUT_OF_BOUNDS"),
        ),
        when(
            (col("longitude") < -180) | (col("longitude") > 180),
            lit("LONGITUDE_OUT_OF_BOUNDS"),
        ),
    )

    # array_compact strips the NULL slots left by when()-without-otherwise;
    # array_remove(error_conditions, NULL) returns NULL itself on Spark 4.x,
    # which silently disabled every DQ rule (regression: tests/test_transform.py)
    validated_df = (
        transformed_df.withColumn("validation_errors", array_compact(error_conditions))
        .withColumn("is_valid", size(col("validation_errors")) == 0)
        .withColumn("processed_at", current_timestamp())
    )

    # 3. Circuit Breaker Metric Check
    total_records = validated_df.count()
    invalid_records = validated_df.filter(~col("is_valid")).count()

    print(f"DEBUG -> Total validated records: {total_records}")
    print(f"DEBUG -> Invalid records: {invalid_records}")

    if total_records == 0:
        print("CRITICAL: Zero records after transformation. Stopping pipeline.")
        sys.exit(1)

    error_rate = invalid_records / total_records
    print(
        f"Data Quality Audit: {total_records} total | {invalid_records} invalid | Error Rate: {error_rate:.2%}"
    )

    # Circuit breaker: Fail job if more than 20% of batch is corrupt
    if error_rate > 0.20:
        raise ValueError(
            f"Circuit Breaker Triggered! Error rate ({error_rate:.2%}) exceeds threshold (20%)."
        )

    # 4. Quarantine Routing
    valid_df = validated_df.filter(col("is_valid")).drop(
        "validation_errors", "is_valid"
    )

    # Fixed: String conversion for array errors to ensure safe Parquet output serialization
    quarantine_df = (
        validated_df.filter(~col("is_valid"))
        .withColumn("validation_errors", concat_ws(", ", col("validation_errors")))
        .drop("is_valid")
    )

    # 5. Sink Writes
    clear_stale_temp_dirs()
    print(f"Writing clean records to: {PROCESSED_PATH}")
    (
        valid_df.coalesce(
            1
        )  # Fixed: Condenses output partitions into clean, single Parquet files
        .write.mode("overwrite")
        # .partitionBy("country")
        .parquet(PROCESSED_PATH)
    )

    if invalid_records > 0:
        print(
            f"Routing {invalid_records} corrupt record(s) to quarantine: {QUARANTINE_PATH}"
        )
        (
            quarantine_df.coalesce(
                1
            )  # Fixed: Prevents generation of empty/fragmented files in append mode
            .write.mode("append")
            .parquet(QUARANTINE_PATH)
        )

    print("PySpark pipeline with Data Quality checks executed successfully.")


if __name__ == "__main__":
    transform_and_validate_weather()
