from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    dayofweek,
    hour,
    lit,
    lower,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run batch metrics for GH Archive JSON lines.")
    parser.add_argument("--input", required=True, help="Input directory that contains JSON lines.")
    parser.add_argument("--output", required=True, help="Output directory for parquet data.")
    return parser


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("github-stream-batch-analytics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    args = build_parser().parse_args()
    spark = build_spark()

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField(
                "actor",
                StructType([StructField("login", StringType(), True)]),
                True,
            ),
            StructField(
                "repo",
                StructType(
                    [
                        StructField("id", LongType(), True),
                        StructField("name", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("public", BooleanType(), True),
        ]
    )

    raw_df = spark.read.schema(schema).json(args.input)
    events_df = (
        raw_df.withColumn("created_ts", to_timestamp(col("created_at")))
        .withColumn("metric_date", to_date(col("created_ts")))
        .withColumn("hour_of_day", hour(col("created_ts")))
        .withColumn("day_of_week", dayofweek(col("created_ts")))
        .withColumn("actor_login", col("actor.login"))
        .withColumn("repo_name", col("repo.name"))
        .withColumn(
            "actor_category",
            when(lower(col("actor.login")).contains("bot"), lit("bot")).otherwise(lit("human")),
        )
        .fillna({"repo_name": "unknown/unknown", "actor_login": "unknown"})
    )

    daily_metrics_df = (
        events_df.groupBy("metric_date", "repo_name", "type", "actor_category")
        .agg(
            count("*").alias("event_count"),
            countDistinct("actor_login").alias("unique_actors"),
            avg("hour_of_day").alias("avg_hour"),
        )
        .withColumnRenamed("type", "event_type")
        .withColumn(
            "language_guess",
            when(lower(col("repo_name")).contains("spark"), lit("Scala"))
            .when(lower(col("repo_name")).contains("python"), lit("Python"))
            .when(lower(col("repo_name")).contains("react"), lit("JavaScript"))
            .when(lower(col("repo_name")).contains("java"), lit("Java"))
            .otherwise(lit("unknown")),
        )
    )

    activity_patterns_df = events_df.groupBy(
        "metric_date", "hour_of_day", "day_of_week", "actor_category"
    ).agg(
        count("*").alias("event_count"),
        countDistinct("repo_name").alias("unique_repos"),
    )

    daily_metrics_output = f"{args.output}/daily_metrics"
    activity_patterns_output = f"{args.output}/activity_patterns"
    daily_metrics_df.write.mode("overwrite").parquet(daily_metrics_output)
    activity_patterns_df.write.mode("overwrite").parquet(activity_patterns_output)

    print(f"daily metrics saved to {daily_metrics_output}")
    print(f"activity patterns saved to {activity_patterns_output}")
    spark.stop()


if __name__ == "__main__":
    main()
