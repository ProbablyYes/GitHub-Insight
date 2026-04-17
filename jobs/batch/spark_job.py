from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    dayofweek,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run batch metrics for curated GH Archive parquet.")
    parser.add_argument("--input", required=True, help="Input directory that contains curated parquet data.")
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

    events_df = (
        spark.read.parquet(args.input)
        .withColumnRenamed("event_date", "metric_date")
        .withColumnRenamed("event_hour", "hour_of_day")
        .withColumn("day_of_week", dayofweek(col("metric_date")))
    )

    daily_metrics_df = (
        events_df.groupBy(
            "metric_date", "repo_name", "event_type", "actor_category", "language_guess"
        )
        .agg(
            count("*").alias("event_count"),
            countDistinct("actor_login").alias("unique_actors"),
            avg("hour_of_day").alias("avg_hour"),
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
