from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    countDistinct,
    current_timestamp,
    dayofweek,
    lit,
    row_number,
)
from pyspark.sql.window import Window

try:
    from jobs.common.spark_runtime import validate_local_spark_runtime
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from jobs.common.spark_runtime import validate_local_spark_runtime


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run batch metrics for curated GH Archive parquet.")
    parser.add_argument("--input", required=True, help="Input directory that contains curated parquet data.")
    parser.add_argument("--output", required=True, help="Output directory for parquet data.")
    parser.add_argument("--top-n", type=int, default=10, help="Top N users/repos per day.")
    return parser


def build_spark() -> SparkSession:
    validate_local_spark_runtime()
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
        .select(
            "event_date",
            "event_hour",
            "event_type",
            "actor_category",
            "language_guess",
            "actor_login",
            "repo_name",
        )
        .filter(col("event_date").isNotNull() & col("event_type").isNotNull())
        .withColumn("actor_login", coalesce(col("actor_login"), lit("unknown")))
        .withColumn("repo_name", coalesce(col("repo_name"), lit("unknown/unknown")))
        .withColumn("language_guess", coalesce(col("language_guess"), lit("unknown")))
        .withColumn("actor_category", coalesce(col("actor_category"), lit("unknown")))
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

    language_day_trend_df = events_df.groupBy("metric_date", "language_guess").agg(
        count("*").alias("event_count"),
        countDistinct("repo_name").alias("unique_repos"),
        countDistinct("actor_login").alias("unique_actors"),
    )

    top_users_base_df = events_df.groupBy("metric_date", "actor_login").agg(
        count("*").alias("event_count"),
        countDistinct("repo_name").alias("unique_repos"),
    )
    top_users_window = Window.partitionBy("metric_date").orderBy(
        col("event_count").desc(), col("actor_login")
    )
    top_users_day_df = (
        top_users_base_df.withColumn("rank", row_number().over(top_users_window))
        .filter(col("rank") <= lit(args.top_n))
    )

    top_repos_base_df = events_df.groupBy("metric_date", "repo_name").agg(
        count("*").alias("event_count"),
        countDistinct("actor_login").alias("unique_actors"),
    )
    top_repos_window = Window.partitionBy("metric_date").orderBy(
        col("event_count").desc(), col("repo_name")
    )
    top_repos_day_df = (
        top_repos_base_df.withColumn("rank", row_number().over(top_repos_window))
        .filter(col("rank") <= lit(args.top_n))
    )

    event_type_day_df = events_df.groupBy("metric_date", "event_type").agg(
        count("*").alias("event_count"),
        countDistinct("actor_login").alias("unique_actors"),
        countDistinct("repo_name").alias("unique_repos"),
    )

    summary_json_df = events_df.agg(
        count("*").alias("total_events"),
        countDistinct("metric_date").alias("total_days"),
        countDistinct("repo_name").alias("total_repos"),
        countDistinct("actor_login").alias("total_actors"),
    ).withColumn("generated_at", current_timestamp())

    output_paths = {
        "daily_metrics": daily_metrics_df,
        "activity_patterns": activity_patterns_df,
        "language_day_trend": language_day_trend_df,
        "top_users_day": top_users_day_df,
        "top_repos_day": top_repos_day_df,
        "event_type_day": event_type_day_df,
    }

    for dataset_name, dataset_df in output_paths.items():
        dataset_df.write.mode("overwrite").parquet(f"{args.output}/{dataset_name}")

    summary_json_output = f"{args.output}/summary_json"
    summary_json_df.coalesce(1).write.mode("overwrite").json(summary_json_output)

    for dataset_name in output_paths:
        print(f"{dataset_name} saved to {args.output}/{dataset_name}")
    print(f"summary json saved to {summary_json_output}")
    spark.stop()


if __name__ == "__main__":
    main()
