from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    dayofmonth,
    expr,
    hour,
    input_file_name,
    lit,
    lower,
    lpad,
    minute,
    month,
    regexp_extract,
    to_date,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Clean GH Archive raw JSON into a curated parquet dataset."
    )
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="One or more input JSON files or directories containing raw JSON lines.",
    )
    parser.add_argument("--output", required=True, help="Output directory for curated parquet.")
    parser.add_argument(
        "--report-path",
        default="",
        help="Optional path for a single-row parquet/json cleaning report.",
    )
    return parser


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("github-stream-batch-curation")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )


def build_raw_schema() -> StructType:
    return StructType(
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
            StructField(
                "org",
                StructType([StructField("login", StringType(), True)]),
                True,
            ),
            StructField(
                "payload",
                StructType(
                    [
                        StructField("action", StringType(), True),
                        StructField("size", LongType(), True),
                    ]
                ),
                True,
            ),
            StructField("public", BooleanType(), True),
        ]
    )


def read_raw_events(spark: SparkSession, input_paths: list[str]) -> DataFrame:
    return spark.read.schema(build_raw_schema()).json(input_paths)


def curate_events(raw_df: DataFrame) -> DataFrame:
    event_ts = to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ssX")

    curated_df = (
        raw_df.filter(col("id").isNotNull() & col("type").isNotNull() & col("created_at").isNotNull())
        .withColumn("event_timestamp", event_ts)
        .filter(col("event_timestamp").isNotNull())
        .dropDuplicates(["id"])
        .withColumn("event_id", col("id"))
        .withColumn("event_type", col("type"))
        .withColumn("actor_login", expr("coalesce(actor.login, 'unknown')"))
        .withColumn(
            "actor_category",
            when(lower(col("actor_login")).contains("bot"), lit("bot")).otherwise(lit("human")),
        )
        .withColumn("repo_id", expr("coalesce(repo.id, 0L)"))
        .withColumn("repo_name", expr("coalesce(repo.name, 'unknown/unknown')"))
        .withColumn("org_login", expr("coalesce(org.login, 'unknown')"))
        .withColumn("payload_action", expr("coalesce(payload.action, 'unknown')"))
        .withColumn("payload_size", expr("coalesce(payload.size, 0L)"))
        .withColumn("public", expr("coalesce(public, true)"))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("event_minute", minute(col("event_timestamp")))
        .withColumn("source_file", regexp_extract(input_file_name(), r"[^/\\\\]+$", 0))
        .withColumn("ingest_date", expr("current_date()"))
        .withColumn(
            "language_guess",
            when(lower(col("repo_name")).contains("spark"), lit("Scala"))
            .when(lower(col("repo_name")).contains("scala"), lit("Scala"))
            .when(lower(col("repo_name")).contains("python"), lit("Python"))
            .when(lower(col("repo_name")).contains("pandas"), lit("Python"))
            .when(lower(col("repo_name")).contains("react"), lit("JavaScript"))
            .when(lower(col("repo_name")).contains("js"), lit("JavaScript"))
            .when(lower(col("repo_name")).contains("java"), lit("Java"))
            .when(lower(col("repo_name")).contains("spring"), lit("Java"))
            .when(lower(col("repo_name")).contains("golang"), lit("Go"))
            .when(lower(col("repo_name")).contains("go"), lit("Go"))
            .otherwise(lit("unknown"))
        )
        .withColumn("is_push_event", col("event_type") == lit("PushEvent"))
        .withColumn("is_watch_event", col("event_type") == lit("WatchEvent"))
        .withColumn("is_fork_event", col("event_type") == lit("ForkEvent"))
        .withColumn("is_issue_event", col("event_type") == lit("IssuesEvent"))
        .withColumn("is_pr_event", col("event_type") == lit("PullRequestEvent"))
        .withColumn("event_year", year(col("event_timestamp")))
        .withColumn("event_month", lpad(month(col("event_timestamp")).cast("string"), 2, "0"))
        .withColumn("event_day", lpad(dayofmonth(col("event_timestamp")).cast("string"), 2, "0"))
    )

    return curated_df.select(
        "event_id",
        "event_type",
        "created_at",
        "event_timestamp",
        "event_date",
        "event_hour",
        "event_minute",
        "event_year",
        "event_month",
        "event_day",
        "actor_login",
        "actor_category",
        "repo_id",
        "repo_name",
        "org_login",
        "public",
        "payload_action",
        "payload_size",
        "language_guess",
        "source_file",
        "ingest_date",
        "is_push_event",
        "is_watch_event",
        "is_fork_event",
        "is_issue_event",
        "is_pr_event",
    )


def write_curated_data(curated_df: DataFrame, output_dir: str) -> None:
    output_path = Path(output_dir)
    if output_path.exists():
        shutil.rmtree(output_path, ignore_errors=True)

    (
        curated_df.write.mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .parquet(str(output_path))
    )


def write_report(spark: SparkSession, raw_df: DataFrame, curated_df: DataFrame, report_path: str) -> None:
    if not report_path:
        return

    raw_count = raw_df.count()
    curated_count = curated_df.count()
    dropped_count = raw_count - curated_count

    report_df = spark.createDataFrame(
        [
            {
                "raw_count": raw_count,
                "curated_count": curated_count,
                "dropped_count": dropped_count,
            }
        ]
    )

    report_output = Path(report_path)
    if report_output.exists():
        shutil.rmtree(report_output, ignore_errors=True)

    if report_output.suffix.lower() == ".json":
        report_df.coalesce(1).write.mode("overwrite").json(str(report_output.with_suffix("")))
    else:
        report_df.coalesce(1).write.mode("overwrite").parquet(str(report_output))


def main() -> None:
    args = build_parser().parse_args()
    spark = build_spark()

    raw_df = read_raw_events(spark, args.input)
    curated_df = curate_events(raw_df)
    write_curated_data(curated_df, args.output)
    write_report(spark, raw_df, curated_df, args.report_path)

    print(f"curated dataset saved to {args.output}")
    print(f"curated rows: {curated_df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
