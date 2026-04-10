#!/usr/bin/env python3
"""
01_bronze_ingest.py — Bronze ingest pipeline (local PySpark, Option A)

Reads Kaggle e-commerce events CSV(s) from data/raw/,
adds ingestion metadata (ingest_ts, source_file),
derives event_date from event_time,
writes parquet to data/bronze/events/ partitioned by event_date.

Dataset (example):
- ecommerce-events-history-in-cosmetics-shop (Kaggle)
  Download via curl (see curl file) and unzip into data/raw/
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_BRONZE_DIR = Path("data/bronze/events")


def build_spark(app_name: str = "bronze_ingest") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Reasonable local defaults; adjust if needed:
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )


def find_csv_inputs(raw_dir: Path, glob_pattern: str) -> List[str]:
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir.resolve()}")

    files = sorted(raw_dir.rglob(glob_pattern))
    if not files:
        raise FileNotFoundError(
            f"No input files found in {raw_dir.resolve()} matching: {glob_pattern}"
        )
    return [str(p) for p in files]


def read_events_csv(spark: SparkSession, paths: List[str]) -> DataFrame:
    """
    Reads CSV with header. Keeps everything as string initially (safe for bronze),
    but parses event_time into a timestamp column for event_date partitioning.
    """
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "false")
        .csv(paths)
    )
    return df


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """
    Adds:
      - ingest_ts: current timestamp at ingest time
      - source_file: file path for each row
      - event_ts: parsed timestamp from event_time (best-effort)
      - event_date: date derived from event_ts (for partitioning)
    """
    # Best-effort timestamp parsing:
    # Kaggle cosmetics shop dataset uses "YYYY-MM-DD HH:MM:SS UTC" in many exports.
    # We'll try a few patterns; keep event_ts null if parsing fails.
    event_time_col = F.col("event_time")

    ts1 = F.to_timestamp(event_time_col, "yyyy-MM-dd HH:mm:ss 'UTC'")
    ts2 = F.to_timestamp(event_time_col, "yyyy-MM-dd HH:mm:ss")
    ts3 = F.to_timestamp(event_time_col)  # fallback

    event_ts = F.coalesce(ts1, ts2, ts3)

    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("event_ts", event_ts)
        .withColumn("event_date", F.to_date(F.col("event_ts")))
    )


def write_bronze(df: DataFrame, bronze_dir: Path, mode: str) -> None:
    bronze_dir.parent.mkdir(parents=True, exist_ok=True)
    (
        df.write.mode(mode)
        .partitionBy("event_date")
        .parquet(str(bronze_dir))
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze ingest pipeline (local PySpark).")
    p.add_argument(
        "--raw-dir",
        default=str(DEFAULT_RAW_DIR),
        help="Directory containing raw Kaggle CSV files (default: data/raw).",
    )
    p.add_argument(
        "--glob",
        default="*.csv",
        help="Glob pattern to find input files under raw-dir (default: *.csv).",
    )
    p.add_argument(
        "--bronze-dir",
        default=str(DEFAULT_BRONZE_DIR),
        help="Output directory for bronze parquet (default: data/bronze/events).",
    )
    p.add_argument(
        "--mode",
        choices=["append", "overwrite"],
        default="append",
        help="Write mode for parquet output (default: append).",
    )
    p.add_argument(
        "--show",
        type=int,
        default=5,
        help="Show N rows and schema for quick verification (default: 5).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    raw_dir = Path(args.raw_dir)
    bronze_dir = Path(args.bronze_dir)

    spark = build_spark()

    try:
        input_paths = find_csv_inputs(raw_dir, args.glob)
        df_raw = read_events_csv(spark, input_paths)
        df_bronze = add_ingestion_metadata(df_raw)

        if args.show and args.show > 0:
            df_bronze.printSchema()
            df_bronze.show(args.show, truncate=False)

        write_bronze(df_bronze, bronze_dir, args.mode)

        # Basic counts per partition date (small sanity check; can be expensive on huge data)
        # Comment out if dataset is large and this is slow.
        df_bronze.groupBy("event_date").count().orderBy("event_date").show(20, truncate=False)

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())