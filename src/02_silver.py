#!/usr/bin/env python3
"""
02_silver_clean.py — Silver clean + standardize + dedupe

Reads bronze parquet from data/bronze/events/,
standardizes types (timestamps, numerics),
cleans strings (trim, null handling),
generates or uses event_id, deduplicates,
and writes to data/silver/events/.

Output:
- stable schema
- event_id
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_BRONZE_DIR = Path("/home/lea/portfolio-projects/sflake-dbricks/data/bronze")
DEFAULT_SILVER_DIR = Path("/home/lea/portfolio-projects/sflake-dbricks/data/silver/events")



def build_spark(app_name: str = "silver_clean") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Tune memory for a 32GB machine: keep some headroom for OS/Python
        .config("spark.driver.memory", "24g")
        .config("spark.executor.memory", "24g")
        .config("spark.driver.maxResultSize", "6g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        # Reduce per-task shuffle pressure by increasing partitions
        .config("spark.sql.shuffle.partitions", "400")
        # Set local dir for shuffle spill files if needed
        .config("spark.local.dir", "/tmp/spark")
        .getOrCreate()
    )


def read_bronze_parquet(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Bronze directory not found: {path.resolve()}")
    return spark.read.parquet(str(path))


def clean_strings(df: DataFrame) -> DataFrame:
    """
    Trim all string columns and convert blank strings to null.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            c = F.col(field.name)
            df = df.withColumn(
                field.name,
                F.when(F.trim(c) == "", F.lit(None)).otherwise(F.trim(c)),
            )
    return df


def standardize_types(df: DataFrame) -> DataFrame:
    """
    Standardize timestamps, dates, and numeric columns.
    Adjust this if your bronze schema uses different column names.
    """
    # event timestamp
    if "event_ts" in df.columns:
        df = df.withColumn("event_ts", F.col("event_ts").cast(T.TimestampType()))
    elif "event_time" in df.columns:
        df = df.withColumn("event_ts", F.col("event_time").cast(T.TimestampType()))
    elif "event_timestamp" in df.columns:
        df = df.withColumn("event_ts", F.col("event_timestamp").cast(T.TimestampType()))

    # event date
    if "event_date" in df.columns:
        df = df.withColumn("event_date", F.col("event_date").cast(T.DateType()))
    elif "event_ts" in df.columns:
        df = df.withColumn("event_date", F.to_date("event_ts"))

    # common numeric fields
    numeric_candidates = {
        "price": T.DoubleType(),
        "amount": T.DoubleType(),
        "quantity": T.IntegerType(),
        "user_id": T.LongType(),
        "product_id": T.LongType(),
        "category_id": T.LongType(),
    }

    for col_name, dtype in numeric_candidates.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

    return df


def add_event_id(df: DataFrame) -> DataFrame:
    """
    Reuse event_id if present.
    Otherwise generate a deterministic SHA-256 hash from core event fields:
    user_id + event_ts + event_type + product_id
    """
    if "event_id" in df.columns:
        return df

    def safe_str(col_name: str) -> F.Column:
        if col_name in df.columns:
            return F.coalesce(F.col(col_name).cast("string"), F.lit(""))
        return F.lit("")

    df = df.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "||",
                safe_str("user_id"),
                safe_str("event_ts"),
                safe_str("event_type"),
                safe_str("product_id"),
            ),
            256,
        ),
    )
    return df


def dedupe_events(df: DataFrame) -> DataFrame:
    """
    Deduplicate on event_id.
    If ingest_ts exists, keep the most recent record per event_id.
    Otherwise use dropDuplicates(["event_id"]).
    """
    if "ingest_ts" in df.columns:
        w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc_nulls_last())
        df = (
            df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )
    else:
        df = df.dropDuplicates(["event_id"])

    return df


def select_stable_schema(df: DataFrame) -> DataFrame:
    """
    Optional: enforce a stable output column order.
    Keeps known columns first, then any extras after.
    """
    preferred_order = [
        "event_id",
        "event_ts",
        "event_date",
        "event_type",
        "user_id",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "ingest_ts",
        "source_file",
    ]

    existing_preferred = [c for c in preferred_order if c in df.columns]
    extras = [c for c in df.columns if c not in existing_preferred]

    return df.select(*(existing_preferred + extras))


def write_silver(df: DataFrame, silver_dir: Path, mode: str) -> None:
    silver_dir.parent.mkdir(parents=True, exist_ok=True)

    writer = df.write.mode(mode)
    if "event_date" in df.columns:
        writer = writer.partitionBy("event_date")

    writer.parquet(str(silver_dir))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Silver clean + standardize + dedupe"
    )
    parser.add_argument(
        "--bronze-dir",
        default=str(DEFAULT_BRONZE_DIR),
        help="Input bronze parquet path",
    )
    parser.add_argument(
        "--silver-dir",
        default=str(DEFAULT_SILVER_DIR),
        help="Output silver parquet path",
    )
    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Write mode",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=10,
        help="Show N rows for verification",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    spark = build_spark()

    try:
        bronze_path = Path(args.bronze_dir)
        silver_path = Path(args.silver_dir)

        df = read_bronze_parquet(spark, bronze_path)
        df = clean_strings(df)
        df = standardize_types(df)
        df = add_event_id(df)
        df = dedupe_events(df)
        df = select_stable_schema(df)

        print("Silver schema:")
        df.printSchema()

        print(f"Silver row count: {df.count()}")

        if args.show > 0:
            # For quick local tests avoid collecting large results to driver
            n = min(args.show, 20)
            df.limit(n).show(n, truncate=False)

        write_silver(df, silver_path, args.mode)
        print(f"Silver data written to: {silver_path}")

        return 0

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())