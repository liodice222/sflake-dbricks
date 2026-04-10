#!/usr/bin/env python3
"""
03_gold_models.py — Gold analytics models (local PySpark)

Reads silver parquet from data/silver/events/, derives session_id (gap-based),
builds dim_customer, dim_product, fact_events, session_summary,
and writes each to data/gold/<table>/.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_SILVER_DIR = Path("/home/lea/portfolio-projects/sflake-dbricks/data/silver/events")
DEFAULT_GOLD_DIR = Path("/home/lea/portfolio-projects/sflake-dbricks/data/gold")

DEFAULT_SESSION_GAP_SEC = 30 * 60


def build_spark(app_name: str = "gold_models") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "24g")
        .config("spark.executor.memory", "24g")
        .config("spark.driver.maxResultSize", "6g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.local.dir", "/tmp/spark")
        .getOrCreate()
    )


def read_silver_parquet(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Silver directory not found: {path.resolve()}")
    return spark.read.parquet(str(path))


def is_purchase_event() -> F.Column:
    et = F.lower(F.trim(F.coalesce(F.col("event_type").cast("string"), F.lit(""))))
    return et.rlike("purchase|transaction|order|buy")


def add_revenue(df: DataFrame) -> DataFrame:
    qty = (
        F.coalesce(F.col("quantity").cast(T.DoubleType()), F.lit(1.0))
        if "quantity" in df.columns
        else F.lit(1.0)
    )
    if "price" in df.columns:
        price = F.col("price").cast(T.DoubleType())
    else:
        price = F.lit(None).cast(T.DoubleType())
    rev = F.when(is_purchase_event(), F.coalesce(price, F.lit(0.0)) * qty).otherwise(
        F.lit(None).cast(T.DoubleType())
    )
    return df.withColumn("revenue", rev)


def add_session_id(df: DataFrame, gap_seconds: int = DEFAULT_SESSION_GAP_SEC) -> DataFrame:
    """
    Gap-based sessions per user; rows with null user_id use event_id as the partition key
    so they are not lumped into one session.
    """
    ts = F.col("event_ts").cast(T.TimestampType())
    df2 = df.withColumn("event_ts", ts)

    session_key = F.coalesce(F.col("user_id").cast("string"), F.col("event_id").cast("string"))
    w_user = Window.partitionBy(session_key).orderBy(ts)
    prev_ts = F.lag(ts).over(w_user)
    gap = F.unix_timestamp(ts) - F.unix_timestamp(prev_ts)
    new_session = F.when(
        prev_ts.isNull() | gap.isNull() | (gap > F.lit(gap_seconds)),
        F.lit(1),
    ).otherwise(F.lit(0))

    w_cum = Window.partitionBy(session_key).orderBy(ts).rowsBetween(Window.unboundedPreceding, 0)
    session_grp = F.sum(new_session).over(w_cum)
    session_id = F.sha2(F.concat_ws("||", session_key, session_grp.cast("string")), 256)

    return df2.withColumn("session_id", session_id)


def build_dim_customer(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("user_id").isNotNull())
        .groupBy(F.col("user_id").alias("customer_id"))
        .agg(
            F.min("event_ts").alias("first_seen_ts"),
            F.max("event_ts").alias("last_seen_ts"),
            F.count(F.lit(1)).alias("total_events"),
        )
    )


def price_band_expr(price_col: F.Column) -> F.Column:
    return (
        F.when(price_col.isNull(), F.lit("unknown"))
        .when(price_col < 10, F.lit("under_10"))
        .when(price_col < 50, F.lit("10_to_50"))
        .when(price_col < 100, F.lit("50_to_100"))
        .otherwise(F.lit("100_plus"))
    )


def build_dim_product(df: DataFrame) -> DataFrame:
    base = df.filter(F.col("product_id").isNotNull())
    parts = []
    if "category_code" in df.columns:
        parts.append(F.col("category_code").cast("string"))
    if "category_id" in df.columns:
        parts.append(F.col("category_id").cast("string"))
    category = F.coalesce(*parts) if parts else F.lit(None).cast(T.StringType())
    brand_col = F.col("brand") if "brand" in df.columns else F.lit(None).cast(T.StringType())
    price_col = F.col("price").cast(T.DoubleType()) if "price" in df.columns else F.lit(None).cast(T.DoubleType())

    agg = base.groupBy("product_id").agg(
        F.max(category).alias("category"),
        F.max(brand_col).alias("brand"),
        F.avg(price_col).alias("price"),
    )
    return agg.withColumn("price_band", price_band_expr(F.col("price")))


def build_fact_events(df: DataFrame) -> DataFrame:
    cols = [
        F.col("event_id"),
        F.col("event_ts"),
        F.col("event_type"),
        F.col("user_id").alias("customer_id"),
        F.col("product_id"),
        F.col("session_id"),
        F.col("revenue"),
    ]
    if "event_date" in df.columns:
        cols.append(F.col("event_date"))
    return df.select(*cols)


def build_session_summary(df: DataFrame) -> DataFrame:
    tmp = df.withColumn("_purchase", is_purchase_event())
    return (
        tmp.groupBy("session_id", F.col("user_id").alias("customer_id"))
        .agg(
            F.min("event_ts").alias("start_ts"),
            F.max("event_ts").alias("end_ts"),
            F.count(F.lit(1)).alias("event_count"),
            F.max(F.when(F.col("_purchase"), F.lit(1)).otherwise(F.lit(0))).alias("_p"),
        )
        .withColumn("purchased_flag", F.col("_p") == F.lit(1))
        .drop("_p")
    )


def write_table(df: DataFrame, out_dir: Path, mode: str, partition_cols: list[str] | None = None) -> None:
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    w = df.write.mode(mode)
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.parquet(str(out_dir))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gold analytics models (dims + facts + session summary).")
    p.add_argument("--silver-dir", default=str(DEFAULT_SILVER_DIR), help="Input silver parquet path")
    p.add_argument("--gold-dir", default=str(DEFAULT_GOLD_DIR), help="Output root for gold tables")
    p.add_argument("--mode", choices=["overwrite", "append"], default="overwrite")
    p.add_argument(
        "--session-gap-minutes",
        type=int,
        default=30,
        help="Idle minutes before a new session (same session_key)",
    )
    p.add_argument("--show", type=int, default=5, help="Sample rows per table (0 = off)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    gap_sec = max(1, int(args.session_gap_minutes) * 60)

    silver_path = Path(args.silver_dir)
    gold_root = Path(args.gold_dir)

    spark = build_spark()
    try:
        events = read_silver_parquet(spark, silver_path)
        events = add_revenue(events)
        events = add_session_id(events, gap_seconds=gap_sec)

        events.cache()
        events.count()

        dim_customer = build_dim_customer(events)
        dim_product = build_dim_product(events)
        fact_events = build_fact_events(events)
        session_summary = build_session_summary(events)

        dim_customer.printSchema()
        dim_product.printSchema()
        fact_events.printSchema()
        session_summary.printSchema()

        n_show = min(args.show, 20)
        if n_show > 0:
            dim_customer.show(n_show, truncate=False)
            dim_product.show(n_show, truncate=False)
            fact_events.limit(n_show).show(n_show, truncate=False)
            session_summary.show(n_show, truncate=False)

        write_table(dim_customer, gold_root / "dim_customer", args.mode)
        write_table(dim_product, gold_root / "dim_product", args.mode)
        fe_parts = ["event_date"] if "event_date" in fact_events.columns else None
        write_table(fact_events, gold_root / "fact_events", args.mode, partition_cols=fe_parts)
        write_table(session_summary, gold_root / "session_summary", args.mode)

        print(f"Gold tables written under: {gold_root.resolve()}")
        events.unpersist()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())