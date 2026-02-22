bronze: raw ingest
silver: cleaned, standardized
gold: analytics tables
1. create snowflake account
2. create databrick environment
3. normalize standardize data from kaggle
4. load data to sflake
5. create data quality checks
more detailed steps:
0) Create project structure
Create repo folder: ecommerce-lakehouse-spark-snowflake/
Add folders:
data/raw/ (Kaggle files)
data/bronze/
data/silver/
data/gold/
notebooks/ (or src/)
sql/
docs/
Add .gitignore to ignore data/raw/ (keep only samples tracked)
1) Write your “data contract” first
Create docs/data_contract.md with:
Entities: customer/user, product, event, order (if present), session (derived)
Layer definitions:
Bronze = raw ingest + ingestion metadata
Silver = cleaned/typed/deduped/standardized
Gold = analytics tables (facts/dims)
Keys:
event_id if present; otherwise generate deterministic hash: sha2(concat(user_id, event_time, event_type, product_id), 256)
customer_id (user_id), product_id
2) Set up Spark environment (choose one)
Option A: Local PySpark venv (pip install pyspark pandas pyarrow)
Option B: Databricks Community Edition notebook + upload data
3) Bronze ingest (first pipeline)
Create notebooks/01_bronze_ingest.ipynb:
Read raw dataset from data/raw/...
Add ingestion metadata:
ingest_ts = current timestamp
source_file = input file name
Write parquet to data/bronze/ partitioned by date:
event_date = to_date(event_timestamp)
Output: data/bronze/events/ parquet partitions
4) Silver clean + standardize + dedupe
Create notebooks/02_silver_clean.ipynb:
Read bronze parquet
Standardize types (timestamps, numerics)
Clean (trim strings, null handling)
Deduplicate:
define or generate event_id
dropDuplicates(["event_id"])
Write to data/silver/events/
Output: stable schema + event_id
5) Gold analytics models
Create notebooks/03_gold_models.ipynb and build:
dim_customer (customer_id, first_seen_ts, last_seen_ts, etc.)
dim_product (product_id, category/brand/price band if available)
fact_events (event_id, event_ts, event_type, customer_id, product_id, session_id, revenue if any)
session_summary (session_id, customer_id, start/end, event_count, purchased_flag)
Write to data/gold/... for each table.
6) Incremental processing (after gold works once)
Create notebooks/04_incremental_load.ipynb:
Maintain checkpoint file: data/checkpoints/bronze_ingested_files.json
Each run:
list raw files
ingest only unseen files (append to bronze)
Silver/Gold:
process only new date partitions or event_ts > max_processed_ts
7) Load Gold into Snowflake (free tier)
Create Snowflake trial account
Create DB/schema:
ECOMMERCE_DE
ANALYTICS
Create tables: DIM_CUSTOMER, DIM_PRODUCT, FACT_EVENTS, SESSION_SUMMARY
Start simple load:
export gold tables to CSV (data/export/*.csv)
upload to Snowflake internal stage
COPY INTO tables
(Automate later with Snowflake Python connector.)
8) Add data quality checks
Create notebooks/05_data_quality.ipynb:
event_id not null
no duplicate event_id
event_ts not null
expected row count relationships (bronze/silver/gold)
valid event_type values
FK coverage (product_id in fact exists in dim)
Write results to data/gold/data_quality_results/
9) Portfolio packaging
In README.md, include:
architecture diagram
medallion explanation
link to data contract
how to run locally
Snowflake load steps
screenshots showing Snowflake tables populated
Minimal first milestone (what to do first)
Folders + README + docs/data_contract.md
PySpark environment
Build 01_bronze_ingest and confirm parquet output
Build 02_silver_clean and confirm dedupe + event_id