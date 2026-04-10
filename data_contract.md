Entities: customer/user, product, event, order, session (derived)

Layer definitions:
Bronze = raw ingest + ingestion metadata
Silver = cleaned/typed/deduped/standardized
Gold = analytics tables (facts/dims)
- dim_customer: customer_id, first_seen_ts, last_seen_ts, total_events (one row per customer).
- dim_product: product_id, optional category, brand, price, price_band (one row per product).
- fact_events: event_id, event_ts, event_type, customer_id, product_id, session_id, revenue, event_date (event-level rows).
- session_summary: session_id, customer_id, start_ts, end_ts, event_count, purchased_flag.

Keys:
event_id if present; otherwise deterministic hash: sha2(concat(user_id, event_time, event_type, product_id), 256)
customer_id (user_id), product_id