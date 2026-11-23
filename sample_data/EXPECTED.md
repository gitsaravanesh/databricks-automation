# Expected results

Computed from the sample files. Upload one batch, run the job, compare; then the next batch.

| Table | SCD | Bronze rows after 1 / 2 / 3 | Silver rows after 1 / 2 / 3 |
|---|---|---|---|
| stores | 1 | 5 / 6 / 6 | 5 / 5 / 5 |
| customers | 2 | 8 / 11 / 13 | 8 / 10 / 9 (current rows) |
| products | 2 | 6 / 7 / 9 | 6 / 6 / 7 (current rows) |
| orders | 1 | 10 / 17 / 20 | 10 / 11 / 12 |
| order_items | 1 | 15 / 21 / 22 | 15 / 16 / 17 |
| payments | 1 | 10 / 13 / 14 | 10 / 12 / 13 |
| shipments | 1 | 1 / 5 / 6 | 1 / 4 / 4 |
| returns | 1 | 1 / 3 / 4 | 1 / 2 / 2 |
| inventory | 1 | 12 / 14 / 17 | 12 / 12 / 12 |
| web_events | append | 10 / 18 / 26 | 10 / 18 / 24 |

SCD 2 tables keep history: their row count also includes closed versions (`__END_AT` is set). Filter `__END_AT IS NULL` to count current rows.

## What each batch demonstrates

| Check | After batch 1 | After batch 2 | After batch 3 |
|---|---|---|---|
| `orders` order 1 status (normal updates) | PLACED | SHIPPED | DELIVERED |
| `orders` order 2 status (late CANCELLED in batch 3 is older, so ignored) | PLACED | SHIPPED | SHIPPED |
| `orders` order 5 (deleted in batch 2) | PLACED | (deleted) | (deleted) |
| `orders` row with missing `order_id` | - | in bronze, dropped from silver | same |
| `customers` customer 1 versions (SCD 2) | 1 | 2 (city changed) | 3 (email changed) |
| `customers` customer 4 (deleted in batch 3) | current | current | closed, no current row |
| `products` product 2 price versions (SCD 2) | 49.99 | 49.99 → 44.99 | → 39.99 |
| `web_events` duplicates (e-0019 twice, e-0003 re-sent) | - | - | bronze has both copies, silver has one |

## Queries to check

```sql
-- Row counts, bronze vs silver
SELECT 'bronze' AS layer, count(*) FROM <catalog>.dev_bronze.orders
UNION ALL SELECT 'silver', count(*) FROM <catalog>.dev_silver.orders;

-- Order 2 should stay SHIPPED: the late CANCELLED has an older commit time
SELECT order_id, status, mdb_commit_ts FROM <catalog>.dev_silver.orders ORDER BY order_id;

-- SCD 2 history for customer 1
SELECT customer_id, city, email, __START_AT, __END_AT
FROM <catalog>.dev_silver.customers WHERE customer_id = 1 ORDER BY __START_AT;

-- The row with a missing key: in bronze only
SELECT * FROM <catalog>.dev_bronze.orders WHERE order_id IS NULL;

-- Which file each bronze row came from
SELECT _source_file, count(*) FROM <catalog>.dev_bronze.orders GROUP BY _source_file;
```
