"""
Source tables: one folder per table under the pipeline's source_root.

To add a table: add one entry below and drop its Parquet files into
<source_root>/<folder>/. Nothing else changes.

  keys        columns that identify a row
  cdc         True  = files hold change events (op + commit time)
              False = append-only data (no updates or deletes)
  scd         "1" keep latest only, "2" keep history (CDC tables only)
  event_time  event timestamp used to de-duplicate append-only tables
"""

# Columns the CDC tool adds to every change record
OP_COL = "op"                     # c = insert, u = update, d = delete
SEQUENCE_COL = "mdb_commit_ts"    # commit time in the source database: newest wins
DELETE_WHEN = "lower(op) = 'd'"   # which change records are deletes

TABLES = [
    {"folder": "stores",      "keys": ["store_id"],               "cdc": True, "scd": "1"},
    {"folder": "customers",   "keys": ["customer_id"],            "cdc": True, "scd": "2"},
    {"folder": "products",    "keys": ["product_id"],             "cdc": True, "scd": "2"},
    {"folder": "orders",      "keys": ["order_id"],               "cdc": True, "scd": "1"},
    {"folder": "order_items", "keys": ["order_id", "line_no"],    "cdc": True, "scd": "1"},
    {"folder": "payments",    "keys": ["payment_id"],             "cdc": True, "scd": "1"},
    {"folder": "shipments",   "keys": ["shipment_id"],            "cdc": True, "scd": "1"},
    {"folder": "returns",     "keys": ["return_id"],              "cdc": True, "scd": "1"},
    {"folder": "inventory",   "keys": ["product_id", "store_id"], "cdc": True, "scd": "1"},
    {"folder": "web_events",  "keys": ["event_id"],               "cdc": False, "event_time": "event_ts"},
]


def key_checks(keys):
    """Expectation rules: every key column must be present."""
    return {f"{k}_not_null": f"{k} IS NOT NULL" for k in keys}
