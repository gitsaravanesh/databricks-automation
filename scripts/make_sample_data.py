"""
Generate sample CDC Parquet files for the pipeline, plus EXPECTED.md.

Three batches, as a CDC tool would deliver them over three days:
  batch_01_initial_load   every table's starting rows (inserts)
  batch_02_changes        updates, new rows, deletes, and one row with a missing key
  batch_03_more_changes   more updates, a late (out-of-order) update, a delete,
                          and duplicate web events

Run from the repo root:
    pip install pyarrow pandas
    python scripts/make_sample_data.py
"""

import argparse
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src" / "bronze_silver"))
from utilities.tables import OP_COL, SEQUENCE_COL, TABLES  # noqa: E402

UTC = timezone.utc
T1 = datetime(2026, 11, 20, 9, 0, tzinfo=UTC)   # batch 1 commit times
T2 = datetime(2026, 11, 21, 9, 0, tzinfo=UTC)   # batch 2
T3 = datetime(2026, 11, 22, 9, 0, tzinfo=UTC)   # batch 3

BATCH_DIRS = {1: "batch_01_initial_load", 2: "batch_02_changes", 3: "batch_03_more_changes"}

TS = pa.timestamp("us", tz="UTC")
SCHEMAS = {
    "stores": pa.schema([("store_id", pa.int64()), ("store_name", pa.string()), ("city", pa.string()),
                         ("region", pa.string()), ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "customers": pa.schema([("customer_id", pa.int64()), ("full_name", pa.string()), ("email", pa.string()),
                            ("city", pa.string()), ("country", pa.string()),
                            ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "products": pa.schema([("product_id", pa.int64()), ("product_name", pa.string()),
                           ("category", pa.string()), ("price", pa.float64()),
                           ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "orders": pa.schema([("order_id", pa.int64()), ("customer_id", pa.int64()), ("store_id", pa.int64()),
                         ("order_date", pa.date32()), ("status", pa.string()), ("total_amount", pa.float64()),
                         ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "order_items": pa.schema([("order_id", pa.int64()), ("line_no", pa.int64()), ("product_id", pa.int64()),
                              ("qty", pa.int64()), ("unit_price", pa.float64()),
                              ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "payments": pa.schema([("payment_id", pa.int64()), ("order_id", pa.int64()), ("method", pa.string()),
                           ("amount", pa.float64()), ("status", pa.string()),
                           ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "shipments": pa.schema([("shipment_id", pa.int64()), ("order_id", pa.int64()), ("carrier", pa.string()),
                            ("status", pa.string()), ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "returns": pa.schema([("return_id", pa.int64()), ("order_id", pa.int64()), ("reason", pa.string()),
                          ("refund_amount", pa.float64()), ("status", pa.string()),
                          ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "inventory": pa.schema([("product_id", pa.int64()), ("store_id", pa.int64()), ("qty_on_hand", pa.int64()),
                            ("op", pa.string()), ("mdb_commit_ts", TS)]),
    "web_events": pa.schema([("event_id", pa.string()), ("customer_id", pa.int64()), ("event_type", pa.string()),
                             ("page", pa.string()), ("event_ts", TS)]),
}


def at(base, seconds):
    return base + timedelta(seconds=seconds)


# --------------------------------------------------------------------------
# Starting data
# --------------------------------------------------------------------------
STORES = {
    1: ("Downtown Toronto", "Toronto", "CA"),
    2: ("Seattle Central", "Seattle", "US"),
    3: ("Austin Hub", "Austin", "US"),
    4: ("Vancouver West", "Vancouver", "CA"),
    5: ("Boston Back Bay", "Boston", "US"),
}
CUSTOMERS = {
    1: ("Asha Raman", "asha.raman@example.com", "Seattle", "US"),
    2: ("Ben Carter", "ben.carter@example.com", "Austin", "US"),
    3: ("Chloe Martin", "chloe.martin@example.com", "Toronto", "CA"),
    4: ("Dev Patel", "dev.patel@example.com", "Boston", "US"),
    5: ("Emma Wilson", "emma.wilson@example.com", "Vancouver", "CA"),
    6: ("Farid Khan", "farid.khan@example.com", "Seattle", "US"),
    7: ("Grace Lee", "grace.lee@example.com", "Toronto", "CA"),
    8: ("Hiro Tanaka", "hiro.tanaka@example.com", "Austin", "US"),
}
PRODUCTS = {
    1: ("Mirrorless Camera", "Cameras", 899.00),
    2: ("Camera Strap", "Accessories", 49.99),
    3: ("50mm Lens", "Lenses", 349.00),
    4: ("Tripod", "Accessories", 129.00),
    5: ("Photo Book", "Prints", 59.00),
    6: ("Canvas Print", "Prints", 89.00),
}
# order_id: (customer_id, store_id, order_date, status)
ORDERS = {
    1: (1, 2, date(2026, 11, 19), "PLACED"), 2: (2, 3, date(2026, 11, 19), "PLACED"),
    3: (3, 1, date(2026, 11, 19), "PLACED"), 4: (4, 5, date(2026, 11, 19), "SHIPPED"),
    5: (5, 4, date(2026, 11, 20), "PLACED"), 6: (6, 2, date(2026, 11, 20), "DELIVERED"),
    7: (7, 1, date(2026, 11, 20), "PLACED"), 8: (8, 3, date(2026, 11, 20), "PLACED"),
    9: (1, 2, date(2026, 11, 20), "PLACED"), 10: (2, 3, date(2026, 11, 20), "PLACED"),
}


def order_lines(order_id):
    """Odd orders have 2 lines, even orders 1. Returns [(line_no, product_id, qty)]."""
    lines = [(1, (order_id % 6) + 1, 1)]
    if order_id % 2:
        lines.append((2, ((order_id + 2) % 6) + 1, 2))
    return lines


def order_total(order_id):
    return round(sum(PRODUCTS[p][2] * q for _, p, q in order_lines(order_id)), 2)


# --------------------------------------------------------------------------
# Batches: {table: [row dict, ...]}
# --------------------------------------------------------------------------
def batch_1():
    b = {}
    b["stores"] = [dict(store_id=i, store_name=n, city=c, region=r, op="c", mdb_commit_ts=at(T1, i))
                   for i, (n, c, r) in STORES.items()]
    b["customers"] = [dict(customer_id=i, full_name=n, email=e, city=c, country=k, op="c",
                           mdb_commit_ts=at(T1, 10 + i)) for i, (n, e, c, k) in CUSTOMERS.items()]
    b["products"] = [dict(product_id=i, product_name=n, category=c, price=p, op="c", mdb_commit_ts=at(T1, 20 + i))
                     for i, (n, c, p) in PRODUCTS.items()]
    b["orders"] = [dict(order_id=i, customer_id=c, store_id=s, order_date=d, status=st,
                        total_amount=order_total(i), op="c", mdb_commit_ts=at(T1, 100 + i))
                   for i, (c, s, d, st) in ORDERS.items()]
    b["order_items"] = [dict(order_id=o, line_no=ln, product_id=p, qty=q, unit_price=PRODUCTS[p][2], op="c",
                             mdb_commit_ts=at(T1, 100 + o))
                        for o in ORDERS for ln, p, q in order_lines(o)]
    b["payments"] = [dict(payment_id=100 + o, order_id=o, method=("CARD" if o % 3 else "PAYPAL"),
                          amount=order_total(o), status="CAPTURED", op="c", mdb_commit_ts=at(T1, 200 + o))
                     for o in ORDERS]
    b["shipments"] = [dict(shipment_id=1001, order_id=4, carrier="UPS", status="IN_TRANSIT", op="c",
                           mdb_commit_ts=at(T1, 300))]
    b["returns"] = [dict(return_id=501, order_id=6, reason="Damaged in transit", refund_amount=20.00,
                         status="REQUESTED", op="c", mdb_commit_ts=at(T1, 400))]
    b["inventory"] = [dict(product_id=p, store_id=s, qty_on_hand=10 * p + s, op="c",
                           mdb_commit_ts=at(T1, 500 + 10 * p + s))
                      for p in PRODUCTS for s in (1, 2)]
    b["web_events"] = [dict(event_id=f"e-{i:04d}", customer_id=(i % 8) + 1,
                            event_type=("view" if i % 3 else "add_to_cart"), page=f"/product/{(i % 6) + 1}",
                            event_ts=at(T1, 60 * i)) for i in range(1, 11)]
    return b


def batch_2():
    b = {}
    b["stores"] = [dict(store_id=3, store_name="Austin Domain", city="Austin", region="US", op="u",
                        mdb_commit_ts=at(T2, 1))]
    b["customers"] = [
        dict(customer_id=1, full_name="Asha Raman", email="asha.raman@example.com", city="Portland", country="US",
             op="u", mdb_commit_ts=at(T2, 10)),
        dict(customer_id=9, full_name="Isla Brown", email="isla.brown@example.com", city="Toronto", country="CA",
             op="c", mdb_commit_ts=at(T2, 11)),
        dict(customer_id=10, full_name="Jay Singh", email="jay.singh@example.com", city="Boston", country="US",
             op="c", mdb_commit_ts=at(T2, 12)),
    ]
    b["products"] = [dict(product_id=2, product_name="Camera Strap", category="Accessories", price=44.99, op="u",
                          mdb_commit_ts=at(T2, 20))]

    def order_row(i, status, op, t, **over):
        c, s, d, _ = ORDERS.get(i, (1, 1, date(2026, 11, 21), "PLACED"))
        row = dict(order_id=i, customer_id=c, store_id=s, order_date=d, status=status,
                   total_amount=order_total(i) if i in ORDERS else 0.0, op=op, mdb_commit_ts=t)
        row.update(over)
        return row

    b["orders"] = [
        order_row(1, "SHIPPED", "u", at(T2, 101)),
        order_row(2, "SHIPPED", "u", at(T2, 102)),
        order_row(3, "SHIPPED", "u", at(T2, 103)),
        order_row(5, "PLACED", "d", at(T2, 105)),                       # order 5 deleted
        order_row(11, "PLACED", "c", at(T2, 111), customer_id=9, store_id=1,
                  order_date=date(2026, 11, 21), total_amount=899.00),
        order_row(12, "PLACED", "c", at(T2, 112), customer_id=10, store_id=5,
                  order_date=date(2026, 11, 21), total_amount=188.00),
        order_row(None, "PLACED", "c", at(T2, 199), customer_id=3, store_id=1,   # missing key: dropped in silver
                  order_date=date(2026, 11, 21), total_amount=59.00),
    ]
    b["order_items"] = [
        dict(order_id=1, line_no=1, product_id=2, qty=3, unit_price=44.99, op="u", mdb_commit_ts=at(T2, 101)),
        dict(order_id=5, line_no=1, product_id=6, qty=1, unit_price=89.00, op="d", mdb_commit_ts=at(T2, 105)),
        dict(order_id=5, line_no=2, product_id=2, qty=2, unit_price=49.99, op="d", mdb_commit_ts=at(T2, 105)),
        dict(order_id=11, line_no=1, product_id=1, qty=1, unit_price=899.00, op="c", mdb_commit_ts=at(T2, 111)),
        dict(order_id=12, line_no=1, product_id=4, qty=1, unit_price=129.00, op="c", mdb_commit_ts=at(T2, 112)),
        dict(order_id=12, line_no=2, product_id=5, qty=1, unit_price=59.00, op="c", mdb_commit_ts=at(T2, 112)),
    ]
    b["payments"] = [
        dict(payment_id=105, order_id=5, method="CARD", amount=order_total(5), status="REFUNDED", op="u",
             mdb_commit_ts=at(T2, 205)),
        dict(payment_id=111, order_id=11, method="CARD", amount=899.00, status="CAPTURED", op="c",
             mdb_commit_ts=at(T2, 211)),
        dict(payment_id=112, order_id=12, method="PAYPAL", amount=188.00, status="CAPTURED", op="c",
             mdb_commit_ts=at(T2, 212)),
    ]
    b["shipments"] = [
        dict(shipment_id=1001, order_id=4, carrier="UPS", status="DELIVERED", op="u", mdb_commit_ts=at(T2, 300)),
        dict(shipment_id=1002, order_id=1, carrier="FedEx", status="IN_TRANSIT", op="c", mdb_commit_ts=at(T2, 301)),
        dict(shipment_id=1003, order_id=2, carrier="UPS", status="IN_TRANSIT", op="c", mdb_commit_ts=at(T2, 302)),
        dict(shipment_id=1004, order_id=3, carrier="Canada Post", status="IN_TRANSIT", op="c",
             mdb_commit_ts=at(T2, 303)),
    ]
    b["returns"] = [
        dict(return_id=501, order_id=6, reason="Damaged in transit", refund_amount=20.00, status="APPROVED",
             op="u", mdb_commit_ts=at(T2, 400)),
        dict(return_id=502, order_id=4, reason="Wrong size", refund_amount=129.00, status="REQUESTED",
             op="c", mdb_commit_ts=at(T2, 401)),
    ]
    b["inventory"] = [
        dict(product_id=2, store_id=1, qty_on_hand=12, op="u", mdb_commit_ts=at(T2, 500)),
        dict(product_id=3, store_id=2, qty_on_hand=25, op="u", mdb_commit_ts=at(T2, 501)),
    ]
    b["web_events"] = [dict(event_id=f"e-{i:04d}", customer_id=(i % 10) + 1,
                            event_type=("view" if i % 2 else "purchase"), page=f"/product/{(i % 6) + 1}",
                            event_ts=at(T2, 60 * (i - 10))) for i in range(11, 19)]
    return b


def batch_3():
    b = {}
    b["customers"] = [
        dict(customer_id=1, full_name="Asha Raman", email="asha.r@example.com", city="Portland", country="US",
             op="u", mdb_commit_ts=at(T3, 10)),
        dict(customer_id=4, full_name="Dev Patel", email="dev.patel@example.com", city="Boston", country="US",
             op="d", mdb_commit_ts=at(T3, 11)),                         # customer 4 deleted
    ]
    b["products"] = [
        dict(product_id=2, product_name="Camera Strap", category="Accessories", price=39.99, op="u",
             mdb_commit_ts=at(T3, 20)),
        dict(product_id=7, product_name="Lens Cleaning Kit", category="Accessories", price=19.99, op="c",
             mdb_commit_ts=at(T3, 21)),
    ]
    c1, s1, d1, _ = ORDERS[1]
    c2, s2, d2, _ = ORDERS[2]
    b["orders"] = [
        dict(order_id=1, customer_id=c1, store_id=s1, order_date=d1, status="DELIVERED",
             total_amount=order_total(1), op="u", mdb_commit_ts=at(T3, 101)),
        # Late, out-of-order update: committed BEFORE batch 2's update of order 2,
        # but delivered now. AUTO CDC keeps the newer value (SHIPPED).
        dict(order_id=2, customer_id=c2, store_id=s2, order_date=d2, status="CANCELLED",
             total_amount=order_total(2), op="u", mdb_commit_ts=datetime(2026, 11, 21, 8, 0, tzinfo=UTC)),
        dict(order_id=13, customer_id=7, store_id=4, order_date=date(2026, 11, 22), status="PLACED",
             total_amount=19.99, op="c", mdb_commit_ts=at(T3, 113)),
    ]
    b["order_items"] = [
        dict(order_id=13, line_no=1, product_id=7, qty=1, unit_price=19.99, op="c", mdb_commit_ts=at(T3, 113)),
    ]
    b["payments"] = [
        dict(payment_id=113, order_id=13, method="CARD", amount=19.99, status="CAPTURED", op="c",
             mdb_commit_ts=at(T3, 213)),
    ]
    b["shipments"] = [
        dict(shipment_id=1002, order_id=1, carrier="FedEx", status="DELIVERED", op="u", mdb_commit_ts=at(T3, 301)),
    ]
    b["returns"] = [
        dict(return_id=502, order_id=4, reason="Wrong size", refund_amount=129.00, status="REJECTED",
             op="u", mdb_commit_ts=at(T3, 401)),
    ]
    b["inventory"] = [
        dict(product_id=6, store_id=2, qty_on_hand=62, op="d", mdb_commit_ts=at(T3, 500)),   # discontinued
        dict(product_id=7, store_id=1, qty_on_hand=40, op="c", mdb_commit_ts=at(T3, 501)),
        dict(product_id=2, store_id=1, qty_on_hand=8, op="u", mdb_commit_ts=at(T3, 502)),
    ]
    events = [dict(event_id=f"e-{i:04d}", customer_id=(i % 10) + 1,
                   event_type=("view" if i % 2 else "add_to_cart"), page=f"/product/{(i % 7) + 1}",
                   event_ts=at(T3, 60 * (i - 18))) for i in range(19, 25)]
    events.append(dict(events[0]))                          # e-0019 sent twice in this batch
    first = batch_1()["web_events"][2]                      # e-0003 re-sent two days later
    events.append(dict(first))
    b["web_events"] = events
    return b


# --------------------------------------------------------------------------
# Write files
# --------------------------------------------------------------------------
def write_batches(out: Path):
    if out.exists():
        shutil.rmtree(out)
    for n, make in ((1, batch_1), (2, batch_2), (3, batch_3)):
        for table, rows in make().items():
            folder = out / BATCH_DIRS[n] / table
            folder.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMAS[table]),
                           folder / f"{table}_batch_{n:02d}.parquet")


# --------------------------------------------------------------------------
# Expected results, computed from the files just written
# --------------------------------------------------------------------------
def read_up_to(out: Path, table: str, upto: int):
    import pandas as pd
    frames = [pq.read_table(f).to_pandas()
              for n in range(1, upto + 1)
              for f in sorted((out / BATCH_DIRS[n] / table).glob("*.parquet"))]
    return pd.concat(frames, ignore_index=True) if frames else None


def current_rows(df, cfg):
    """Rows AUTO CDC keeps: missing keys dropped, newest change per key, deletes removed."""
    df = df.dropna(subset=cfg["keys"])
    latest = df.sort_values(SEQUENCE_COL).groupby(cfg["keys"], as_index=False).tail(1)
    return latest[latest[OP_COL].str.lower() != "d"]


def write_expected(out: Path):
    lines = [
        "# Expected results",
        "",
        "Computed from the sample files. Upload one batch, run the pipeline, compare; then the next batch.",
        "",
        "| Table | SCD | Bronze rows after 1 / 2 / 3 | Silver rows after 1 / 2 / 3 |",
        "|---|---|---|---|",
    ]
    for cfg in TABLES:
        t = cfg["folder"]
        bronze, silver = [], []
        for k in (1, 2, 3):
            df = read_up_to(out, t, k)
            bronze.append(len(df))
            if cfg["cdc"]:
                silver.append(len(current_rows(df, cfg)))
            else:
                silver.append(df.dropna(subset=cfg["keys"])[cfg["keys"]].drop_duplicates().shape[0])
        scd = cfg.get("scd") or "append"
        note = " (current rows)" if scd == "2" else ""
        lines.append(f"| {t} | {scd} | {' / '.join(map(str, bronze))} | {' / '.join(map(str, silver))}{note} |")

    def status(k, oid):
        row = current_rows(read_up_to(out, "orders", k), {"keys": ["order_id"]})
        row = row[row.order_id == oid]
        return row.iloc[0]["status"] if len(row) else "(deleted)"

    lines += [
        "",
        "SCD 2 tables keep history: their row count also includes closed versions "
        "(`__END_AT` is set). Filter `__END_AT IS NULL` to count current rows.",
        "",
        "## What each batch demonstrates",
        "",
        "| Check | After batch 1 | After batch 2 | After batch 3 |",
        "|---|---|---|---|",
        f"| `orders` order 1 status (normal updates) | {status(1, 1)} | {status(2, 1)} | {status(3, 1)} |",
        f"| `orders` order 2 status (late CANCELLED in batch 3 is older, so ignored) "
        f"| {status(1, 2)} | {status(2, 2)} | {status(3, 2)} |",
        f"| `orders` order 5 (deleted in batch 2) | {status(1, 5)} | {status(2, 5)} | {status(3, 5)} |",
        "| `orders` row with missing `order_id` | - | in bronze, dropped from silver | same |",
        "| `customers` customer 1 versions (SCD 2) | 1 | 2 (city changed) | 3 (email changed) |",
        "| `customers` customer 4 (deleted in batch 3) | current | current | closed, no current row |",
        "| `products` product 2 price versions (SCD 2) | 49.99 | 49.99 → 44.99 | → 39.99 |",
        "| `web_events` duplicates (e-0019 twice, e-0003 re-sent) | - | - | bronze has both copies, silver has one |",
        "",
        "## Queries to check",
        "",
        "```sql",
        "-- Row counts, bronze vs silver",
        "SELECT 'bronze' AS layer, count(*) FROM workspace.dev_bronze.orders",
        "UNION ALL SELECT 'silver', count(*) FROM workspace.dev_silver.orders;",
        "",
        "-- Order 2 should stay SHIPPED: the late CANCELLED has an older commit time",
        "SELECT order_id, status, mdb_commit_ts FROM workspace.dev_silver.orders ORDER BY order_id;",
        "",
        "-- SCD 2 history for customer 1",
        "SELECT customer_id, city, email, __START_AT, __END_AT",
        "FROM workspace.dev_silver.customers WHERE customer_id = 1 ORDER BY __START_AT;",
        "",
        "-- The row with a missing key: in bronze only",
        "SELECT * FROM workspace.dev_bronze.orders WHERE order_id IS NULL;",
        "",
        "-- Which file each bronze row came from",
        "SELECT _source_file, count(*) FROM workspace.dev_bronze.orders GROUP BY _source_file;",
        "```",
        "",
    ]
    (out / "EXPECTED.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(REPO / "sample_data"))
    out = Path(parser.parse_args().out)
    write_batches(out)
    write_expected(out)
    print(f"Wrote sample data and EXPECTED.md to {out}")


if __name__ == "__main__":
    main()
