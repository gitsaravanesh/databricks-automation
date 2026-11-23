"""
Applying CDC change events to a silver Delta table (used by silver.py in foreachBatch).

  SCD 1: one row per key. The newest change (by SEQUENCE_COL) wins: it updates or
         deletes the row. Changes older than the row already stored are ignored.
  SCD 2: every change adds a version with __START_AT / __END_AT (the commit times);
         the current version has __END_AT = NULL. A delete closes the current
         version. Changes older than the current version are ignored.

Limitation: a deleted key leaves nothing behind, so a change that is older than
the delete but arrives in a later run brings the key back.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utilities.tables import DELETE_WHEN, OP_COL, SEQUENCE_COL

IS_DELETE = "__is_delete"
START_AT = "__START_AT"
END_AT = "__END_AT"


def drop_missing_keys(batch: DataFrame, keys) -> DataFrame:
    """Rows with a missing key can't be matched to a silver row, so they stay in bronze only."""
    return batch.where(" AND ".join(f"`{k}` IS NOT NULL" for k in keys))


def ensure_table(spark, table: str, df: DataFrame) -> None:
    """Create the table from df's columns, or add df's new columns to it."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)
        return
    existing = {f.name.lower() for f in spark.table(table).schema.fields}
    new = [f for f in df.schema.fields if f.name.lower() not in existing]
    if new:
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in new)
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({cols})")


def _key_match(keys, source_prefix: str = "s.") -> str:
    return " AND ".join(f"t.`{k}` = {source_prefix}`{k}`" for k in keys)


def _assign(cols) -> dict:
    return {f"`{c}`": f"s.`{c}`" for c in cols}


def apply_scd1(spark, changes: DataFrame, table: str, keys) -> None:
    changes = changes.withColumn(IS_DELETE, F.expr(DELETE_WHEN)).drop(OP_COL)
    newest = (
        changes.withColumn("__rn", F.row_number().over(Window.partitionBy(*keys).orderBy(F.col(SEQUENCE_COL).desc())))
        .where("__rn = 1")
        .drop("__rn")
    )
    data_cols = [c for c in newest.columns if c != IS_DELETE]
    ensure_table(spark, table, newest.select(*data_cols))

    newer = f"s.`{SEQUENCE_COL}` > t.`{SEQUENCE_COL}`"
    (
        DeltaTable.forName(spark, table).alias("t")
        .merge(newest.alias("s"), _key_match(keys))
        .whenMatchedDelete(condition=f"s.{IS_DELETE} AND {newer}")
        .whenMatchedUpdate(condition=f"NOT s.{IS_DELETE} AND {newer}", set=_assign(data_cols))
        .whenNotMatchedInsert(condition=f"NOT s.{IS_DELETE}", values=_assign(data_cols))
        .execute()
    )


def apply_scd2(spark, changes: DataFrame, table: str, keys) -> None:
    changes = changes.withColumn(IS_DELETE, F.expr(DELETE_WHEN)).drop(OP_COL)
    data_cols = [c for c in changes.columns if c != IS_DELETE]
    template = changes.select(
        *data_cols,
        F.col(SEQUENCE_COL).alias(START_AT),
        F.col(SEQUENCE_COL).alias(END_AT),
    )
    ensure_table(spark, table, template)

    # Ignore changes that are not newer than the current version
    current = spark.table(table).where(f"{END_AT} IS NULL").select(*keys, F.col(START_AT).alias("__current_start"))
    changes = (
        changes.join(current, keys, "left")
        .where(f"__current_start IS NULL OR `{SEQUENCE_COL}` > __current_start")
        .drop("__current_start")
    )

    by_time = Window.partitionBy(*keys).orderBy(SEQUENCE_COL)
    changes = (
        changes.withColumn(START_AT, F.col(SEQUENCE_COL))
        .withColumn(END_AT, F.lead(SEQUENCE_COL).over(by_time))   # closed by the next change
        .withColumn("__rn", F.row_number().over(by_time))
    )
    key_types = {f.name: f.dataType for f in changes.schema.fields if f.name in keys}

    # Staged MERGE: the first change per key closes the current version (matched on
    # the __mk_ columns); every non-delete change is inserted (null __mk_ never matches).
    closes = changes.where("__rn = 1").select(
        "*", *[F.col(k).alias(f"__mk_{k}") for k in keys], F.lit("close").alias("__action")
    )
    inserts = changes.where(f"NOT {IS_DELETE}").select(
        "*", *[F.lit(None).cast(key_types[k]).alias(f"__mk_{k}") for k in keys], F.lit("insert").alias("__action")
    )
    staged = closes.unionByName(inserts)

    match = " AND ".join(f"t.`{k}` = s.`__mk_{k}`" for k in keys) + f" AND t.{END_AT} IS NULL"
    (
        DeltaTable.forName(spark, table).alias("t")
        .merge(staged.alias("s"), match)
        .whenMatchedUpdate(condition="s.__action = 'close'", set={END_AT: f"s.`{SEQUENCE_COL}`"})
        .whenNotMatchedInsert(condition="s.__action = 'insert'", values=_assign(data_cols + [START_AT, END_AT]))
        .execute()
    )
