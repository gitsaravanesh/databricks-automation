"""
Silver: cleaned data, one current row per key.

For every table, the new bronze rows since the last run are read, rows with a
missing key are dropped, and then:
  CDC tables    -> newest change per key wins (by commit time), deletes are
                   applied, SCD type 1 or 2 (utilities/merge.py)
  append tables -> duplicates removed within a time window

Run by the job's "silver" task on the existing cluster, after "bronze".
"""

import os
import sys

# Make utilities/ importable when the job runs this file directly
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:  # some runners exec the file without setting __file__
    _here = os.path.dirname(os.path.abspath(sys.argv[0]))
sys.path.insert(0, _here)

from pyspark.sql import SparkSession  # noqa: E402

from utilities.merge import apply_scd1, apply_scd2, drop_missing_keys  # noqa: E402
from utilities.settings import get_settings, parse_args  # noqa: E402
from utilities.streams import run_all, run_until_done  # noqa: E402
from utilities.tables import TABLES  # noqa: E402


def load_silver(spark, settings, cfg):
    folder = cfg["folder"]
    keys = cfg["keys"]
    target = settings.silver_table(folder)
    source = (
        spark.readStream.table(settings.bronze_table(folder))
        .drop("_rescued_data")   # kept in bronze; no-op if the column isn't there
    )

    if cfg["cdc"]:
        apply = apply_scd2 if cfg["scd"] == "2" else apply_scd1

        def write_batch(batch, batch_id):
            clean = drop_missing_keys(batch, keys).cache()
            dropped = batch.count() - clean.count()
            if dropped:
                print(f"{folder}: dropped {dropped} row(s) with a missing key")
            apply(batch.sparkSession, clean, target, keys)
            clean.unpersist()

        def start():
            return (
                source.writeStream.foreachBatch(write_batch)
                .option("checkpointLocation", settings.checkpoint_path("silver", folder))
                .trigger(availableNow=True)
                .start()
            )
    else:
        def start():
            return (
                drop_missing_keys(source, keys)
                # Remember events for 1 day to drop re-sent duplicates;
                # events more than 1 day late are dropped too.
                .withWatermark(cfg["event_time"], "1 day")
                .dropDuplicatesWithinWatermark(keys)
                .writeStream.option("checkpointLocation", settings.checkpoint_path("silver", folder))
                .trigger(availableNow=True)
                .toTable(target)
            )

    run_until_done(start)


def main(argv):
    settings = get_settings(parse_args(argv))
    spark = SparkSession.builder.getOrCreate()
    run_all(TABLES, lambda cfg: load_silver(spark, settings, cfg))


if __name__ == "__main__":
    main(sys.argv[1:])
