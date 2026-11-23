"""
Silver: cleaned data, one current row per key.

For every table:
  1. a cleaning view drops rows with a missing key (counted in the pipeline's
     data quality metrics)
  2. CDC tables  -> AUTO CDC: newest change per key wins (by commit time),
                    deletes are applied, SCD type 1 or 2
     append tables -> duplicates removed within a time window
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.settings import get_settings
from utilities.tables import DELETE_WHEN, OP_COL, SEQUENCE_COL, TABLES, key_checks

settings = get_settings(spark.conf)  # noqa: F821  (spark is provided by the pipeline)


def define_silver(cfg):
    folder = cfg["folder"]
    keys = cfg["keys"]
    clean_view = f"{folder}_clean"

    @dp.temporary_view(name=clean_view)
    @dp.expect_all_or_drop(key_checks(keys))
    def clean():
        return (
            spark.readStream.table(settings.bronze_table(folder))  # noqa: F821
            .drop("_rescued_data")  # kept in bronze; no-op if the column isn't there
        )

    if cfg["cdc"]:
        dp.create_streaming_table(
            name=settings.silver_table(folder),
            comment=f"Current {folder} (SCD type {cfg['scd']}) built from CDC events.",
            table_properties={"quality": "silver"},
        )
        dp.create_auto_cdc_flow(
            target=settings.silver_table(folder),
            source=clean_view,
            keys=keys,
            sequence_by=F.col(SEQUENCE_COL),
            apply_as_deletes=F.expr(DELETE_WHEN),
            except_column_list=[OP_COL],
            stored_as_scd_type=cfg["scd"],
        )
    else:
        @dp.table(
            name=settings.silver_table(folder),
            comment=f"Cleaned, de-duplicated {folder}.",
            table_properties={"quality": "silver"},
        )
        def silver_append():
            return (
                spark.readStream.table(clean_view)  # noqa: F821
                # Remember events for 1 day to drop re-sent duplicates;
                # events more than 1 day late are dropped too.
                .withWatermark(cfg["event_time"], "1 day")
                .dropDuplicatesWithinWatermark(keys)
            )


for cfg in TABLES:
    define_silver(cfg)
