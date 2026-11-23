"""
Bronze: one streaming table per source folder, files loaded as-is.

Auto Loader reads new Parquet files from <source_root>/<folder>/ and the
pipeline remembers which files it has processed, so every run loads only new
files. Two lineage columns are added: the source file and the load time.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.settings import autoloader_options, get_settings
from utilities.tables import TABLES

settings = get_settings(spark.conf)  # noqa: F821  (spark is provided by the pipeline)


def define_bronze(cfg):
    # A factory function binds this table's folder. A plain for-loop closure
    # would make every table read the last folder in the list.
    folder = cfg["folder"]

    @dp.table(
        name=settings.bronze_table(folder),
        comment=f"Raw Parquet files from {settings.source_path(folder)}, loaded as-is.",
        table_properties={
            "quality": "bronze",
            # Landing files may be moved or deleted after loading, so stop a
            # full refresh from emptying bronze and losing data.
            "pipelines.reset.allowed": "false",
        },
    )
    def bronze():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .options(**autoloader_options(settings, folder))
            .load(settings.source_path(folder))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )


for cfg in TABLES:
    define_bronze(cfg)
