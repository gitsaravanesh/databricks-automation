"""
Bronze: one Delta table per source folder, files loaded as-is.

Auto Loader reads new Parquet files from <source_root>/<folder>/ and remembers in
its checkpoint which files it has processed, so every run loads only new files.
Two lineage columns are added: the source file and the load time.

Run by the job's "bronze" task on the existing cluster (see resources/bronze_silver.job.yml).
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
from pyspark.sql import functions as F  # noqa: E402

from utilities.settings import autoloader_options, get_settings, parse_args  # noqa: E402
from utilities.streams import run_all, run_until_done  # noqa: E402
from utilities.tables import TABLES  # noqa: E402


def load_bronze(spark, settings, cfg):
    folder = cfg["folder"]

    def start():
        return (
            spark.readStream.format("cloudFiles")
            .options(**autoloader_options(settings, folder))
            .load(settings.source_path(folder))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
            .writeStream.option("checkpointLocation", settings.checkpoint_path("bronze", folder))
            .option("mergeSchema", "true")            # new source columns are added to the table
            .trigger(availableNow=True)
            .toTable(settings.bronze_table(folder))
        )

    run_until_done(start)


def main(argv):
    settings = get_settings(parse_args(argv))
    spark = SparkSession.builder.getOrCreate()
    run_all(TABLES, lambda cfg: load_bronze(spark, settings, cfg))


if __name__ == "__main__":
    main(sys.argv[1:])
