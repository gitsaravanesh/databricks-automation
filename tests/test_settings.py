"""Checks on settings and Auto Loader options (no Spark needed)."""

import pytest

from utilities.settings import autoloader_options, get_settings, parse_args

BASE = {
    "source_root": "/Volumes/workspace/dev_bronze/landing/incoming/",
    "catalog": "workspace",
    "bronze_schema": "dev_bronze",
    "silver_schema": "dev_silver",
    "archive_root": "/Volumes/workspace/dev_bronze/landing/archive",
}


def test_names_and_paths():
    s = get_settings(BASE)
    assert s.source_root.endswith("incoming")  # trailing slash removed
    assert s.source_path("orders") == "/Volumes/workspace/dev_bronze/landing/incoming/orders/"
    assert s.bronze_table("orders") == "workspace.dev_bronze.orders"
    assert s.silver_table("orders") == "workspace.dev_silver.orders"


def test_clean_source_off_by_default():
    opts = autoloader_options(get_settings(BASE), "orders")
    assert opts["cloudFiles.format"] == "parquet"
    assert "cloudFiles.cleanSource" not in opts


def test_clean_source_move_sets_archive_per_table():
    s = get_settings({**BASE, "clean_source": "move", "archive_after": "1 hour"})
    opts = autoloader_options(s, "orders")
    assert opts["cloudFiles.cleanSource"] == "MOVE"
    assert opts["cloudFiles.cleanSource.moveDestination"] == "/Volumes/workspace/dev_bronze/landing/archive/orders/"
    assert opts["cloudFiles.cleanSource.retentionDuration"] == "1 hour"


def test_archive_inside_source_is_rejected():
    bad = {**BASE, "clean_source": "MOVE",
           "archive_root": "/Volumes/workspace/dev_bronze/landing/incoming/archive"}
    with pytest.raises(ValueError):
        autoloader_options(get_settings(bad), "orders")


def test_landing_defaults_to_volume_in_bronze_schema():
    s = get_settings({"catalog": "main", "bronze_schema": "test_bronze", "silver_schema": "test_silver",
                      "source_root": "", "archive_root": ""})
    assert s.source_root == "/Volumes/main/test_bronze/landing/incoming"
    assert s.archive_root == "/Volumes/main/test_bronze/landing/archive"


def test_cloud_storage_paths_are_used_when_given():
    s = get_settings({**BASE, "source_root": "abfss://landing@acct.dfs.core.windows.net/incoming/"})
    assert s.source_path("orders") == "abfss://landing@acct.dfs.core.windows.net/incoming/orders/"


def test_parse_args_reads_job_parameters():
    args = parse_args(["--catalog=main", "--source_root=", "--archive_after=30 days"])
    assert args == {"catalog": "main", "source_root": "", "archive_after": "30 days"}


def test_parse_args_rejects_other_formats():
    with pytest.raises(ValueError):
        parse_args(["catalog=main"])


def test_checkpoints_default_to_landing_volume():
    s = get_settings({**BASE, "checkpoint_root": ""})
    assert s.checkpoint_path("silver", "orders") == "/Volumes/workspace/dev_bronze/landing/_checkpoints/silver/orders"
    opts = autoloader_options(s, "orders")
    assert opts["cloudFiles.schemaLocation"] == s.checkpoint_path("bronze", "orders")


def test_checkpoints_inside_source_are_rejected():
    bad = {**BASE, "checkpoint_root": "/Volumes/workspace/dev_bronze/landing/incoming/_checkpoints"}
    with pytest.raises(ValueError):
        autoloader_options(get_settings(bad), "orders")
