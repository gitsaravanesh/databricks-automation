"""Checks on settings and Auto Loader options (no Spark needed)."""

import pytest

from utilities.settings import autoloader_options, get_settings

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


def test_move_without_archive_is_rejected():
    with pytest.raises(ValueError):
        autoloader_options(get_settings({**BASE, "clean_source": "MOVE", "archive_root": ""}), "orders")
