"""Checks on the table list: catch config mistakes before anything deploys."""

from utilities.tables import TABLES, key_checks


def test_folders_are_unique():
    folders = [t["folder"] for t in TABLES]
    assert len(folders) == len(set(folders)), "a folder is listed twice"


def test_every_table_has_keys():
    for t in TABLES:
        assert t["keys"], f"{t['folder']} has no key columns"


def test_cdc_tables_have_valid_scd_type():
    for t in TABLES:
        if t["cdc"]:
            assert t.get("scd") in ("1", "2"), f"{t['folder']}: scd must be '1' or '2'"


def test_append_tables_have_event_time():
    for t in TABLES:
        if not t["cdc"]:
            assert t.get("event_time"), f"{t['folder']}: append tables need event_time for de-duplication"


def test_key_checks_cover_every_key():
    checks = key_checks(["order_id", "line_no"])
    assert checks == {
        "order_id_not_null": "order_id IS NOT NULL",
        "line_no_not_null": "line_no IS NOT NULL",
    }
