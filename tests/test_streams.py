"""Checks on the stream runner (no Spark needed)."""

import pytest

from utilities.streams import is_schema_change, run_all, run_until_done


class FakeQuery:
    def __init__(self, error=None):
        self.error = error

    def awaitTermination(self):  # noqa: N802  (Spark's name)
        if self.error:
            raise self.error


def test_schema_change_is_retried():
    queries = [FakeQuery(RuntimeError("[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE] new column")), FakeQuery()]
    run_until_done(lambda: queries.pop(0))
    assert queries == []


def test_other_errors_are_not_retried():
    queries = [FakeQuery(RuntimeError("table not found")), FakeQuery()]
    with pytest.raises(RuntimeError):
        run_until_done(lambda: queries.pop(0))
    assert len(queries) == 1


def test_is_schema_change():
    assert is_schema_change(RuntimeError("DELTA_SCHEMA_CHANGED_WITH_STARTING_OPTIONS"))
    assert not is_schema_change(RuntimeError("PERMISSION_DENIED"))


def test_one_failing_table_does_not_stop_the_rest():
    loaded = []

    def load(cfg):
        if cfg["folder"] == "b":
            raise RuntimeError("boom")
        loaded.append(cfg["folder"])

    with pytest.raises(RuntimeError, match="b"):
        run_all([{"folder": "a"}, {"folder": "b"}, {"folder": "c"}], load)
    assert loaded == ["a", "c"]
