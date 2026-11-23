"""Checks that the sample data matches the table list (needs pyarrow)."""

from pathlib import Path

import pytest

from utilities.tables import OP_COL, SEQUENCE_COL, TABLES

pq = pytest.importorskip("pyarrow.parquet")

SAMPLE = Path(__file__).resolve().parents[1] / "sample_data"
BATCHES = sorted(p for p in SAMPLE.glob("batch_*") if p.is_dir())


def test_three_batches_exist():
    assert len(BATCHES) == 3


def test_first_batch_has_every_table():
    # Auto Loader needs at least one file per folder on the first run to infer the schema
    first = {p.name for p in BATCHES[0].iterdir() if p.is_dir()}
    assert first == {t["folder"] for t in TABLES}


@pytest.mark.parametrize("cfg", TABLES, ids=lambda c: c["folder"])
def test_files_have_expected_columns(cfg):
    needed = set(cfg["keys"])
    needed |= {OP_COL, SEQUENCE_COL} if cfg["cdc"] else {cfg["event_time"]}
    files = [f for b in BATCHES for f in (b / cfg["folder"]).glob("*.parquet")]
    assert files, f"no files for {cfg['folder']}"
    for f in files:
        cols = set(pq.read_schema(f).names)
        assert needed <= cols, f"{f} is missing {needed - cols}"
