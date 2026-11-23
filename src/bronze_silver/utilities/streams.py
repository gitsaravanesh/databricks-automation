"""
Running one streaming load per table as a batch job.

Each stream uses trigger(availableNow=True): it processes everything that is new
since the last run (tracked in its checkpoint), then stops.
"""

# A stream stops when its input gains a column (Auto Loader with addNewColumns,
# or a Delta source table whose schema changed). Starting it again picks up the
# new schema, so these errors are retried.
SCHEMA_CHANGE_MARKERS = (
    "UnknownFieldException",
    "UNKNOWN_FIELD_EXCEPTION",
    "DELTA_SCHEMA_CHANGED",
    "DELTA_SCHEMA_CHANGE",
)


def is_schema_change(error: Exception) -> bool:
    text = f"{type(error).__name__}: {error}"
    return any(marker in text for marker in SCHEMA_CHANGE_MARKERS)


def run_until_done(start_query, attempts: int = 3) -> None:
    """Start a stream with start_query() and wait for it, restarting it after a schema change."""
    for attempt in range(1, attempts + 1):
        try:
            start_query().awaitTermination()
            return
        except Exception as error:  # noqa: BLE001  (Spark raises several exception types)
            if attempt == attempts or not is_schema_change(error):
                raise
            print(f"  schema changed, restarting (attempt {attempt + 1} of {attempts})")


def run_all(tables, load_one) -> None:
    """Load every table; one failing table doesn't stop the others, but fails the task at the end."""
    failed = []
    for cfg in tables:
        folder = cfg["folder"]
        print(f"{folder}: loading")
        try:
            load_one(cfg)
        except Exception as error:  # noqa: BLE001
            print(f"{folder}: FAILED: {error}")
            failed.append(folder)
    if failed:
        raise RuntimeError(f"tables that failed: {', '.join(failed)}")
