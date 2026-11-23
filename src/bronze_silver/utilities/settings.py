"""
Job settings, passed by the job as --key=value parameters (see
resources/bronze_silver.job.yml).

get_settings() takes anything with .get(key, default): the dict from
parse_args() in the job, or a plain dict in unit tests.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    source_root: str          # where new files land, e.g. /Volumes/.../landing/incoming
    catalog: str
    bronze_schema: str
    silver_schema: str
    clean_source: str         # OFF (default) or MOVE
    archive_root: str         # where processed files go when clean_source = MOVE
    archive_after: str        # how long to wait before moving, e.g. "30 days"
    checkpoint_root: str      # stream checkpoints and Auto Loader schemas

    def source_path(self, folder: str) -> str:
        return f"{self.source_root}/{folder}/"

    def archive_path(self, folder: str) -> str:
        return f"{self.archive_root}/{folder}/"

    def checkpoint_path(self, layer: str, folder: str) -> str:
        return f"{self.checkpoint_root}/{layer}/{folder}"

    def bronze_table(self, folder: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{folder}"

    def silver_table(self, folder: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{folder}"


def parse_args(argv) -> dict:
    """Turn ["--catalog=main", "--source_root=", ...] into {"catalog": "main", "source_root": "", ...}."""
    args = {}
    for arg in argv:
        if not arg.startswith("--") or "=" not in arg:
            raise ValueError(f"expected --key=value, got {arg!r}")
        key, value = arg[2:].split("=", 1)
        args[key] = value
    return args


def get_settings(conf) -> Settings:
    catalog = conf.get("catalog")
    bronze_schema = conf.get("bronze_schema", "bronze")
    # Default landing area: the "landing" volume in the bronze schema.
    # Set source_root / archive_root to use cloud storage (s3://, abfss://) instead.
    landing = f"/Volumes/{catalog}/{bronze_schema}/landing"
    return Settings(
        source_root=(conf.get("source_root", "") or f"{landing}/incoming").rstrip("/"),
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=conf.get("silver_schema", "silver"),
        clean_source=conf.get("clean_source", "OFF").upper(),
        archive_root=(conf.get("archive_root", "") or f"{landing}/archive").rstrip("/"),
        archive_after=conf.get("archive_after", "30 days"),
        checkpoint_root=(conf.get("checkpoint_root", "") or f"{landing}/_checkpoints").rstrip("/"),
    )


def autoloader_options(settings: Settings, folder: str) -> dict:
    """Auto Loader options for one source folder."""
    if settings.checkpoint_root.startswith(settings.source_root + "/"):
        raise ValueError("checkpoint_root must not be inside source_root, or Auto Loader reads its files")
    options = {
        "cloudFiles.format": "parquet",
        # Where Auto Loader keeps the inferred schema (a pipeline does this for you)
        "cloudFiles.schemaLocation": settings.checkpoint_path("bronze", folder),
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
    }
    if settings.clean_source == "MOVE":
        if not settings.archive_root:
            raise ValueError("clean_source=MOVE needs archive_root")
        if settings.archive_root.startswith(settings.source_root + "/"):
            raise ValueError("archive_root must not be inside source_root, or archived files are re-read")
        options["cloudFiles.cleanSource"] = "MOVE"
        options["cloudFiles.cleanSource.moveDestination"] = settings.archive_path(folder)
        options["cloudFiles.cleanSource.retentionDuration"] = settings.archive_after
    return options
