"""Lake I/O for Spark jobs: partition-aware parquet reads and writes.

Layout convention (Hive-style, one level): <root>/<partition_col>=<value>/*.parquet
Partition columns are ALSO stored inside the files, so a reader never depends on
directory discovery to recover them.

Two backends, selected automatically (Settings.effective_spark_writer):

  native  Linux/macOS/Docker, or Windows with HADOOP_HOME + winutils. Spark writes the
          dataset in one job, partitioned by a throwaway copy of the partition column so
          the real column is kept inside the files (see write_parquet).

  arrow   Windows hosts without Hadoop native libraries (Hadoop's local FileSystem needs
          winutils.exe/hadoop.dll for directory listing and permission calls). Spark still
          performs ALL transformations; only the file boundary changes: reads pass an
          explicit file list to Spark (which Hadoop can open), and writes collect the
          result through Arrow (`DataFrame.toArrow`) and write parquet with pyarrow.
          This is a dev-host convenience for datasets that fit in driver memory; it is
          never used in the containerised deployment.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Settings, get_settings
from ..logging_utils import get_logger

log = get_logger("spark.io")

# throwaway column used only to drive `partitionBy` in the native writer (see write_parquet)
_PART_COL = "__partition_key"


def _files(root: Path, partition_values: list[str] | None = None) -> list[Path]:
    if not root.exists():
        return []
    if partition_values is None:
        return sorted(p for p in root.rglob("*.parquet"))
    out: list[Path] = []
    for d in sorted(root.iterdir()):
        if d.is_dir() and "=" in d.name and d.name.split("=", 1)[1] in partition_values:
            out.extend(sorted(d.glob("*.parquet")))
    return out


def list_partitions(root: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted(d.name.split("=", 1)[1] for d in root.iterdir() if d.is_dir() and "=" in d.name)


def read_parquet(
    spark: SparkSession,
    root: Path,
    partition_values: list[str] | None = None,
    settings: Settings | None = None,
) -> DataFrame | None:
    """Read a (partitioned) parquet dataset. Returns None when nothing exists yet."""
    settings = settings or get_settings()
    files = _files(root, partition_values)
    if not files:
        return None
    # Always an explicit file list, on both backends: the files carry the partition column
    # themselves, so directory discovery would only re-derive it from the path and risk a
    # duplicate-column clash. Keeping one read path also keeps the platforms identical.
    return spark.read.parquet(*[f.as_posix() for f in files])


def read_files(spark: SparkSession, files: list[Path]) -> DataFrame | None:
    if not files:
        return None
    return spark.read.parquet(*[f.as_posix() for f in files])


def _to_arrow(df: DataFrame) -> pa.Table:
    if hasattr(df, "toArrow"):
        return df.toArrow()
    return pa.Table.from_pandas(df.toPandas(), preserve_index=False)


def write_parquet(
    df: DataFrame,
    root: Path,
    partition_by: str | None = None,
    mode: str = "overwrite",
    settings: Settings | None = None,
) -> dict:
    """Write a DataFrame to <root>.

    mode="overwrite"            replace the whole dataset
    mode="overwrite_partitions" replace only the partitions present in `df` (requires partition_by)
    """
    settings = settings or get_settings()
    backend = settings.effective_spark_writer
    root.mkdir(parents=True, exist_ok=True)
    if mode == "overwrite_partitions" and not partition_by:
        raise ValueError("overwrite_partitions requires partition_by")

    if backend == "native":
        # Write to a staging directory first: the input of a merge is often the very
        # dataset being replaced, and Spark refuses to overwrite a path it is reading.
        staging = root.parent / f"{root.name}__staging"
        shutil.rmtree(staging, ignore_errors=True)
        writer = df.write.mode("overwrite")
        if partition_by:
            nulls = df.filter(df[partition_by].isNull()).count()
            if nulls:
                raise ValueError(
                    f"{root.name}: {nulls} rows have a NULL partition key '{partition_by}'; refusing to drop them silently"
                )
            # `partitionBy` removes the column from the file contents and encodes it only in
            # the directory name, which breaks the module contract: readers that open an
            # explicit file list (partition pruning, and the pyarrow warehouse loader) cannot
            # recover it from the path. Partition by a throwaway copy instead, so the real
            # column survives inside every file; the directories are renamed back below.
            writer = (
                df.withColumn(_PART_COL, F.col(partition_by)).write.mode("overwrite").partitionBy(_PART_COL)
            )
        writer.parquet(staging.as_posix())
        if partition_by:
            for d in list(staging.iterdir()):
                if d.is_dir() and d.name.startswith(f"{_PART_COL}="):
                    d.rename(d.parent / f"{partition_by}={d.name.split('=', 1)[1]}")
        if mode == "overwrite":
            shutil.rmtree(root, ignore_errors=True)
            staging.rename(root)
        else:
            root.mkdir(parents=True, exist_ok=True)
            for d in staging.iterdir():
                if d.is_dir() and "=" in d.name:
                    target = root / d.name
                    shutil.rmtree(target, ignore_errors=True)
                    d.rename(target)
            shutil.rmtree(staging, ignore_errors=True)
        return {"backend": backend, "path": str(root)}

    table = _to_arrow(df)
    if not partition_by:
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, root / "part-00000.parquet", compression="snappy")
        return {"backend": backend, "path": str(root), "rows": table.num_rows, "partitions": 0}

    if mode == "overwrite" and root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    import pyarrow.compute as pc

    col = table.column(partition_by)
    if col.null_count:
        raise ValueError(
            f"{root.name}: {col.null_count} rows have a NULL partition key '{partition_by}'; refusing to drop them silently"
        )
    values = sorted(set(col.to_pylist()))

    for v in values:
        part = table.filter(pc.equal(col, v))
        d = root / f"{partition_by}={v}"
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(part, d / "part-00000.parquet", compression="snappy")
    log.info(
        "wrote partitioned dataset",
        path=str(root),
        partitions=len(values),
        rows=table.num_rows,
        backend=backend,
    )
    return {"backend": backend, "path": str(root), "rows": table.num_rows, "partitions": len(values)}


def to_arrow(df: DataFrame) -> pa.Table:
    return _to_arrow(df)
