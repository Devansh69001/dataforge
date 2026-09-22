"""SparkSession factory with the platform quirks handled in one place.

* SPARK_MASTER          local[*] by default, or spark://spark-master:7077 in Docker
* Java 17+/21/23        `-Djava.security.manager=allow` keeps Hadoop's UserGroupInformation
                        working on JDK 23 (Subject.getSubject was hard-deprecated).
* Windows               JDK loopback pipes fail when the temp path contains spaces, so
                        JVM temp dirs are redirected to a short, space-free path.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from ..config import Settings, get_settings
from ..logging_utils import get_logger

log = get_logger("spark")

_session: SparkSession | None = None


def _shell_safe(path: str) -> str:
    """Spark launches the interpreter through a shell; on Windows a path with spaces must
    be passed in its short (8.3) form."""
    if os.name != "nt" or " " not in path:
        return path
    import ctypes

    buf = ctypes.create_unicode_buffer(512)
    if ctypes.windll.kernel32.GetShortPathNameW(path, buf, 512):
        return buf.value
    return path


def get_spark(app_name: str = "dataforge", settings: Settings | None = None) -> SparkSession:
    global _session
    if _session is not None:
        return _session
    settings = settings or get_settings()
    tmp = settings.effective_spark_tmp_dir
    Path(tmp).mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("TMP", tmp)
    os.environ.setdefault("TEMP", tmp)
    # Python workers (createDataFrame from local rows, UDFs) must use this interpreter,
    # not whatever "python3" resolves to on the host
    os.environ.setdefault("PYSPARK_PYTHON", _shell_safe(sys.executable))
    java_opts = f"-Djava.io.tmpdir={tmp} -Djdk.net.unixdomain.tmpdir={tmp} -Djava.security.manager=allow"

    builder = (
        SparkSession.builder.master(settings.spark_master)
        .appName(app_name)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        # Spark 4 enables ANSI mode by default (casts throw). Silver relies on
        # "invalid -> NULL -> quarantined by rule", so keep the permissive semantics.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .config("spark.local.dir", f"{tmp}/spark-local")
        .config("spark.ui.enabled", os.environ.get("SPARK_UI_ENABLED", "false"))
    )
    _session = builder.getOrCreate()
    _session.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    log.info(
        "spark session started",
        master=settings.spark_master,
        version=_session.version,
        writer=settings.effective_spark_writer,
        hadoop_natives=settings.hadoop_natives_available,
    )
    return _session


def stop_spark() -> None:
    global _session
    if _session is not None:
        _session.stop()
        _session = None
