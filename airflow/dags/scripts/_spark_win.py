# scripts/_spark_win.py
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession


def spark_path(p: str | Path) -> str:
    """Return a Windows-safe Spark-readable path string."""
    p = Path(p).expanduser()
    # don't force resolve() if path might be relative to project; but it's fine here
    try:
        p = p.resolve()
    except Exception:
        pass
    return str(p).replace("\\", "/")


# Backward compatible name used by some scripts
def _as_file_uri(p: str | Path) -> str:
    return spark_path(p)


def _ensure_hadoop_env(hadoop_home: str | None = None):
    """Ensure Spark can find winutils.exe / hadoop.dll if present."""
    hh = hadoop_home or os.environ.get("HADOOP_HOME") or os.environ.get("hadoop_home_dir")
    if not hh:
        default_hh = Path.home() / "hadoop"
        if default_hh.exists():
            hh = str(default_hh)

    if hh:
        hh = str(Path(hh).resolve())  # avoids 'C:hadoop' (missing backslash)
        os.environ["HADOOP_HOME"] = hh
        os.environ["hadoop_home_dir"] = hh

        bin_dir = str(Path(hh) / "bin")
        path = os.environ.get("PATH", "")
        if bin_dir not in path.split(";"):
            os.environ["PATH"] = bin_dir + ";" + path


def get_spark(app_name: str,
              hadoop_home: str | None = None,
              driver_memory: str = "2g",
              shuffle_partitions: int = 16) -> SparkSession:
    _ensure_hadoop_env(hadoop_home=hadoop_home)

    tmp_base = Path(os.environ.get("SPARK_LOCAL_DIR", Path(tempfile.gettempdir()) / "spark_tmp"))
    tmp_dir = tmp_base
    tmp_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.local.dir", str(tmp_dir))
        .config("spark.sql.warehouse.dir", str(tmp_dir / "warehouse"))
        .getOrCreate()
    )
    return spark




if __name__ == "__main__":
    s = get_spark("diag")
    print("Spark:", s.version)
    try:
        print("Hadoop:", s.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
    except Exception:
        print("Hadoop: None")
    s.stop()
