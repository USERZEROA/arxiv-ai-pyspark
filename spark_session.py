"""SparkSession setup shared by pipeline.py, tests and history_server.py.

Two Windows-only workarounds are applied automatically (nothing changes on
Linux/macOS or in CI):

1. Spark's launcher scripts (spark-class.cmd) break when the install path
   contains non-ASCII characters. We point SPARK_HOME and all project paths at
   the 8.3 short-path form, which is pure ASCII.
2. Hadoop's local file system needs winutils.exe/hadoop.dll on Windows. Instead
   of downloading third-party binaries, we compile a tiny subclass
   (tools/windows/NoChmodLocalFileSystem.java) with the local JDK and register
   it for the file:// scheme.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyspark

IS_WINDOWS = os.name == "nt"
ROOT = Path(__file__).resolve().parent
SHIM_SRC = ROOT / "tools" / "windows" / "NoChmodLocalFileSystem.java"
SHIM_JAR = ROOT / "tools" / "windows" / "build" / "no-chmod-localfs.jar"
SHIM_CLASS = "local.shim.NoChmodLocalFileSystem"


def safe_path(path) -> str:
    """Return an ASCII-safe absolute path string (8.3 short path on Windows).

    The path (or its nearest existing parent) must exist for Windows to return
    a short name, so we create missing directories up front.
    """
    p = Path(path).resolve()
    if not IS_WINDOWS:
        return str(p)
    if p.is_file():
        # shorten only the directory part; keep the (ASCII) file name and extension
        return str(Path(safe_path(p.parent)) / p.name)
    import ctypes

    existing, tail = p, []
    while not existing.exists():
        tail.append(existing.name)
        existing = existing.parent
    buf = ctypes.create_unicode_buffer(32768)
    ctypes.windll.kernel32.GetShortPathNameW(str(existing), buf, len(buf))
    short = Path(buf.value or str(existing))
    for part in reversed(tail):
        short = short / part
    return str(short)


def file_uri(path) -> str:
    return Path(safe_path(path)).as_uri()


def _java_tool(name):
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / (name + ".exe")
        if candidate.exists():
            return str(candidate)
    found = shutil.which(name)
    if not found:
        raise RuntimeError(f"'{name}' not found; a JDK (not just a JRE) is needed on Windows")
    return found


def build_windows_shim() -> str:
    """Compile the local-FS shim once and return the jar path (Windows only)."""
    if SHIM_JAR.exists() and SHIM_JAR.stat().st_mtime >= SHIM_SRC.stat().st_mtime:
        return safe_path(SHIM_JAR)
    jars = Path(pyspark.__file__).parent / "jars"
    hadoop_api = next(jars.glob("hadoop-client-api-*.jar"))
    classes = SHIM_JAR.parent / "classes"
    classes.mkdir(parents=True, exist_ok=True)
    subprocess.run([_java_tool("javac"), "--release", "17", "-cp", safe_path(hadoop_api),
                    "-d", safe_path(classes), safe_path(SHIM_SRC)], check=True)
    subprocess.run([_java_tool("jar"), "cf", safe_path(SHIM_JAR),
                    "-C", safe_path(classes), "."], check=True)
    return safe_path(SHIM_JAR)


def prepare_environment():
    """Set env vars needed before the JVM starts."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if IS_WINDOWS:
        os.environ["SPARK_HOME"] = safe_path(Path(pyspark.__file__).parent)


def build_spark(app_name, master="local[*]", shuffle_partitions=48, driver_memory="16g",
                event_log_dir=None, extra_conf=None):
    """Create a local SparkSession.

    event_log_dir: if given, Spark event logs are written there so the run can
    be replayed later in the Spark History Server.
    """
    from pyspark.sql import SparkSession

    prepare_environment()
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if event_log_dir:
        Path(event_log_dir).mkdir(parents=True, exist_ok=True)
        builder = (builder.config("spark.eventLog.enabled", "true")
                   .config("spark.eventLog.dir", file_uri(event_log_dir)))
    if IS_WINDOWS:
        jar = build_windows_shim()
        builder = (builder.config("spark.driver.extraClassPath", jar)
                   .config("spark.hadoop.fs.file.impl", SHIM_CLASS)
                   .config("spark.hadoop.fs.file.impl.disable.cache", "true"))
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
