"""Start the Spark History Server on the event logs in spark-events/.

The live Spark UI (http://localhost:4040) disappears when pipeline.py ends.
The event logs written during the run can be replayed here instead:

    python history_server.py          # then open http://localhost:18080
    (Ctrl+C to stop)

Uses the Spark distribution bundled with the pyspark package. On Windows the
same non-ASCII-path and winutils workarounds as spark_session.py are applied.
"""

import os
import subprocess
from pathlib import Path

import pyspark

from spark_session import IS_WINDOWS, SHIM_CLASS, ROOT, build_windows_shim, file_uri, prepare_environment

EVENT_LOG_DIR = ROOT / "spark-events"


def main(port=18080):
    if not EVENT_LOG_DIR.exists() or not any(EVENT_LOG_DIR.iterdir()):
        raise SystemExit("spark-events/ is empty - run pipeline.py first")
    prepare_environment()
    spark_home = Path(os.environ.get("SPARK_HOME") or Path(pyspark.__file__).parent)
    opts = [f"-Dspark.history.fs.logDirectory={file_uri(EVENT_LOG_DIR)}",
            f"-Dspark.history.ui.port={port}"]
    env = dict(os.environ)
    if IS_WINDOWS:
        env["SPARK_DAEMON_CLASSPATH"] = build_windows_shim()
        opts.append(f"-Dspark.hadoop.fs.file.impl={SHIM_CLASS}")
        launcher = [str(spark_home / "bin" / "spark-class.cmd")]
    else:
        launcher = [str(spark_home / "bin" / "spark-class")]
    env["SPARK_HISTORY_OPTS"] = " ".join(opts)
    print(f"Spark History Server on http://localhost:{port}  (logs: spark-events/)")
    subprocess.run(launcher + ["org.apache.spark.deploy.history.HistoryServer"], env=env, check=True)


if __name__ == "__main__":
    main()
