"""Single place that builds the SparkSession. Swap master/jar handling here when moving to a cluster."""

from __future__ import annotations

import logging
import os
import sys
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

JDBC_VERSION = "42.7.4"
JDBC_JAR = Path(__file__).resolve().parents[2] / "lib" / f"postgresql-{JDBC_VERSION}.jar"
JDBC_URL = f"https://repo1.maven.org/maven2/org/postgresql/postgresql/{JDBC_VERSION}/postgresql-{JDBC_VERSION}.jar"


def ensure_jdbc_jar() -> Path:
    if not JDBC_JAR.exists():
        JDBC_JAR.parent.mkdir(parents=True, exist_ok=True)
        log.info("downloading %s", JDBC_URL)
        urllib.request.urlretrieve(JDBC_URL, JDBC_JAR)
    return JDBC_JAR


# ponytail: local[*] on a 20-core Windows box launches 20 python workers at once and they miss the
# 15s handshake; 4 cores is plenty for a run's few thousand rows. Cluster master URL overrides this.
def get_spark(app_name: str = "eventgraph", master: str = "local[4]") -> SparkSession:
    # ponytail: jar goes on the driver classpath, not spark.jars — spark.jars needs winutils on Windows
    jar = str(ensure_jdbc_jar())
    # workers must run the same interpreter as the driver (Windows resolves `python3` to a Store alias)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        # the machine's hostname resolves to host.docker.internal; workers dial 127.0.0.1
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraClassPath", jar)
        .config("spark.executor.extraClassPath", jar)
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )


def read_extracted(spark: SparkSession, path: Path) -> DataFrame:
    """Parquet produced by stage B. Windows without winutils cannot use Hadoop's LocalFileSystem,
    so there we load with pyarrow and hand the rows to Spark; elsewhere plain spark.read.parquet."""
    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        # ponytail: driver-side load; fine for a run's few thousand rows, not for millions
        table = pq.read_table(str(path))
        schema = T.StructType(
            [
                T.StructField("run_document_id", T.LongType()),
                T.StructField("extraction", T.StringType()),
                T.StructField("tokens_in", T.LongType()),
                T.StructField("tokens_out", T.LongType()),
                T.StructField("elapsed_ms", T.LongType()),
            ]
        )
        return spark.createDataFrame(table.to_pylist(), schema)
    return spark.read.parquet(str(path.resolve()))
