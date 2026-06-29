#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

# Spark 4 notebook helpers. Copied into the notebook home as utils.py when
# building the apachehudi/spark4-hudi (Spark 4) image.

import os
import urllib.request

from pyspark.sql import SparkSession
from IPython.display import HTML, display as display_html

_spark = None

# Default number of rows to show in display()
DEFAULT_DISPLAY_ROWS = 100

# Reused HTML/CSS for DataFrame display (avoids string rebuild on every call)
_DISPLAY_TABLE_CSS = """
<style>
    .dataframe {
        border-radius: 0.5rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        overflow-x: auto;
        border: 1px solid #e2e8f0;
    }
    .dataframe th {
        background-color: #f1f5f9;
        color: #1f2937;
        font-weight: 600;
        padding: 0.75rem 1.5rem;
        text-align: left;
        border-bottom: 2px solid #e2e8f0;
    }
    .dataframe td {
        padding: 0.75rem 1.5rem;
        border-bottom: 1px solid #e2e8f0;
    }
    .dataframe tr:nth-child(even) {
        background-color: #f8fafc;
    }
    .dataframe tr:hover {
        background-color: #e2e8f0;
        transition: background-color 0.2s ease-in-out;
    }
</style>
"""


def get_spark_session(
    app_name="Hudi-Notebooks",
    log_level="WARN",
    hudi_version=None,
    include_lance=False,
):
    """
    Initialize a SparkSession (singleton).

    Connects to the in-container Spark standalone master started by entrypoint.sh.
    The master URL, driver memory (4g) and executor memory (4g) are configured in
    $SPARK_HOME/conf/spark-defaults.conf.

    Parameters:
    - app_name (str): Optional name for the Spark application.
    - log_level (str): Log level for Spark (DEBUG, INFO, WARN, ERROR). Defaults to WARN.
    - hudi_version (str): Hudi bundle version. Defaults to the HUDI_VERSION baked into
      the image (1.1.1 on the Spark 4 image).
    - include_lance (bool): When True, also resolve the Lance Spark bundle and add it to
      the classpath.

    Returns:
    - SparkSession object
    """
    global _spark

    if _spark is not None:
        return _spark

    if hudi_version is None:
        hudi_version = os.getenv("HUDI_VERSION")

    hudi_home = os.getenv("HUDI_HOME", "/opt/hudi")
    spark_version = os.getenv("SPARK_VERSION", "4.0.2")
    spark_minor_version = ".".join(spark_version.split(".")[:2])
    scala_version = os.getenv("SCALA_VERSION", "2.13")
    bundle_name = f"hudi-spark{spark_minor_version}-bundle_{scala_version}"
    bundle_jar = f"{bundle_name}-{hudi_version}.jar"

    # Resolve the Hudi bundle to a local path. The image pre-downloads it under
    # $HUDI_HOME; if it is missing, fetch it from Maven Central now.
    hudi_local_jar = os.path.join(hudi_home, hudi_version, bundle_jar)
    if not os.path.exists(hudi_local_jar):
        os.makedirs(os.path.dirname(hudi_local_jar), exist_ok=True)
        hudi_jar_url = (
            f"https://repo1.maven.org/maven2/org/apache/hudi/"
            f"{bundle_name}/{hudi_version}/{bundle_jar}"
        )
        print(f"Hudi bundle not found at {hudi_local_jar}; downloading {hudi_jar_url} ...")
        urllib.request.urlretrieve(hudi_jar_url, hudi_local_jar)

    classpath_jars = [hudi_local_jar]

    if include_lance:
        lance_version = "0.5.0"
        lance_home = "/opt/lance"
        lance_bundle = f"lance-spark-bundle-{spark_minor_version}_{scala_version}"
        lance_jar = f"{lance_bundle}-{lance_version}.jar"
        lance_local_jar = os.path.join(lance_home, lance_version, lance_jar)
        if not os.path.exists(lance_local_jar):
            os.makedirs(os.path.dirname(lance_local_jar), exist_ok=True)
            lance_jar_url = (
                f"https://repo1.maven.org/maven2/org/lance/"
                f"{lance_bundle}/{lance_version}/{lance_jar}"
            )
            print(f"Lance bundle not found at {lance_local_jar}; downloading {lance_jar_url} ...")
            urllib.request.urlretrieve(lance_jar_url, lance_local_jar)
        classpath_jars.append(lance_local_jar)

    extraclasspath = ":".join(classpath_jars)
    _spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.extraClassPath", extraclasspath)
        .config("spark.executor.extraClassPath", extraclasspath)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .enableHiveSupport()
        .getOrCreate()
    )

    _spark.sparkContext.setLogLevel(log_level)
    print(
        f"SparkSession started with app name: {app_name}, "
        f"Spark version: {spark_version}, Hudi version: {hudi_version}"
    )

    return _spark


def stop_spark_session():
    """Stop the global SparkSession and clear the singleton."""
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None
        print("SparkSession stopped successfully.")


def _require_spark():
    """Return the active SparkSession, raising a clear error if it isn't initialized yet."""
    if _spark is None:
        raise RuntimeError("SparkSession not initialized. Call get_spark_session() first.")
    return _spark


def ls(base_path):
    """
    List files or directories at the given MinIO S3 path.

    Args:
        base_path: Path starting with 's3a://' (e.g. s3a://warehouse/hudi_table/).
    """
    _require_spark()

    try:
        hadoop_conf = _spark._jsc.hadoopConfiguration()
        p = _spark._jvm.org.apache.hadoop.fs.Path(base_path)
        fs = p.getFileSystem(hadoop_conf)
        if not fs.exists(p):
            print(f"Path does not exist: {base_path}")
            return []
        status = fs.listStatus(p)
        files = [str(file.getPath()) for file in status]
        for f in files:
            print(f)
    except Exception as e:
        print(f"Exception occurred while listing files from path {base_path}", e)


def drop_table(table_name: str = None, table_path: str = None):
    _require_spark()
    try:
        _spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✓ Table '{table_name}' dropped successfully.")
    except Exception as e:
        raise RuntimeError(f"Failed to drop table '{table_name}': {e}") from e
    try:
        _path = _spark._jvm.org.apache.hadoop.fs.Path(table_path)
        _path.getFileSystem(_spark._jsc.hadoopConfiguration()).delete(_path, True)
    except Exception as e:
        raise RuntimeError(f"Failed to delete a files from path '{table_path}': {e}") from e

def create_table(sql_query: str, table_name: str, table_path: str = None, describe: bool = True) -> None:
    _require_spark()
    drop_table(table_name, table_path)
    try:
        _spark.sql(sql_query)
        print(f"✓ Table '{table_name}' created successfully.")
        if describe:
            desc_table(table_name)
    except Exception as e:
        raise RuntimeError(
            f"Failed to create table '{table_name}': {e}"
        ) from e


def desc_table(table_name: str) -> None:
    _require_spark()
    print(f"\nSchema for '{table_name}':\n")
    display(
        _spark.sql(f"DESCRIBE EXTENDED {table_name}")
    )


def get_count(table_name: str = None) -> int:
    _require_spark()
    return (
        _spark.table(table_name)
        .count()
    )


def insert_data(insert_query: str, table_name: str, show_count: bool = True) -> None:
    _require_spark()
    try:
        _spark.sql(insert_query)
        message = f"✓ Data inserted successfully into '{table_name}'."
        if show_count:
            message += f" Total Records: {get_count(table_name):,}"
        print(message)
    except Exception as e:
        raise RuntimeError(
            f"Failed to insert data into '{table_name}': {e}"
        ) from e


def display(df, num_rows=None):
    """
    Display a PySpark DataFrame as a formatted HTML table (Databricks-style).

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to display.
        num_rows (int): Number of rows to show. Defaults to DEFAULT_DISPLAY_ROWS (100).
    """
    if num_rows is None:
        num_rows = DEFAULT_DISPLAY_ROWS

    try:
        pandas_df = df.limit(num_rows).toPandas()
    except Exception as e:
        print(f"Error converting DataFrame to Pandas: {e}")
        return

    html_table = pandas_df.to_html(
        index=False,
        classes=[
            "w-full",
            "border-collapse",
            "text-sm",
            "text-gray-900",
            "dark:text-white",
        ],
    )
    display_html(HTML(_DISPLAY_TABLE_CSS + html_table))
