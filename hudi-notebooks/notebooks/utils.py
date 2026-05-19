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

import os

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
    hudi_version="1.0.2",
):
    """
    Initialize a SparkSession (singleton).

    Parameters:
    - app_name (str): Optional name for the Spark application.
    - log_level (str): Log level for Spark (DEBUG, INFO, WARN, ERROR). Defaults to WARN.
    - hudi_version (str): Hudi bundle version. Defaults to 1.0.2.

    Returns:
    - SparkSession object
    """
    global _spark

    if _spark is not None:
        return _spark

    hudi_home = os.getenv("HUDI_HOME")
    spark_version = os.getenv("SPARK_VERSION", "3.5.7")
    spark_minor_version = ".".join(spark_version.split(".")[:2])
    scala_version = os.getenv("SCALA_VERSION", "2.12")
    hudi_jar_path = f"hudi-spark{spark_minor_version}-bundle_{scala_version}-{hudi_version}.jar"
    hudi_packages = f"org.apache.hudi:hudi-spark{spark_minor_version}-bundle_{scala_version}:{hudi_version}"
    hudi_jars = os.path.join(hudi_home, hudi_version, hudi_jar_path)
    use_jars = os.path.exists(hudi_jars)
    conf_param = "spark.jars" if use_jars else "spark.jars.packages"
    conf_value = hudi_jars if use_jars else hudi_packages

    _spark = (
        SparkSession.builder.appName(app_name)
        .config(conf_param, conf_value)
        .config("spark.hadoop.fs.defaultFS", "s3a://warehouse")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .enableHiveSupport()
        .getOrCreate()
    )

    _spark.sparkContext.setLogLevel(log_level)
    print(f"SparkSession started with app name: {app_name}, log level: {log_level}")

    return _spark


def stop_spark_session():
    """Stop the global SparkSession and clear the singleton."""
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None
        print("SparkSession stopped successfully.")


def ls(base_path):
    """
    List files or directories at the given MinIO S3 path.

    Args:
        base_path: Path starting with 's3a://' (e.g. s3a://warehouse/hudi_table/).
    """
    if not base_path.startswith("s3a://"):
        raise ValueError("Path must start with 's3a://'")

    global _spark
    if _spark is None:
        raise RuntimeError("SparkSession not initialized. Call get_spark_session() first.")

    try:
        hadoop_conf = _spark._jsc.hadoopConfiguration()
        fs = _spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        p = _spark._jvm.org.apache.hadoop.fs.Path(base_path)
        if not fs.exists(p):
            print(f"Path does not exist: {base_path}")
            return []
        status = fs.listStatus(p)
        files = [str(file.getPath()) for file in status]
        for f in files:
            print(f)
    except Exception as e:
        print(f"Exception occurred while listing files from path {base_path}", e)


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
