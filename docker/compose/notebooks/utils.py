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

HUDI_JAR = os.environ.get("HUDI_SPARK_BUNDLE")
if not HUDI_JAR:
        raise EnvironmentError("HUDI_SPARK_BUNDLE environment variable not set")

HADOOP_S3_JAR = "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.734.jar"

ALL_JARS = f"{HUDI_JAR},{HADOOP_S3_JAR}"

def get_spark_session(app_name="Hudi-Notebooks"):
    """
    Initialize a SparkSession with Hudi extensions.
    
    Parameters:
    - app_name (str): Optional name for the Spark application.
    
    Returns:
    - SparkSession object
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", ALL_JARS) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9090") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.defaultFS", "s3a://warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"SparkSession started with app name: {app_name}")
    return spark

# Initialize Spark globally so other functions can use it
spark = get_spark_session()

# S3 Utility Function
from py4j.java_gateway import java_import

def ls(path):
    """
    List files or directories at the given MinIO S3 path.
    
    Example: ls("s3a://warehouse/hudi_table/")
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    p = spark._jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(p):
        print(f"Path does not exist: {path}")
        return []

    status = fs.listStatus(p)
    files = [str(file.getPath()) for file in status]
    for f in files:
        print(f)


# Display Utility Function

from IPython.display import display as display_html, HTML

def display(df, num_rows=100, truncate=False):
    """
    Displays a PySpark DataFrame in a formatted HTML table.

    This function is designed to mimic the Databricks 'display' function by
    presenting a sample of the DataFrame in a clean, readable table format
    using HTML and Tailwind CSS for styling.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to display.
        num_rows (int): The number of rows to show. Defaults to 100.
        truncate (bool): Whether to truncate the output of long strings.
                         This argument is not currently used for simplicity
                         but can be added for more advanced functionality.
    """
    
    # Collect a limited number of rows to the driver as a Pandas DataFrame
    try:
        pandas_df = df.limit(num_rows).toPandas()
    except Exception as e:
        print(f"Error converting DataFrame to Pandas: {e}")
        return

    # Use pandas to_html to get a clean table, then add custom styling.
    # The styling uses Tailwind CSS classes for a clean, modern look.
    html_table = pandas_df.to_html(index=False, classes=[
        "w-full", "border-collapse", "text-sm", "text-gray-900", "dark:text-white"
    ])

    # We are adding custom styling here to make it look like a well-formatted blog post table.
    custom_css = """
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
    
    # Display the final HTML
    display_html(HTML(custom_css + html_table))

# Example usage with your data
# display(inputDF)