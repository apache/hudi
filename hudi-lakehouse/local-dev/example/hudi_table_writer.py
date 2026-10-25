# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example Hudi write job for the hudi-lakehouse local-dev environment.

Writes a partitioned Copy-on-Write table to object storage and registers it
in the Hive Metastore via Hudi's hive-sync (thrift/hms mode), so it is
immediately queryable from the hudi-trino chart.

All endpoints/knobs come from environment variables (set in spark-app.yaml);
copy this file together with spark-app.yaml to run your own jobs.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE_NAME = os.environ.get("TABLE_NAME", "trips")
DATABASE = os.environ.get("DATABASE", "default")
BASE_PATH = os.environ.get("BASE_PATH", f"s3a://warehouse/hudi/{TABLE_NAME}")
HMS_URI = os.environ.get("HMS_URI", "thrift://hive-metastore:9083")
NUM_ROWS = int(os.environ.get("NUM_ROWS", "100"))

hudi_options = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "trip_id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "city",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    # The in-repo Trino plugin currently reads with a Hudi 1.0.x runtime,
    # which understands table versions 6 and 8 only. Pin the write version
    # and disable auto-upgrade so the table stays readable from Trino.
    "hoodie.write.table.version": os.environ.get("WRITE_TABLE_VERSION", "8"),
    "hoodie.write.auto.upgrade": "false",
    # Register the table in the Hive Metastore (hive-sync ships inside the
    # Hudi Spark bundle; hms mode talks thrift directly, no HiveServer2).
    "hoodie.datasource.meta.sync.enable": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.metastore.uris": HMS_URI,
    "hoodie.datasource.hive_sync.database": DATABASE,
    "hoodie.datasource.hive_sync.table": TABLE_NAME,
    "hoodie.datasource.hive_sync.partition_fields": "city",
    "hoodie.datasource.hive_sync.partition_extractor_class":
        "org.apache.hudi.hive.MultiPartKeysValueExtractor",
}

spark = SparkSession.builder.appName(f"hudi-write-{TABLE_NAME}").getOrCreate()

df = (
    spark.range(NUM_ROWS)
    .select(
        F.col("id").cast("string").alias("trip_id"),
        F.current_timestamp().alias("ts"),
        (F.rand(seed=42) * 100).alias("fare"),
        F.element_at(
            F.array(F.lit("san_francisco"), F.lit("chennai"), F.lit("sao_paulo")),
            (F.col("id") % 3 + 1).cast("int"),
        ).alias("city"),
    )
)

df.write.format("hudi").options(**hudi_options).mode("overwrite").save(BASE_PATH)

# Second commit: upsert a slice with doubled fares to exercise mutation.
updates = df.limit(10).withColumn("fare", F.col("fare") * 2)
updates.write.format("hudi").options(**hudi_options).mode("append").save(BASE_PATH)

count = spark.read.format("hudi").load(BASE_PATH).count()
assert count == NUM_ROWS, f"expected {NUM_ROWS} rows after upsert, got {count}"
print(f"DEMO_OK rows={count} table={DATABASE}.{TABLE_NAME} path={BASE_PATH}")

spark.stop()
