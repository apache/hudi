# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""All Spark operations here use a remote session; the harness bounds this process."""

from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.connect.session import SparkSession as ConnectSession


def main():
    mode, remote, root_arg = sys.argv[1:]
    root = Path(root_arg)
    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        assert isinstance(spark, ConnectSession), "Expected a remote Spark Connect session"
        assert spark.version == "4.1.1", spark.version
        assert spark.conf.get("spark.hudi.test.run.id") == root.name, "Connected to another test's server"
        if mode == "probe":
            assert spark.range(1).count() == 1
            assert spark.conf.get("spark.sql.extensions") == "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
            assert not spark.conf.isModifiable("spark.sql.extensions")
            assert spark.conf.get("spark.sql.catalog.spark_catalog") == "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
            spark.sql("CREATE DATABASE sdp_test").collect()
            print("Remote Spark 4.1.1 server ready with Hudi extension and catalog")
        elif mode == "verify":
            expected = [(i, 100 + i, f"value-{i}") for i in range(1, 4)]
            table = "spark_catalog.sdp_test.events"
            actual = [tuple(row) for row in spark.table(table).select("id", "ts", "value").orderBy("id").collect()]
            assert actual == expected, (actual, expected)
            location = root / "warehouse/sdp_test.db/events"
            assert (location / ".hoodie/hoodie.properties").is_file(), "Missing Hudi table metadata"
            by_path = spark.read.format("hudi").load(location.as_uri()).select("id", "ts", "value").orderBy("id").collect()
            assert [tuple(row) for row in by_path] == expected, by_path
            print(f"Verified catalog and Hudi path reads: {actual}")
        else:
            raise ValueError(f"Unknown client mode: {mode}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
