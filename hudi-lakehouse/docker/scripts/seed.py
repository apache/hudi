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

"""Seed the demo tables through Spark Connect; retries never replace existing rows."""
import argparse
import signal

from pyspark.sql import SparkSession


def seed(spark):
    spark.sql("""CREATE TABLE IF NOT EXISTS default.drivers (
        driver_id STRING, driver_name STRING, home_city STRING,
        vehicle_type STRING, onboarded_date DATE
    ) USING hudi TBLPROPERTIES (type = 'cow', primaryKey = 'driver_id')""").collect()
    spark.sql("""CREATE TABLE IF NOT EXISTS default.trips (
        trip_id STRING, rider_id STRING, driver_id STRING, city STRING,
        trip_status STRING, fare_amount DECIMAL(10,2), tip_amount DECIMAL(10,2),
        distance_km DOUBLE, pickup_ts TIMESTAMP, dropoff_ts TIMESTAMP, trip_date DATE
    ) USING hudi PARTITIONED BY (trip_date)
    TBLPROPERTIES (type = 'cow', primaryKey = 'trip_id', preCombineField = 'pickup_ts')""").collect()
    spark.sql("""INSERT INTO default.drivers
        SELECT source.* FROM (
        SELECT * FROM VALUES
          ('driver_1', 'Ananya', 'chennai', 'sedan', DATE '2024-01-01'),
          ('driver_2', 'Alex', 'san_francisco', 'sedan', DATE '2024-01-01'),
          ('driver_3', 'Ana', 'sao_paulo', 'sedan', DATE '2024-01-01')
        AS drivers(driver_id, driver_name, home_city, vehicle_type, onboarded_date)
    ) source LEFT ANTI JOIN default.drivers target
        ON target.driver_id = source.driver_id""").collect()
    # Stable keys make reruns safe, including a retry after only one table committed.
    # First-run dates are yesterday; subsequent installs preserve the original data.
    spark.sql("""INSERT INTO default.trips
        SELECT source.* FROM (
        SELECT concat('trip_', CAST(id AS STRING)) AS trip_id,
               concat('rider_', CAST(id AS STRING)) AS rider_id,
               concat('driver_', CAST(CAST(id / 3 AS INT) + 1 AS STRING)) AS driver_id,
               element_at(array('chennai', 'san_francisco', 'sao_paulo'),
                          CAST(id / 3 AS INT) + 1) AS city,
               CASE WHEN id % 3 = 2 THEN 'cancelled' ELSE 'completed' END AS trip_status,
               CAST(10 * (CAST(id / 3 AS INT) + 1) + (id % 3) * 10 AS DECIMAL(10,2)) AS fare_amount,
               CAST(CAST(id / 3 AS INT) + 1 + id % 3 AS DECIMAL(10,2)) AS tip_amount,
               CAST(5 + id AS DOUBLE) AS distance_km,
               CAST(date_sub(current_date(), 1) AS TIMESTAMP) AS pickup_ts,
               CAST(date_sub(current_date(), 1) AS TIMESTAMP) + INTERVAL 15 MINUTES AS dropoff_ts,
               date_sub(current_date(), 1) AS trip_date
        FROM range(9)
    ) source LEFT ANTI JOIN default.trips target
        ON target.trip_id = source.trip_id""").collect()


def verify(spark):
    assert spark.conf.get("spark.sql.catalog.spark_catalog") == (
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    )
    tables = {row.tableName for row in spark.sql("SHOW TABLES IN spark_catalog.default").collect()}
    assert {"trips", "drivers"} <= tables, tables
    assert spark.sql("SELECT count(*) FROM default.trips").collect()[0][0] == 9
    assert spark.sql("SELECT count(*) FROM default.drivers").collect()[0][0] == 3
    rows = spark.sql("""SELECT city, SUM(fare_amount + tip_amount) AS gross_bookings
        FROM default.trips WHERE trip_status = 'completed' GROUP BY city ORDER BY city""").collect()
    actual = [(row.city, str(row.gross_bookings)) for row in rows]
    assert actual == [("chennai", "33.00"), ("san_francisco", "55.00"), ("sao_paulo", "77.00")], actual
    assert spark.sql("SHOW PARTITIONS default.trips").collect()
    print("PASS: HMS tables, partitions and sample Hudi data: " + str(actual), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    signal.alarm(600)
    spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
    try:
        if not args.verify_only:
            seed(spark)
        verify(spark)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
