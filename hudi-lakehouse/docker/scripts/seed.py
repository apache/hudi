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

"""Initialize missing demo tables; preserve existing tables and all user changes."""
import argparse
import signal
import sys

DEMO_TABLES = {
    'drivers': """CREATE TABLE default.drivers USING hudi
        TBLPROPERTIES (type = 'cow', primaryKey = 'driver_id') AS
        SELECT * FROM VALUES
          ('driver_1', 'Ananya', 'chennai', 'sedan', DATE '2024-01-01'),
          ('driver_2', 'Alex', 'san_francisco', 'sedan', DATE '2024-01-01'),
          ('driver_3', 'Ana', 'sao_paulo', 'sedan', DATE '2024-01-01')
        AS drivers(driver_id, driver_name, home_city, vehicle_type, onboarded_date)""",
    'trips': """CREATE TABLE default.trips USING hudi PARTITIONED BY (trip_date)
        TBLPROPERTIES (type = 'cow', primaryKey = 'trip_id', preCombineField = 'ts') AS
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
               unix_millis(CAST(date_sub(current_date(), 1) AS TIMESTAMP)) AS ts,
               date_sub(current_date(), 1) AS trip_date
        FROM range(9)""",
}


def seed(spark):
    for table, create_sql in DEMO_TABLES.items():
        if spark.catalog.tableExists(f'spark_catalog.default.{table}'):
            print(f'Preserving existing table default.{table}; skipping sample initialization.', flush=True)
            continue
        # Create and populate together rather than leaving an empty table between
        # separate CREATE and INSERT operations. Never repair an existing table.
        spark.sql(create_sql).collect()
        verify(spark, (table,))


def verify(spark, expected_tables=('drivers', 'trips')):
    assert spark.conf.get("spark.sql.catalog.spark_catalog") == (
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    )
    tables = {row.tableName for row in spark.sql("SHOW TABLES IN spark_catalog.default").collect()}
    assert set(expected_tables) <= tables, f'Missing demo tables: {set(expected_tables) - tables}'
    for table in expected_tables:
        expected_count = 3 if table == 'drivers' else 9
        actual_count = spark.sql(f'SELECT count(*) FROM default.{table}').collect()[0][0]
        assert actual_count == expected_count, (
            f'default.{table} no longer matches the demo dataset: '
            f'expected {expected_count} rows, found {actual_count}. Data was not modified.'
        )
    if 'trips' in expected_tables:
        rows = spark.sql("""SELECT city, SUM(fare_amount + tip_amount) AS gross_bookings
            FROM default.trips WHERE trip_status = 'completed' GROUP BY city ORDER BY city""").collect()
        actual = [(row.city, str(row.gross_bookings)) for row in rows]
        assert actual == [("chennai", "33.00"), ("san_francisco", "55.00"), ("sao_paulo", "77.00")], (
            f'default.trips totals no longer match the demo dataset: {actual}. Data was not modified.'
        )
        assert spark.sql("SHOW PARTITIONS default.trips").collect(), 'Missing default.trips partitions'
    print('PASS: sample Hudi data in ' + ', '.join(expected_tables), flush=True)


def main():
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()

    def timed_out(signum, frame):
        raise TimeoutError('Seed exceeded its 600-second timeout')

    spark = None
    signal.signal(signal.SIGALRM, timed_out)
    signal.alarm(600)
    try:
        spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
        if args.verify_only:
            verify(spark)
        else:
            seed(spark)
    except TimeoutError as error:
        # Bound cancellation/cleanup too, even if the Connect server is unreachable.
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.alarm(15)
        print(f'{error}; requesting cancellation. Run lakehouse.sh down before retrying.', file=sys.stderr)
        if spark is not None:
            try:
                spark.interruptAll()
            except Exception as cancellation_error:
                print(f'Could not confirm cancellation: {cancellation_error}', file=sys.stderr)
        raise SystemExit(124)
    finally:
        if spark is not None:
            spark.stop()
        signal.alarm(0)


if __name__ == '__main__':
    main()
