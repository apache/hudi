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

"""Live acceptance helper: run only against a disposable installation."""
import argparse
import signal


def main():
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['modify', 'verify'])
    parser.add_argument('--connect-uri', default='sc://spark-connect:15002')
    args = parser.parse_args()
    signal.alarm(180)
    spark = SparkSession.builder.remote(args.connect_uri).getOrCreate()

    def sql(query):
        return spark.sql(query).collect()

    try:
        assert spark.table('default.trips').schema['ts'].dataType.simpleString() == 'bigint'
        if args.action == 'modify':
            sql("UPDATE default.trips SET fare_amount = 123.45 WHERE trip_id = 'trip_0'")
            sql("DELETE FROM default.trips WHERE trip_id = 'trip_1'")
            sql('DELETE FROM default.drivers')

        assert sql('SELECT count(*) FROM default.trips')[0][0] == 8
        assert str(sql("SELECT fare_amount FROM default.trips WHERE trip_id = 'trip_0'")[0][0]) == '123.45'
        assert not sql("SELECT trip_id FROM default.trips WHERE trip_id = 'trip_1'")
        assert sql('SELECT count(*) FROM default.drivers')[0][0] == 0
        print(f'PASS: user data lifecycle ({args.action})', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
