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
"""Read-only, opt-in SQL/catalog/table-data readiness for Spark Connect."""
import argparse
import re
import sys

from pyspark.sql import SparkSession


def identifier(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise argparse.ArgumentTypeError('invalid SQL identifier')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema', required=True, type=identifier)
    parser.add_argument('--table', required=True, type=identifier)
    parser.add_argument('--column', required=True, type=identifier)
    args = parser.parse_args()
    spark = None
    stage = 'connection'
    try:
        spark = SparkSession.builder.remote('sc://127.0.0.1:15002').getOrCreate()
        stage = 'sql'
        if spark.sql('SELECT 1').collect()[0][0] != 1:
            raise ValueError('unexpected SQL result')
        print('PASS: sql', flush=True)
        stage = 'catalog'
        if not any(row[0] == args.schema for row in spark.sql('SHOW DATABASES').collect()):
            raise ValueError('catalog schema not found')
        print('PASS: catalog', flush=True)
        stage = 'storage'
        rows = spark.sql(f'SELECT `{args.column}` FROM `{args.schema}`.`{args.table}` '
                         f'WHERE `{args.column}` IS NOT NULL LIMIT 1').collect()
        if not rows:
            raise ValueError('probe table has no non-null probe value')
        print('PASS: storage', flush=True)
        return 0
    except Exception as exc:
        print(f'FAIL: {stage}: {exc}', file=sys.stderr, flush=True)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    raise SystemExit(main())
