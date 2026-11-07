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

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.active()


@dp.table(name="events", format="hudi", table_properties={
    "type": "cow", "primaryKey": "id", "preCombineField": "ts",
}, spark_conf={"spark.sql.shuffle.partitions": "2"})
def events():
    return spark.readStream.schema("id INT, ts LONG, value STRING").json(spark.conf.get("hudi.test.input"))
