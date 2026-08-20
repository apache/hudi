/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val outputDir = "/tmp/native-spark-bundle"

// Force a real join rather than a broadcast, so the plan exercises Comet's join, shuffle and sort.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// Deterministic input, so the query result can be asserted exactly. 300 rows spread evenly over
// three partitions, fare equal to the row id.
def writeTable(name: String): String = {
  val path = "file:///tmp/hudi-bundles/tests/" + name
  spark.range(0, 300).selectExpr(
      "concat('id-', cast(id as string)) as uuid",
      "cast(id % 3 as string) as partitionpath",
      "cast(id as double) as fare",
      "id as ts")
    .write.format("hudi").
    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
    option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
    option(TABLE_NAME, name).
    mode(Overwrite).
    save(path)
  path
}

spark.read.format("hudi").load(writeTable("trips_native_1")).createOrReplaceTempView("t1")
spark.read.format("hudi").load(writeTable("trips_native_2")).createOrReplaceTempView("t2")

// Each partition holds 100 rows per side, so the join emits 100 * 100 rows per partition and each
// t1 fare is summed 100 times. Aggregate a data column, not just the partition column: a scan
// projecting no data columns reads as ReadSchema struct<> and Comet declines to bridge it.
val query = spark.sql(
  "select t1.partitionpath, count(*) as c, sum(t1.fare) as s from t1 " +
  "join t2 on t1.partitionpath = t2.partitionpath group by t1.partitionpath order by t1.partitionpath")

val rows = query.collect().map(r => s"${r.get(0)},${r.get(1)},${r.get(2)}")
rows.foreach(r => println("::warning::native bundle row " + r))
sc.parallelize(rows, 1).saveAsTextFile(outputDir + "/rows")

// Comet does not recognize Hudi's file format and leaves the scan to Spark, but with
// spark.comet.convert.parquet.enabled it bridges Hudi's columnar output into Arrow and runs
// everything above the scan natively. Asserting on the plan matters because Comet degrades
// silently: a mis-relocated Comet or a missing libcomet.so still returns correct results.
val plan = query.queryExecution.executedPlan.toString
println("::warning::native bundle executed plan\n" + plan)
sc.parallelize(Seq(plan), 1).saveAsTextFile(outputDir + "/plan")

System.exit(0)
