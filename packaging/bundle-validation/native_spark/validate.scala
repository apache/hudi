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
def rows(from: Int, to: Int) = spark.range(from, to).selectExpr(
  "concat('id-', cast(id as string)) as uuid",
  "cast(id % 3 as string) as partitionpath",
  "cast(id as double) as fare",
  "id as ts")

def write(name: String, tableType: String, mode: org.apache.spark.sql.SaveMode,
          df: org.apache.spark.sql.DataFrame): String = {
  val path = "file:///tmp/hudi-bundles/tests/" + name
  df.write.format("hudi").
    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
    option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
    option(TABLE_TYPE_OPT_KEY, tableType).
    option(TABLE_NAME, name).
    mode(mode).
    save(path)
  path
}

// Each partition holds 100 rows per side, so the join emits 100 * 100 rows per partition and each
// left fare is summed 100 times. Aggregate a data column, not just the partition column: a scan
// projecting no data columns reads as ReadSchema struct<> and Comet declines to bridge it.
def probe(label: String, leftPath: String, rightPath: String): Unit = {
  spark.read.format("hudi").load(leftPath).createOrReplaceTempView("t1")
  spark.read.format("hudi").load(rightPath).createOrReplaceTempView("t2")
  val query = spark.sql(
    "select t1.partitionpath, count(*) as c, sum(t1.fare) as s from t1 " +
    "join t2 on t1.partitionpath = t2.partitionpath group by t1.partitionpath order by t1.partitionpath")
  val result = query.collect().map(r => s"${r.get(0)},${r.get(1)},${r.get(2)}")
  result.foreach(r => println(s"::warning::native bundle $label row $r"))
  sc.parallelize(result, 1).saveAsTextFile(s"$outputDir/${label}_rows")

  // Comet does not recognize Hudi's file format and leaves the scan to Spark, but with
  // spark.comet.convert.parquet.enabled it bridges the scan's output into Arrow and runs everything
  // above it natively. Copy-on-write keeps the vectorized read and bridges columnar to columnar;
  // merge-on-read reads row by row because file group merging is row level. Asserting on the plan
  // matters because Comet degrades silently: a mis-relocated Comet or a libcomet.so that failed to
  // load still returns correct results.
  val plan = query.queryExecution.executedPlan.toString
  println(s"::warning::native bundle $label executed plan\n" + plan)
  sc.parallelize(Seq(plan), 1).saveAsTextFile(s"$outputDir/${label}_plan")
}

probe("cow", write("native_cow_1", "COPY_ON_WRITE", Overwrite, rows(0, 300)),
             write("native_cow_2", "COPY_ON_WRITE", Overwrite, rows(0, 300)))

// Merge-on-read with a second commit, so the snapshot read merges base files with log files.
val morLeft = write("native_mor_1", "MERGE_ON_READ", Overwrite, rows(0, 300))
write("native_mor_1", "MERGE_ON_READ", Append, rows(0, 150))
val morRight = write("native_mor_2", "MERGE_ON_READ", Overwrite, rows(0, 300))
write("native_mor_2", "MERGE_ON_READ", Append, rows(0, 150))
probe("mor", morLeft, morRight)

System.exit(0)
