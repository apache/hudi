/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Feeds a ParquetDFSSource directory with parquet batches at a fixed interval, run through spark-shell -i.
// Batch 0 inserts `initial` keys; every later batch upserts up to `updates` existing keys and inserts
// `inserts` new keys. Each batch is staged elsewhere and then renamed into the source directory so the
// streamer never sees a partially written batch. The expected final key count is written to
// <dir>/expected_total.
// Required confs: spark.hudi.rc.e2e.{dir,batches,initial,updates,inserts,interval.seconds}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

def conf(key: String) = spark.conf.get(s"spark.hudi.rc.e2e.$key")
val dir = conf("dir")
val numBatches = conf("batches").toInt
val initial = conf("initial").toLong
val updates = conf("updates").toLong
val inserts = conf("inserts").toLong
val intervalMs = conf("interval.seconds").toLong * 1000

val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
var nextKey = 0L

for (b <- 0 until numBatches) {
  val start = System.currentTimeMillis()
  val ids = if (b == 0) {
    spark.range(0, initial).toDF("id")
  } else {
    val updated = spark.range(0, updates).select(pmod(hash(lit(b), col("id")), lit(nextKey)).cast("long").as("id")).distinct()
    updated.union(spark.range(nextKey, nextKey + inserts).toDF("id"))
  }
  nextKey = if (b == 0) initial else nextKey + inserts
  val df = ids.select(
    format_string("key-%012d", col("id")).as("uuid"),
    lit(start).as("ts"),
    lit(s"rider-$b").as("rider"),
    lit(s"driver-$b").as("driver"),
    (rand(b) * 100).as("fare"),
    lit(b).as("batch"),
    when(col("id") % 3 === 0, "san_francisco").when(col("id") % 3 === 1, "sao_paulo").otherwise("chennai").as("city"))
  val staged = s"$dir/staging/batch_$b"
  val batchSize = if (b == 0) initial else updates + inserts
  df.repartition(math.max(1L, batchSize / 500000L).toInt).write.mode("overwrite").parquet(staged)
  val files = fs.listStatus(new Path(staged)).map(_.getPath).filter(_.getName.endsWith(".parquet"))
  files.zipWithIndex.foreach { case (p, i) => fs.rename(p, new Path(f"$dir/source/batch_$b%04d_$i%03d.parquet")) }
  println(s"GEN batch=$b files=${files.length} expectedTotal=$nextKey")
  val elapsed = System.currentTimeMillis() - start
  if (b < numBatches - 1 && elapsed < intervalMs) Thread.sleep(intervalMs - elapsed)
}

val out = new java.io.PrintWriter(s"$dir/expected_total")
out.print(nextKey)
out.close()
println(s"GEN DONE expectedTotal=$nextKey")
System.exit(0)
