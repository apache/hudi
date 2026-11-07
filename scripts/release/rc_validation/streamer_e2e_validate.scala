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

// Validates the table written by the HoodieStreamer end-to-end run, through spark-shell -i.
// Prints "E2E PASS|FAIL <check>" lines and exits non-zero if any check fails.
// Required confs: spark.hudi.rc.e2e.{table.path,table.type,expected.total,batches}

import org.apache.spark.sql.functions._

def conf(key: String) = spark.conf.get(s"spark.hudi.rc.e2e.$key")
val path = conf("table.path")
val tableType = conf("table.type")
val expectedTotal = conf("expected.total").toLong
val lastBatch = conf("batches").toInt - 1
var failures = List.empty[String]

def check(name: String, detail: => String)(cond: => Boolean): Unit = {
  val ok = try cond catch {
    case e: Throwable => e.printStackTrace(); false
  }
  val info = try detail catch {
    case e: Throwable => s"<${e.getMessage}>"
  }
  println(s"E2E ${if (ok) "PASS" else "FAIL"} $name ($info)")
  if (!ok) failures ::= name
}

val snapshot = spark.read.format("hudi").load(path).cache()
lazy val count = snapshot.count
lazy val distinctKeys = snapshot.select("uuid").distinct.count
check("snapshot record count", s"count=$count expected=$expectedTotal")(count == expectedTotal)
check("no duplicate record keys", s"distinct=$distinctKeys count=$count")(distinctKeys == count)
lazy val maxBatch = snapshot.agg(max("batch")).first.getInt(0)
check("last batch landed", s"maxBatch=$maxBatch expected=$lastBatch")(maxBatch == lastBatch)

if (tableType == "MERGE_ON_READ") {
  lazy val ro = spark.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").load(path).count
  check("read optimized query", s"count=$ro")(ro > 0 && ro <= expectedTotal)
  lazy val compactions = spark.sql(s"CALL show_compaction(path => '$path', limit => 1000)").collect
  lazy val completed = compactions.count(_.getAs[String]("state") == "COMPLETED")
  lazy val pending = compactions.length - completed
  check("async compaction completed", s"completed=$completed pending=$pending")(completed >= 1)
}

spark.sql("DROP TABLE IF EXISTS rc_e2e_table")
spark.sql(s"CREATE TABLE rc_e2e_table USING hudi LOCATION '$path'")
// Active timeline only: show_cleans with showArchived => true cannot read clean metadata from the
// 1.x LSM archived timeline ("Not an Avro data file").
lazy val cleans = spark.sql("CALL show_cleans(table => 'rc_e2e_table', limit => 1000)").collect
lazy val filesDeleted = cleans.map(_.getAs[Int]("total_files_deleted").toLong).sum
check("cleaner deleted files", s"cleans=${cleans.length} filesDeleted=$filesDeleted")(cleans.nonEmpty && filesDeleted > 0)

println(s"E2E SUMMARY table=$tableType failures=${failures.reverse.mkString(",")}")
System.exit(if (failures.isEmpty) 0 else 1)
