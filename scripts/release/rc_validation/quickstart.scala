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

// Quickstart-style sanity checks for a release candidate, run through spark-shell -i.
// Covers COW and MOR via the datasource API and Spark SQL. Prints "QS PASS|FAIL <check>"
// lines and exits non-zero if any check fails.
// Required conf: spark.hudi.rc.quickstart.dir (local working directory).

import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._

val workDir = spark.conf.get("spark.hudi.rc.quickstart.dir")
val base = s"file://$workDir/tables"
var failures = List.empty[String]

def check(name: String, cond: => Boolean): Unit = {
  val ok = try cond catch {
    case e: Throwable => e.printStackTrace(); false
  }
  println(s"QS ${if (ok) "PASS" else "FAIL"} $name")
  if (!ok) failures ::= name
}

// Completion time of a completed instant, read from its "<requested>_<completed>.<action>" file.
// Incremental queries in 1.x bound on completion time, not on the requested instant time.
def completionTime(tablePath: String, requestedTime: String): String =
  new java.io.File(tablePath.stripPrefix("file://") + "/.hoodie/timeline").list
    .filter(f => f.startsWith(requestedTime + "_")).head.split("[_.]")(1)

val cols = Seq("ts", "uuid", "rider", "driver", "fare", "city")
val data = Seq(
  (1695159649087L, "334e26e9", "rider-A", "driver-K", 19.10, "san_francisco"),
  (1695091554788L, "e96c4396", "rider-C", "driver-M", 27.70, "san_francisco"),
  (1695046462179L, "9909a8b1", "rider-D", "driver-L", 33.90, "san_francisco"),
  (1695332066204L, "1dced545", "rider-E", "driver-O", 93.50, "san_francisco"),
  (1695516137016L, "e3cf430c", "rider-F", "driver-P", 34.15, "sao_paulo"),
  (1695376420876L, "7a84095f", "rider-G", "driver-Q", 43.40, "sao_paulo"),
  (1695173887231L, "3eeb61f7", "rider-I", "driver-S", 41.06, "chennai"),
  (1695115999911L, "c8abbe79", "rider-J", "driver-T", 17.85, "chennai"))

for (tableType <- Seq("COPY_ON_WRITE", "MERGE_ON_READ")) {
  val tableName = s"trips_${tableType.toLowerCase}"
  val path = s"$base/$tableName"
  val opts = Map(
    "hoodie.table.name" -> tableName,
    "hoodie.datasource.write.table.type" -> tableType,
    "hoodie.datasource.write.recordkey.field" -> "uuid",
    "hoodie.datasource.write.partitionpath.field" -> "city",
    "hoodie.datasource.write.precombine.field" -> "ts")
  def read() = spark.read.format("hudi").load(path)

  // Datasource writes
  spark.createDataFrame(data).toDF(cols: _*).write.format("hudi").options(opts).mode(Overwrite).save(path)
  check(s"$tableType datasource insert", read().count == 8)
  val firstCommit = read().select("_hoodie_commit_time").distinct.collect.map(_.getString(0)).max

  val updates = read().filter("rider = 'rider-D'").withColumn("fare", col("fare") * 10).select(cols.map(col): _*)
  updates.write.format("hudi").options(opts).option("hoodie.datasource.write.operation", "upsert").mode(Append).save(path)
  check(s"$tableType datasource upsert", read().filter("rider = 'rider-D'").select("fare").first.getDouble(0) == 339.0)

  val deletes = read().filter("rider = 'rider-F'").select(cols.map(col): _*)
  deletes.write.format("hudi").options(opts).option("hoodie.datasource.write.operation", "delete").mode(Append).save(path)
  check(s"$tableType datasource delete", read().count == 7)

  // Reads
  val incremental = spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", completionTime(path, firstCommit))
    .load(path)
  check(s"$tableType incremental query", incremental.select("rider").collect.map(_.getString(0)).toSet == Set("rider-D"))
  check(s"$tableType time travel", spark.read.format("hudi").option("as.of.instant", firstCommit).load(path).count == 8)
  if (tableType == "MERGE_ON_READ") {
    // The update sits in a log file, so a read-optimized query still sees the base-file value.
    check(s"$tableType read optimized query", spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized").load(path)
      .filter("rider = 'rider-D'").select("fare").first.getDouble(0) == 33.9)
  }

  // Spark SQL
  val sqlTable = s"sql_$tableName"
  val sqlType = if (tableType == "COPY_ON_WRITE") "cow" else "mor"
  spark.sql(s"DROP TABLE IF EXISTS $sqlTable")
  spark.sql(
    s"""CREATE TABLE $sqlTable (ts BIGINT, uuid STRING, rider STRING, driver STRING, fare DOUBLE, city STRING)
       |USING HUDI TBLPROPERTIES (type = '$sqlType', primaryKey = 'uuid', preCombineField = 'ts')
       |PARTITIONED BY (city) LOCATION '$base/$sqlTable'""".stripMargin)
  spark.createDataFrame(data).toDF(cols: _*).createOrReplaceTempView("rc_quickstart_source")
  spark.sql(s"INSERT INTO $sqlTable SELECT * FROM rc_quickstart_source")
  check(s"$tableType sql insert", spark.sql(s"SELECT * FROM $sqlTable").count == 8)

  spark.sql(s"UPDATE $sqlTable SET fare = 25.0 WHERE rider = 'rider-D'")
  check(s"$tableType sql update", spark.sql(s"SELECT fare FROM $sqlTable WHERE rider = 'rider-D'").first.getDouble(0) == 25.0)

  // The matched row carries a higher ts than the target row; a lower one would be ignored by event-time ordering.
  spark.sql(
    s"""MERGE INTO $sqlTable t USING (
       |  SELECT 1695400000000L ts, '1dced545' uuid, 'rider-E' rider, 'driver-O' driver, 99.0 fare, 'san_francisco' city
       |  UNION ALL SELECT 1695400000000L, 'new00001', 'rider-Z', 'driver-Z', 10.0, 'chennai') s
       |ON t.uuid = s.uuid
       |WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""".stripMargin)
  check(s"$tableType sql merge into", spark.sql(s"SELECT * FROM $sqlTable").count == 9 &&
    spark.sql(s"SELECT fare FROM $sqlTable WHERE uuid = '1dced545'").first.getDouble(0) == 99.0)

  spark.sql(s"DELETE FROM $sqlTable WHERE rider = 'rider-Z'")
  check(s"$tableType sql delete", spark.sql(s"SELECT * FROM $sqlTable").count == 8)
  check(s"$tableType call show_commits", spark.sql(s"CALL show_commits(table => '$sqlTable')").count >= 4)
}

println(s"QS SUMMARY failures=${failures.reverse.mkString(",")}")
System.exit(if (failures.isEmpty) 0 else 1)
