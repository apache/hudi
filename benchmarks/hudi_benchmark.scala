/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Hudi Benchmark — DSv1 vs DSv2 Read Comparison (COW)

// ---------------------------------------------------------------------------
//  Configuration
// ---------------------------------------------------------------------------

require(sys.env.getOrElse("HUDI_BENCHMARK_DATA_PATH", "").nonEmpty,
  "HUDI_BENCHMARK_DATA_PATH environment variable must be set and non-empty")
require(sys.env.getOrElse("HUDI_BENCHMARK_RECORD_KEY", "").nonEmpty,
  "HUDI_BENCHMARK_RECORD_KEY environment variable must be set and non-empty")
require(sys.env.getOrElse("HUDI_BENCHMARK_PRECOMBINE_FIELD", "").nonEmpty,
  "HUDI_BENCHMARK_PRECOMBINE_FIELD environment variable must be set and non-empty")

val dataPath = sys.env("HUDI_BENCHMARK_DATA_PATH")
val recordKey = sys.env("HUDI_BENCHMARK_RECORD_KEY")
val precombineField = sys.env("HUDI_BENCHMARK_PRECOMBINE_FIELD")
val partitionField = sys.env.getOrElse("HUDI_BENCHMARK_PARTITION_FIELD", "")
val iterations = sys.env.getOrElse("HUDI_BENCHMARK_ITERATIONS", "1").toInt
val projectedColsEnv = sys.env.getOrElse("HUDI_BENCHMARK_PROJECTED_COLS", "")
val limitValue = sys.env.getOrElse("HUDI_BENCHMARK_LIMIT_VALUE", "1000").toInt
val filterColEnv = sys.env.getOrElse("HUDI_BENCHMARK_FILTER_COL", "")

val partitionClause = if (partitionField.nonEmpty) s"partitionedBy '$partitionField'" else ""
val partitionTblProp = if (partitionField.nonEmpty) s"'hoodie.datasource.write.partitionpath.field' = '$partitionField'," else ""

// Table name constant
val cowBulkInsert = "cow_bulk_insert"

// ---------------------------------------------------------------------------
//  Logging Setup
// ---------------------------------------------------------------------------

import java.io.{File, FileOutputStream, OutputStream, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class TeeOutputStream(primary: OutputStream, secondary: OutputStream) extends OutputStream {
  override def write(b: Int): Unit = { primary.write(b); secondary.write(b) }
  override def write(b: Array[Byte]): Unit = { primary.write(b); secondary.write(b) }
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    primary.write(b, off, len); secondary.write(b, off, len)
  }
  override def flush(): Unit = { primary.flush(); secondary.flush() }
  override def close(): Unit = { secondary.close() }
}

val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
val benchmarkDb = s"hudi_benchmark_$timestamp"
val logFileName = s"hudi_benchmark_${timestamp}.log"
val logFile = new FileOutputStream(new File(logFileName))
val origOut = System.out
val teeOut = new PrintStream(new TeeOutputStream(origOut, logFile), true)
System.setOut(teeOut)
Console.setOut(teeOut)

println(s"Benchmark log file: $logFileName")
println(s"Configuration:")
println(s"  Data path:        $dataPath")
println(s"  Record key:       $recordKey")
println(s"  Precombine field: $precombineField")
println(s"  Partition field:  ${if (partitionField.nonEmpty) partitionField else "(none)"}")
println(s"  Iterations:       $iterations")
println(s"  Limit value:      $limitValue")
println(s"  Filter column:    ${if (filterColEnv.nonEmpty) filterColEnv else "(auto-detect)"}")

// ---------------------------------------------------------------------------
//  Helper Functions
// ---------------------------------------------------------------------------

def timed(label: String)(block: => Unit): Double = {
  System.gc()
  val start = System.nanoTime()
  block
  val elapsed = (System.nanoTime() - start) / 1e9
  println(f"  $label: $elapsed%.1fs")
  elapsed
}

def stats(times: Seq[Double]): (Double, Double, Double) = {
  (times.min, times.max, times.sum / times.size)
}

def printStats(label: String, times: Seq[Double]): Unit = {
  val (mn, mx, avg) = stats(times)
  val timesStr = times.map(t => f"$t%.1fs").mkString(", ")
  println(f"$label: $timesStr  (min: $mn%.1fs, max: $mx%.1fs, avg: $avg%.1fs)")
}

// ---------------------------------------------------------------------------
//  Source Data Loading
// ---------------------------------------------------------------------------

spark.sql(s"CREATE DATABASE IF NOT EXISTS $benchmarkDb")
spark.sql(s"USE $benchmarkDb")
println(s"\nUsing database: $benchmarkDb")

println("\n=== Loading source data ===")
timed("Load source data") {
  val df = spark.read.parquet(dataPath)
  df.createOrReplaceTempView("source_data")
  val rowCount = df.count()
  println(s"  Loaded $rowCount rows")
}

// Auto-detect projected columns if not specified
val projectedCols = if (projectedColsEnv.nonEmpty) {
  projectedColsEnv
} else {
  val allCols = spark.sql("SELECT * FROM source_data LIMIT 0").columns
  val keyFields = Set(recordKey, precombineField, partitionField).filter(_.nonEmpty)
  val nonKeyCols = allCols.filterNot(keyFields.contains)
  val selected = nonKeyCols.take(6)
  println(s"  Auto-detected projected columns: ${selected.mkString(", ")}")
  selected.mkString(",")
}

// Print schema summary
println("\n--- Schema Summary ---")
val schemaFields = spark.sql("SELECT * FROM source_data LIMIT 0").schema.fields
println(s"  Total columns: ${schemaFields.length}")
println(s"  First 10: ${schemaFields.take(10).map(f => s"${f.name}:${f.dataType.simpleString}").mkString(", ")}")

// Auto-detect filter column and value for filter pushdown benchmark
val filterCol = if (filterColEnv.nonEmpty) filterColEnv else projectedCols.split(",").head.trim
val (filterExpr, filterIsNumeric) = {
  try {
    val midRow = spark.sql(
      s"SELECT percentile_approx(CAST($filterCol AS DOUBLE), 0.5) FROM source_data"
    ).collect()
    val filterValue = midRow(0).get(0)
    (s"$filterCol >= $filterValue", true)
  } catch {
    case _: Exception =>
      (s"$filterCol IS NOT NULL", false)
  }
}
println(s"\n--- Filter Configuration ---")
println(s"  Filter column: $filterCol")
println(s"  Filter expression: $filterExpr")
println(s"  Numeric filter: $filterIsNumeric")

// Track tables created during benchmark for cleanup
val createdTables = scala.collection.mutable.ListBuffer[String]()

// ---------------------------------------------------------------------------
//  Benchmark Suite
// ---------------------------------------------------------------------------

// Collectors for final summary
var readFullCowDsv1Times: Seq[Double] = Seq.empty
var readFullCowDsv2Times: Seq[Double] = Seq.empty
var readProjCowDsv1Times: Seq[Double] = Seq.empty
var readProjCowDsv2Times: Seq[Double] = Seq.empty
var readFilterCowDsv1Times: Seq[Double] = Seq.empty
var readFilterCowDsv2Times: Seq[Double] = Seq.empty
var readLimitCowDsv1Times: Seq[Double] = Seq.empty
var readLimitCowDsv2Times: Seq[Double] = Seq.empty
var aggCountDsv1Times: Seq[Double] = Seq.empty
var aggCountDsv2Times: Seq[Double] = Seq.empty
var aggMinMaxDsv1Times: Seq[Double] = Seq.empty
var aggMinMaxDsv2Times: Seq[Double] = Seq.empty

try {
  // -------------------------------------------------------------------------
  //  Setup — Create COW table for read benchmarks (untimed)
  // -------------------------------------------------------------------------

  println("\n=== SETUP: Creating COW table for read benchmarks ===")

  val cowReadTable = s"${cowBulkInsert}_1"
  spark.sql(s"DROP TABLE IF EXISTS $cowReadTable PURGE")
  createdTables += cowReadTable

  spark.sql("SET hoodie.datasource.write.operation = bulk_insert")
  timed("COW bulk_insert (setup)") {
    spark.sql(
      s"""CREATE TABLE $cowReadTable USING hudi
         |TBLPROPERTIES (
         |  type = 'cow',
         |  primaryKey = '$recordKey',
         |  preCombineField = '$precombineField',
         |  $partitionTblProp
         |  'hoodie.metadata.index.column.stats.enable' = 'true',
         |  'hoodie.compact.inline' = 'false',
         |  'hoodie.clustering.inline' = 'false',
         |  'hoodie.bulkinsert.shuffle.parallelism' = '200'
         |) $partitionClause AS SELECT * FROM source_data""".stripMargin)
  }

  // -------------------------------------------------------------------------
  //  Read Benchmark — full scan
  // -------------------------------------------------------------------------

  println("\n=== READ BENCHMARK (full scan) ===")

  // Warmup
  println("--- Warmup ---")
  spark.conf.set("hoodie.datasource.read.use.v2", "false")
  timed("Warmup COW DSv1 read") {
    spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100000").write.format("noop").mode("overwrite").save()
  }
  spark.conf.set("hoodie.datasource.read.use.v2", "true")
  timed("Warmup COW DSv2 read") {
    spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100000").write.format("noop").mode("overwrite").save()
  }

  println("\n--- Timed iterations ---")

  // DSv1 reads
  spark.conf.set("hoodie.datasource.read.use.v2", "false")

  readFullCowDsv1Times = (1 to iterations).map { i =>
    timed(s"Read full COW DSv1 (iter $i)") {
      spark.sql(s"SELECT * FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  // DSv2 reads
  spark.conf.set("hoodie.datasource.read.use.v2", "true")

  readFullCowDsv2Times = (1 to iterations).map { i =>
    timed(s"Read full COW DSv2 (iter $i)") {
      spark.sql(s"SELECT * FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  println("\n--- Full Scan Summary ---")
  printStats("Read full (COW, DSv1)", readFullCowDsv1Times)
  printStats("Read full (COW, DSv2)", readFullCowDsv2Times)

  // -------------------------------------------------------------------------
  //  Read Benchmark — projected
  // -------------------------------------------------------------------------

  println(s"\n=== READ BENCHMARK (projected: ${projectedCols.split(",").length} columns) ===")

  println("--- Timed iterations ---")

  // DSv1 projected reads
  spark.conf.set("hoodie.datasource.read.use.v2", "false")

  readProjCowDsv1Times = (1 to iterations).map { i =>
    timed(s"Read projected COW DSv1 (iter $i)") {
      spark.sql(s"SELECT $projectedCols FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  // DSv2 projected reads
  spark.conf.set("hoodie.datasource.read.use.v2", "true")

  readProjCowDsv2Times = (1 to iterations).map { i =>
    timed(s"Read projected COW DSv2 (iter $i)") {
      spark.sql(s"SELECT $projectedCols FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  println("\n--- Projected Read Summary ---")
  printStats("Read projected (COW, DSv1)", readProjCowDsv1Times)
  printStats("Read projected (COW, DSv2)", readProjCowDsv2Times)

  // -------------------------------------------------------------------------
  //  Filter Pushdown Read
  // -------------------------------------------------------------------------

  println(s"\n=== READ BENCHMARK (filter pushdown: $filterExpr) ===")

  // Warmup
  println("--- Warmup ---")
  spark.conf.set("hoodie.datasource.read.use.v2", "false")
  timed("Warmup COW filter DSv1") {
    spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr LIMIT 100000")
      .write.format("noop").mode("overwrite").save()
  }
  spark.conf.set("hoodie.datasource.read.use.v2", "true")
  timed("Warmup COW filter DSv2") {
    spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr LIMIT 100000")
      .write.format("noop").mode("overwrite").save()
  }

  println("\n--- Timed iterations ---")

  // DSv1 filter reads
  spark.conf.set("hoodie.datasource.read.use.v2", "false")

  readFilterCowDsv1Times = (1 to iterations).map { i =>
    timed(s"Read filter COW DSv1 (iter $i)") {
      spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr")
        .write.format("noop").mode("overwrite").save()
    }
  }

  // DSv2 filter reads
  spark.conf.set("hoodie.datasource.read.use.v2", "true")

  readFilterCowDsv2Times = (1 to iterations).map { i =>
    timed(s"Read filter COW DSv2 (iter $i)") {
      spark.sql(s"SELECT $projectedCols FROM $cowReadTable WHERE $filterExpr")
        .write.format("noop").mode("overwrite").save()
    }
  }

  println("\n--- Filter Read Summary ---")
  printStats("Read filter (COW, DSv1)", readFilterCowDsv1Times)
  printStats("Read filter (COW, DSv2)", readFilterCowDsv2Times)

  // -------------------------------------------------------------------------
  //  Limit Pushdown Read
  // -------------------------------------------------------------------------

  println(s"\n=== READ BENCHMARK (limit pushdown: LIMIT $limitValue) ===")

  // Warmup
  println("--- Warmup ---")
  spark.conf.set("hoodie.datasource.read.use.v2", "false")
  timed("Warmup COW limit DSv1") {
    spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100").write.format("noop").mode("overwrite").save()
  }
  spark.conf.set("hoodie.datasource.read.use.v2", "true")
  timed("Warmup COW limit DSv2") {
    spark.sql(s"SELECT * FROM $cowReadTable LIMIT 100").write.format("noop").mode("overwrite").save()
  }

  println("\n--- Timed iterations ---")

  // DSv1 limit reads
  spark.conf.set("hoodie.datasource.read.use.v2", "false")

  readLimitCowDsv1Times = (1 to iterations).map { i =>
    timed(s"Read limit COW DSv1 (iter $i)") {
      spark.sql(s"SELECT * FROM $cowReadTable LIMIT $limitValue")
        .write.format("noop").mode("overwrite").save()
    }
  }

  // DSv2 limit reads
  spark.conf.set("hoodie.datasource.read.use.v2", "true")

  readLimitCowDsv2Times = (1 to iterations).map { i =>
    timed(s"Read limit COW DSv2 (iter $i)") {
      spark.sql(s"SELECT * FROM $cowReadTable LIMIT $limitValue")
        .write.format("noop").mode("overwrite").save()
    }
  }

  println("\n--- Limit Read Summary ---")
  printStats("Read limit (COW, DSv1)", readLimitCowDsv1Times)
  printStats("Read limit (COW, DSv2)", readLimitCowDsv2Times)

  // -------------------------------------------------------------------------
  //  Aggregate Pushdown
  // -------------------------------------------------------------------------

  println("\n=== READ BENCHMARK (aggregate pushdown) ===")

  // Warmup
  println("--- Warmup ---")
  spark.conf.set("hoodie.datasource.read.use.v2", "false")
  timed("Warmup COUNT(*) DSv1") {
    spark.sql(s"SELECT COUNT(*) FROM $cowReadTable").write.format("noop").mode("overwrite").save()
  }
  spark.conf.set("hoodie.datasource.read.use.v2", "true")
  timed("Warmup COUNT(*) DSv2") {
    spark.sql(s"SELECT COUNT(*) FROM $cowReadTable").write.format("noop").mode("overwrite").save()
  }

  println("\n--- Timed iterations (COUNT(*)) ---")

  // DSv1 COUNT(*)
  spark.conf.set("hoodie.datasource.read.use.v2", "false")
  aggCountDsv1Times = (1 to iterations).map { i =>
    timed(s"COUNT(*) DSv1 (iter $i)") {
      spark.sql(s"SELECT COUNT(*) FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  // DSv2 COUNT(*)
  spark.conf.set("hoodie.datasource.read.use.v2", "true")
  aggCountDsv2Times = (1 to iterations).map { i =>
    timed(s"COUNT(*) DSv2 (iter $i)") {
      spark.sql(s"SELECT COUNT(*) FROM $cowReadTable").write.format("noop").mode("overwrite").save()
    }
  }

  println("\n--- Aggregate COUNT(*) Summary ---")
  printStats("COUNT(*) (DSv1)", aggCountDsv1Times)
  printStats("COUNT(*) (DSv2)", aggCountDsv2Times)

  // MIN/MAX — only if filter column is numeric
  if (filterIsNumeric) {
    println(s"\n--- Timed iterations (MIN/MAX on $filterCol) ---")

    spark.conf.set("hoodie.datasource.read.use.v2", "false")
    aggMinMaxDsv1Times = (1 to iterations).map { i =>
      timed(s"MIN/MAX DSv1 (iter $i)") {
        spark.sql(s"SELECT MIN($filterCol), MAX($filterCol) FROM $cowReadTable")
          .write.format("noop").mode("overwrite").save()
      }
    }

    spark.conf.set("hoodie.datasource.read.use.v2", "true")
    aggMinMaxDsv2Times = (1 to iterations).map { i =>
      timed(s"MIN/MAX DSv2 (iter $i)") {
        spark.sql(s"SELECT MIN($filterCol), MAX($filterCol) FROM $cowReadTable")
          .write.format("noop").mode("overwrite").save()
      }
    }

    println("\n--- Aggregate MIN/MAX Summary ---")
    printStats("MIN/MAX (DSv1)", aggMinMaxDsv1Times)
    printStats("MIN/MAX (DSv2)", aggMinMaxDsv2Times)
  }

  // -------------------------------------------------------------------------
  //  Final Summary
  // -------------------------------------------------------------------------

  println("\n" + "=" * 60)
  println("BENCHMARK COMPLETE")
  println("=" * 60)
  println()
  printStats("Read full (COW, DSv1)        ", readFullCowDsv1Times)
  printStats("Read full (COW, DSv2)        ", readFullCowDsv2Times)
  printStats("Read projected (COW, DSv1)   ", readProjCowDsv1Times)
  printStats("Read projected (COW, DSv2)   ", readProjCowDsv2Times)
  printStats("Read filter (COW, DSv1)      ", readFilterCowDsv1Times)
  printStats("Read filter (COW, DSv2)      ", readFilterCowDsv2Times)
  printStats("Read limit (COW, DSv1)       ", readLimitCowDsv1Times)
  printStats("Read limit (COW, DSv2)       ", readLimitCowDsv2Times)
  printStats("Aggregate COUNT(*) (DSv1)    ", aggCountDsv1Times)
  printStats("Aggregate COUNT(*) (DSv2)    ", aggCountDsv2Times)
  if (aggMinMaxDsv1Times.nonEmpty) {
    printStats("Aggregate MIN/MAX (DSv1)     ", aggMinMaxDsv1Times)
    printStats("Aggregate MIN/MAX (DSv2)     ", aggMinMaxDsv2Times)
  }

  // -------------------------------------------------------------------------
  //  Performance Comparison: DSv2 vs DSv1
  // -------------------------------------------------------------------------

  println("\n" + "=" * 60)
  println("DSv2 vs DSv1 PERFORMANCE COMPARISON")
  println("=" * 60)

  def comparePerf(label: String, dsv1Times: Seq[Double], dsv2Times: Seq[Double]): Boolean = {
    if (dsv1Times.isEmpty || dsv2Times.isEmpty) {
      println(f"$label%-35s: N/A (no data)")
      return false
    }
    val dsv1Avg = dsv1Times.sum / dsv1Times.size
    val dsv2Avg = dsv2Times.sum / dsv2Times.size
    val speedup = dsv1Avg / dsv2Avg
    val winner = if (speedup >= 1.0) "DSv2 FASTER" else "DSv1 FASTER"
    println(f"$label%-35s: DSv1 avg ${dsv1Avg}%.1fs, DSv2 avg ${dsv2Avg}%.1fs, speedup ${speedup}%.2fx ($winner)")
    speedup >= 1.0
  }

  println()
  var dsv2Wins = 0
  var totalComparisons = 0

  val comparisons = Seq(
    ("Full scan (COW)", readFullCowDsv1Times, readFullCowDsv2Times),
    ("Projected (COW)", readProjCowDsv1Times, readProjCowDsv2Times),
    ("Filter (COW)", readFilterCowDsv1Times, readFilterCowDsv2Times),
    ("Limit (COW)", readLimitCowDsv1Times, readLimitCowDsv2Times),
    ("Aggregate COUNT(*)", aggCountDsv1Times, aggCountDsv2Times),
    ("Aggregate MIN/MAX", aggMinMaxDsv1Times, aggMinMaxDsv2Times)
  )

  comparisons.foreach { case (label, dsv1, dsv2) =>
    if (dsv1.nonEmpty && dsv2.nonEmpty) {
      totalComparisons += 1
      if (comparePerf(label, dsv1, dsv2)) dsv2Wins += 1
    }
  }

  println()
  if (dsv2Wins > 0) {
    println(s"PASS: DSv2 is faster than DSv1 in $dsv2Wins of $totalComparisons scenarios")
  } else {
    println(s"WARNING: DSv2 was not faster than DSv1 in any scenario (0 of $totalComparisons)")
  }

} finally {
  // -------------------------------------------------------------------------
  //  Cleanup
  // -------------------------------------------------------------------------

  println("\n--- Cleanup ---")
  createdTables.foreach { table =>
    try {
      spark.sql(s"DROP TABLE IF EXISTS $table PURGE")
      println(s"  Dropped $table")
    } catch {
      case e: Exception => println(s"  Warning: failed to drop $table: ${e.getMessage}")
    }
  }

  // Drop the benchmark database (CASCADE drops any remaining tables)
  try {
    spark.sql(s"DROP DATABASE IF EXISTS $benchmarkDb CASCADE")
    println(s"  Dropped database $benchmarkDb")
  } catch {
    case e: Exception => println(s"  Warning: failed to drop database $benchmarkDb: ${e.getMessage}")
  }

  // Restore original stdout and close log file
  System.out.flush()
  Console.setOut(origOut)
  System.setOut(origOut)
  logFile.close()
  println(s"Benchmark log saved to: $logFileName")
}

// Exit spark-shell after benchmark
System.exit(0)
