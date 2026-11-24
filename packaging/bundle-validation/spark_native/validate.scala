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

val outputDir = "/tmp/spark-native-bundle"

// Force a real join rather than a broadcast, so the plan exercises Comet's join, shuffle and sort.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// Deterministic input, so the query result can be asserted exactly. 300 rows spread evenly over
// three partitions, fare equal to the row id plus the offset.
def rows(from: Int, to: Int, offset: Int = 0) = spark.range(from, to).selectExpr(
  "concat('id-', cast(id as string)) as uuid",
  "cast(id % 3 as string) as partitionpath",
  s"cast(id + $offset as double) as fare",
  s"id + $offset as ts")

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

// Merge-on-read with a second commit that updates the fare of half the rows, so the snapshot read
// returns the expected sums only if it merges base files with log files.
val morLeft = write("native_mor_1", "MERGE_ON_READ", Overwrite, rows(0, 300))
write("native_mor_1", "MERGE_ON_READ", Append, rows(0, 150, 1000))
val morRight = write("native_mor_2", "MERGE_ON_READ", Overwrite, rows(0, 300))
write("native_mor_2", "MERGE_ON_READ", Append, rows(0, 150, 1000))
probe("mor", morLeft, morRight)

// Probes for the native Hudi scan (hudi-rs inside Comet). Only meaningful for a
// bundle built with the Hudi-built Comet (-Dcomet.hudi.build), so gated on the
// environment; the probes self-assert and exit non-zero on any failure or when
// a probe was skipped by an exception spark-shell swallowed.
if (sys.env.get("NATIVE_HUDI_SCAN").contains("1")) {
  var failures = 0
  var checksRun = 0
  val expectedChecks = 55
  def check(label: String, condition: Boolean, detail: => String): Unit = {
    checksRun += 1
    if (condition) {
      println(s"::warning::native hudi scan check passed: $label")
    } else {
      println(s"::error::native hudi scan check failed: $label")
      println(detail)
      failures += 1
    }
  }
  // The reader in this bundle's Comet build supports table versions 6/8/9, so
  // the probes write their own version 8 tables, with updates in log files and
  // a delete so the merge-on-read probe exercises real native log merging.
  def writeV8(name: String, tableType: String, mode: org.apache.spark.sql.SaveMode,
              df: org.apache.spark.sql.DataFrame, operation: String,
              options: Map[String, String] = Map.empty): String = {
    val path = "file:///tmp/hudi-bundles/tests/" + name
    df.write.format("hudi").
      option("hoodie.write.table.version", "8").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_TYPE_OPT_KEY, tableType).
      option("hoodie.datasource.write.operation", operation).
      option(TABLE_NAME, name).
      options(options).
      mode(mode).
      save(path)
    path
  }
  def updatedRows(from: Int, to: Int) = spark.range(from, to).selectExpr(
    "concat('id-', cast(id as string)) as uuid",
    "cast(id % 3 as string) as partitionpath",
    "cast(id + 100000 as double) as fare",
    "id + 100000 as ts")
  val nativeCow = writeV8("native_scan_cow_v8", "COPY_ON_WRITE", Overwrite, rows(0, 300), "upsert")
  writeV8("native_scan_cow_v8", "COPY_ON_WRITE", Append, updatedRows(0, 100), "upsert")
  val nativeMor = writeV8("native_scan_mor_v8", "MERGE_ON_READ", Overwrite, rows(0, 300), "upsert")
  writeV8("native_scan_mor_v8", "MERGE_ON_READ", Append, updatedRows(0, 150), "upsert")
  writeV8("native_scan_mor_v8", "MERGE_ON_READ", Append, rows(280, 300), "delete")

  def readSorted(path: String, native: Boolean): (Array[String], String) = {
    spark.conf.set("spark.comet.scan.hudi.enabled", native.toString)
    val df = spark.read.format("hudi").load(path)
      .selectExpr("uuid", "partitionpath", "fare", "ts").orderBy("uuid")
    val result = (df.collect().map(_.toString), df.queryExecution.executedPlan.toString)
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    result
  }
  for ((label, path, isMor) <- Seq(
      ("cow", nativeCow, false),
      ("mor", nativeMor, true))) {
    val (jvmRows, _) = readSorted(path, native = false)
    val (nativeRows, nativePlan) = readSorted(path, native = true)
    check(s"$label rows match the JVM read (n=${jvmRows.length})",
      jvmRows.sameElements(nativeRows),
      s"jvm n=${jvmRows.length} native n=${nativeRows.length}\n" +
        s"only-jvm=${jvmRows.diff(nativeRows).take(5).mkString(";")}\n" +
        s"only-native=${nativeRows.diff(jvmRows).take(5).mkString(";")}")
    check(s"$label row count reflects the writes",
      jvmRows.length == (if (isMor) 280 else 300), s"n=${jvmRows.length}")
    check(s"$label scan is native", nativePlan.contains("CometHudiNativeScan"), nativePlan)
    if (isMor) {
      check(s"$label read has no row-to-columnar bridge",
        !nativePlan.contains("CometSparkRowToColumnar"), nativePlan)
    }
  }
  def joined(native: Boolean): (Array[String], String) = {
    spark.conf.set("spark.comet.scan.hudi.enabled", native.toString)
    val left = spark.read.format("hudi").load(nativeCow).selectExpr("uuid", "fare as cow_fare")
    val right = spark.read.format("hudi").load(nativeMor).selectExpr("uuid", "fare as mor_fare")
    val df = left.join(right, "uuid").selectExpr("uuid", "cow_fare", "mor_fare").orderBy("uuid")
    val result = (df.collect().map(_.toString), df.queryExecution.executedPlan.toString)
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    result
  }
  val (joinJvm, _) = joined(native = false)
  val (joinNative, joinPlan) = joined(native = true)
  check("join rows match the JVM read", joinJvm.sameElements(joinNative),
    s"jvm n=${joinJvm.length} native n=${joinNative.length}")
  check("join runs as a native sort-merge join", joinPlan.contains("CometSortMergeJoin"), joinPlan)
  // The adaptive plan string repeats the tree in its Final and Initial
  // sections, so count within the final plan only.
  val joinFinalPlan = joinPlan.split("== Initial Plan ==").head
  check("join reads both sides through the native scan",
    "CometHudiNativeScan".r.findAllIn(joinFinalPlan).length >= 2, joinPlan)
  check("join plan has no row-to-columnar bridge",
    !joinPlan.contains("CometSparkRowToColumnar"), joinPlan)

  // Shapes the provider must hand back to the JVM scan, and shapes it must
  // read natively with the same rows as the JVM read.
  def compare(label: String, expectNative: Boolean)(q: => org.apache.spark.sql.DataFrame): Unit = {
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    val jvmDf = q
    val jvmRows = jvmDf.collect().map(_.toString).sorted
    val jvmTasks = jvmDf.rdd.getNumPartitions
    spark.conf.set("spark.comet.scan.hudi.enabled", "true")
    val nativeDf = q
    val nativeRows = nativeDf.collect().map(_.toString).sorted
    val nativePlan = nativeDf.queryExecution.executedPlan.toString
    val nativeTasks = nativeDf.rdd.getNumPartitions
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    check(s"$label rows match the JVM read (n=${jvmRows.length})", jvmRows.sameElements(nativeRows),
      s"jvm n=${jvmRows.length} native n=${nativeRows.length}\n" +
        s"only-jvm=${jvmRows.diff(nativeRows).take(5).mkString(";")}\n" +
        s"only-native=${nativeRows.diff(jvmRows).take(5).mkString(";")}")
    check(s"$label scan is ${if (expectNative) "native" else "left on the JVM"}",
      nativePlan.contains("CometHudiNativeScan") == expectNative, nativePlan)
    if (expectNative) {
      check(s"$label packs file slices into as many tasks as the JVM scan", nativeTasks == jvmTasks,
        s"jvm tasks=$jvmTasks native tasks=$nativeTasks")
    }
  }
  spark.read.format("hudi").load(nativeCow).createOrReplaceTempView("native_cow")
  // Spark applies a partition filter holding a subquery at execution time, after the listing;
  // the native scan plans its tasks again over the partitions the filter keeps.
  compare("scalar subquery partition filter", expectNative = true)(
    spark.sql("select uuid, fare from native_cow where partitionpath = (select max(partitionpath) from native_cow)"))
  compare("skip_merge read", expectNative = false)(
    spark.read.format("hudi").option("hoodie.datasource.merge.type", "skip_merge").load(nativeMor)
      .selectExpr("uuid", "fare", "ts"))
  // Null and empty partition values share the default partition path, which the
  // JVM read gives back as null.
  val nullPartitions = writeV8("native_scan_null_partition_v8", "COPY_ON_WRITE", Overwrite,
    spark.range(0, 90).selectExpr(
      "concat('id-', cast(id as string)) as uuid",
      "case when id % 3 = 0 then null when id % 3 = 1 then '' else 'p' end as partitionpath",
      "cast(id as double) as fare", "id as ts"), "upsert")
  // A partitioned scan always carries its partition columns, whatever the
  // query projects, so the table stays on the JVM scan.
  compare("default partition", expectNative = false)(
    spark.read.format("hudi").load(nullPartitions).selectExpr("uuid", "fare"))
  // The JVM read appends partition values parsed back from the path; a
  // timestamp's path form keeps milliseconds and follows the JVM time zone.
  val timestampPartition = writeV8("native_scan_timestamp_partition_v8", "COPY_ON_WRITE", Overwrite,
    rows(0, 60).withColumn("partitionpath",
      org.apache.spark.sql.functions.expr("timestamp_micros((ts % 3) * 1000000 + 123456)")), "upsert")
  compare("timestamp partition column", expectNative = false)(
    spark.read.format("hudi").load(timestampPartition).selectExpr("uuid", "partitionpath", "fare"))
  // A column added after the base files were written is null-filled from the table schema.
  val evolved = writeV8("native_scan_mor_evolved_v8", "MERGE_ON_READ", Overwrite, rows(0, 300), "upsert")
  writeV8("native_scan_mor_evolved_v8", "MERGE_ON_READ", Append,
    updatedRows(0, 100).withColumn("extra", org.apache.spark.sql.functions.lit("new")), "upsert")
  compare("added column over merge-on-read logs", expectNative = true)(
    spark.read.format("hudi").load(evolved).selectExpr("uuid", "fare", "extra"))
  // Small files share a task, as in the JVM scan.
  val manyFiles = writeV8("native_scan_many_files_v8", "MERGE_ON_READ", Overwrite, rows(0, 30000), "upsert",
    Map("hoodie.parquet.max.file.size" -> "65536", "hoodie.parquet.small.file.limit" -> "0",
      "hoodie.parquet.block.size" -> "65536"))
  writeV8("native_scan_many_files_v8", "MERGE_ON_READ", Append, updatedRows(0, 15000), "upsert")
  compare("many small file slices", expectNative = true)(
    spark.read.format("hudi").load(manyFiles).selectExpr("uuid", "fare", "ts"))
  // A LIMIT stops reading mid-partition, with later slices (some merging logs) never consumed.
  {
    val all = spark.read.format("hudi").load(manyFiles).selectExpr("uuid", "fare", "ts")
      .collect().map(_.toString).toSet
    spark.conf.set("spark.comet.scan.hudi.enabled", "true")
    val limited = spark.read.format("hudi").load(manyFiles).selectExpr("uuid", "fare", "ts")
      .limit(50).collect().map(_.toString)
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    check("limit over many merge-on-read slices returns rows of the full read",
      limited.length == 50 && limited.forall(all.contains),
      s"n=${limited.length} not-in-full=${limited.filterNot(all.contains).take(5).mkString(";")}")
  }
  // Executors keep opened tables across queries; a commit after a native read must show up in
  // the next native read.
  {
    val path = writeV8("native_scan_cache_v8", "MERGE_ON_READ", Overwrite, rows(0, 300), "upsert")
    readSorted(path, native = true)
    writeV8("native_scan_cache_v8", "MERGE_ON_READ", Append, updatedRows(0, 50), "upsert")
    val (jvmRows, _) = readSorted(path, native = false)
    val (nativeRows, _) = readSorted(path, native = true)
    check("native read after a new commit matches the JVM read",
      jvmRows.sameElements(nativeRows) && nativeRows.exists(_.contains("100000.0")),
      s"only-jvm=${jvmRows.diff(nativeRows).take(5).mkString(";")}\n" +
        s"only-native=${nativeRows.diff(jvmRows).take(5).mkString(";")}")
  }

  // Dynamic partition pruning: a filtered dimension joined on the partition column prunes the
  // fact scan once the broadcast subquery resolves. The native scan must read only the slices of
  // the partitions it keeps, with and without adaptive execution.
  object PlanWalk extends org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
  def sliceCount(partitions: Array[Array[Byte]]): Int = partitions.map(b =>
    org.apache.comet.serde.OperatorOuterClass.HudiScan.parseFrom(b).getFileSlicesCount).sum
  // The dimension is a Hudi table too, as in TPC-DS, so Comet takes over the broadcast join.
  val dppDim = writeV8("native_scan_dpp_dim_v8", "COPY_ON_WRITE", Overwrite,
    spark.range(0, 3).selectExpr("concat('dim-', cast(id as string)) as uuid", "'0' as partitionpath",
      "cast(id as string) as p", "case when id = 1 then 'keep' else 'drop' end as flag", "id as ts"),
    "upsert")
  spark.read.format("hudi").load(dppDim).createOrReplaceTempView("dpp_dim")
  val dppQuery = "select f.uuid, f.fare from native_cow f join dpp_dim d " +
    "on f.partitionpath = d.p where d.flag = 'keep'"
  for (adaptive <- Seq(true, false)) {
    val label = s"dynamic partition pruning (adaptive=$adaptive)"
    spark.conf.set("spark.sql.adaptive.enabled", adaptive.toString)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    val jvmRows = spark.sql(dppQuery).collect().map(_.toString).sorted
    spark.conf.set("spark.comet.scan.hudi.enabled", "true")
    val nativeDf = spark.sql(dppQuery)
    val nativeRows = nativeDf.collect().map(_.toString).sorted
    val plan = nativeDf.queryExecution.executedPlan
    val factScans = PlanWalk.collect(plan) {
      case scan: org.apache.spark.sql.comet.CometHudiNativeScanExec
          if scan.tableBaseUri.contains("native_scan_cow_v8") => scan
    }
    val fullScans = PlanWalk.collect(spark.read.format("hudi").load(nativeCow)
        .selectExpr("uuid", "fare").queryExecution.executedPlan) {
      case scan: org.apache.spark.sql.comet.CometHudiNativeScanExec => scan
    }
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    // With the native Hudi scan off, the JVM scan runs under Comet behind a Spark-to-Arrow
    // conversion, and its pruning must survive Comet replacing the broadcast join.
    val jvmDf = spark.sql(dppQuery)
    val jvmScanRows = jvmDf.collect().map(_.toString).sorted
    val jvmScanPartitions = PlanWalk.collect(jvmDf.queryExecution.executedPlan) {
      case scan: org.apache.spark.sql.execution.FileSourceScanExec
          if scan.relation.location.rootPaths.exists(_.toString.contains("native_scan_cow_v8")) =>
        scan.metrics.get("numPartitions").map(_.value).getOrElse(-1L)
    }
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.unset("spark.sql.adaptive.enabled")
    check(s"$label JVM scan under Comet rows match the JVM read",
      jvmRows.sameElements(jvmScanRows), s"jvm n=${jvmRows.length} under Comet n=${jvmScanRows.length}")
    val jvmPlan = jvmDf.queryExecution.executedPlan.toString
    check(s"$label JVM scan under Comet reads only the kept partitions",
      jvmPlan.contains("CometSparkColumnarToColumnar") && jvmPlan.contains("CometBroadcastHashJoin") &&
        jvmScanPartitions.nonEmpty && jvmScanPartitions.forall(_ == 1),
      s"partitions read per fact scan: ${jvmScanPartitions.mkString(", ")}\n$jvmPlan")
    check(s"$label rows match the JVM read (n=${jvmRows.length})",
      jvmRows.length == 100 && jvmRows.sameElements(nativeRows),
      s"jvm n=${jvmRows.length} native n=${nativeRows.length}")
    check(s"$label fact scan is native with a runtime filter",
      factScans.exists(_.toString.toLowerCase.contains("dynamicpruning")), plan.toString)
    val listed = fullScans.map(scan => sliceCount(scan.perPartitionData)).sum
    val read = factScans.map(scan => sliceCount(scan.perPartitionData))
    check(s"$label native scan reads only the kept partitions",
      read.nonEmpty && read.forall(n => n > 0 && n < listed),
      s"file slices read per fact scan: ${read.mkString(", ")}, listed for the table: $listed")
    val partitionsRead = factScans.map(_.metrics.get("numPartitions").map(_.value).getOrElse(-1L))
    check(s"$label native scan reports the partitions it reads",
      partitionsRead.nonEmpty && partitionsRead.forall(_ == 1),
      s"partitions read per fact scan: ${partitionsRead.mkString(", ")}")
  }

  def nativeFactScans(plan: org.apache.spark.sql.execution.SparkPlan) = PlanWalk.collect(plan) {
    case scan: org.apache.spark.sql.comet.CometHudiNativeScanExec
        if scan.tableBaseUri.contains("native_scan_cow_v8") => scan
  }
  // Exchanges AQE reused whose plan reads the fact table natively.
  def reusedOverFact(plan: org.apache.spark.sql.execution.SparkPlan): Int = PlanWalk.collect(plan) {
    case reused: org.apache.spark.sql.execution.exchange.ReusedExchangeExec
        if nativeFactScans(reused.child).nonEmpty => reused
  }.size
  def dppBranch(flag: String): String =
    "select f.partitionpath, count(*) as c, sum(f.fare) as s from native_cow f join dpp_dim d " +
      s"on f.partitionpath = d.p where d.flag = '$flag' group by f.partitionpath"
  def jvmAndNative(query: String): (Array[String], Array[String], org.apache.spark.sql.execution.SparkPlan) = {
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    val jvm = spark.sql(query).collect().map(_.toString).sorted
    spark.conf.set("spark.comet.scan.hudi.enabled", "true")
    val df = spark.sql(query)
    val native = df.collect().map(_.toString).sorted
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    (jvm, native, df.queryExecution.executedPlan)
  }

  // The same DPP-filtered aggregation in two branches: each copy is planned with its own pruning
  // placeholders, which must not keep AQE from reusing the exchange over the native fact scan.
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
  val (sameJvm, sameNative, samePlan) =
    jvmAndNative(s"${dppBranch("keep")} union all ${dppBranch("keep")}")
  check(s"repeated DPP-filtered scan rows match the JVM read (n=${sameJvm.length})",
    sameJvm.length == 2 && sameJvm.sameElements(sameNative),
    s"jvm ${sameJvm.mkString(" ")} native ${sameNative.mkString(" ")}")
  check("repeated DPP-filtered native scan reuses its exchange",
    nativeFactScans(samePlan).nonEmpty && reusedOverFact(samePlan) > 0, samePlan.toString)

  // Two branches that differ only in the dimension filter, joined without a broadcast so each
  // fact scan sits right under its own exchange: the exchanges differ only in the scans' pruning
  // filters, and AQE must not reuse one for the other.
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
  spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")
  spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "1.0")
  val (diffJvm, diffNative, diffPlan) =
    jvmAndNative(s"${dppBranch("keep")} union all ${dppBranch("drop")}")
  spark.conf.unset("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly")
  spark.conf.unset("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio")
  val diffScans = nativeFactScans(diffPlan)
  check(s"differently pruned scan rows match the JVM read (n=${diffJvm.length})",
    diffJvm.length == 3 && diffJvm.sameElements(diffNative),
    s"jvm ${diffJvm.mkString(" ")} native ${diffNative.mkString(" ")}")
  check("differently pruned native scans are not reused for each other",
    diffScans.size == 2 && diffScans.forall(_.runtimeFilters.exists(
      _.exists(_.isInstanceOf[org.apache.spark.sql.execution.InSubqueryExec]))) &&
      reusedOverFact(diffPlan) == 0,
    s"fact scans: ${diffScans.size}, reused over fact: ${reusedOverFact(diffPlan)}\n$diffPlan")

  // The same native scan read twice in one query shares one exchange, as the JVM scan does.
  val reuseQuery = "select partitionpath, sum(fare) as s from native_cow group by partitionpath " +
    "union all select partitionpath, sum(fare) as s from native_cow group by partitionpath"
  spark.conf.set("spark.comet.scan.hudi.enabled", "false")
  val reuseJvmRows = spark.sql(reuseQuery).collect().map(_.toString).sorted
  spark.conf.set("spark.comet.scan.hudi.enabled", "true")
  val reuseDf = spark.sql(reuseQuery)
  val reuseRows = reuseDf.collect().map(_.toString).sorted
  val reusePlan = reuseDf.queryExecution.executedPlan.toString
  spark.conf.set("spark.comet.scan.hudi.enabled", "false")
  check(s"repeated scan rows match the JVM read (n=${reuseJvmRows.length})",
    reuseJvmRows.length == 6 && reuseJvmRows.sameElements(reuseRows),
    s"jvm n=${reuseJvmRows.length} native n=${reuseRows.length}")
  check("repeated native scan reuses its exchange",
    reusePlan.contains("CometHudiNativeScan") && reusePlan.contains("ReusedExchange"), reusePlan)

  // The shape of TPC-DS q44: a rank over a grouped aggregate whose filter keeps no rows, compared
  // with a scalar subquery. Once the aggregate's shuffle finishes with no rows, adaptive execution
  // replaces the aggregate and the comparison above it with an empty relation, so the subquery's
  // scan of the table never runs. 150.5 lies inside every file's fare range and equals no fare, so
  // the scan itself runs and returns nothing. The other predicate keeps every row, and one
  // partition's average is above the table's.
  import org.apache.spark.sql.execution.SparkPlan
  def rankAboveAverage(predicate: String, comet: Boolean, nativeScan: Boolean): (Array[String], SparkPlan) = {
    spark.conf.set("spark.comet.enabled", comet.toString)
    spark.conf.set("spark.comet.scan.hudi.enabled", nativeScan.toString)
    val df = spark.sql("select partitionpath, rnk from (select partitionpath, rank() over (order by a) as rnk " +
      s"from (select partitionpath, avg(fare) as a from native_cow where $predicate group by partitionpath " +
      "having avg(fare) > (select avg(fare) from native_cow)) averages) ranked where rnk < 11")
    val result = (df.collect().map(_.toString).sorted, df.queryExecution.executedPlan)
    spark.conf.set("spark.comet.enabled", "true")
    spark.conf.set("spark.comet.scan.hudi.enabled", "false")
    result
  }
  def isGroupedCometAggregate(plan: SparkPlan): Boolean = plan match {
    case agg: org.apache.spark.sql.comet.CometHashAggregateExec => agg.groupingExpressions.nonEmpty
    case _ => false
  }
  // The plan adaptive execution started from, before any stage ran.
  def plannedWithGroupedCometAggregate(plan: SparkPlan): Boolean = plan match {
    case adaptive: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec =>
      adaptive.initialPlan.find(isGroupedCometAggregate).isDefined
    case _ => false
  }
  // The final plan reads an empty relation where the aggregate was, and the subquery's own
  // adaptive plan, which becomes final only when the subquery runs, never did.
  def aggregateDropped(plan: SparkPlan): Boolean = {
    val nodes = PlanWalk.collect(plan) { case node => node.nodeName }
    !nodes.exists(_.contains("Aggregate")) && nodes.exists(_.contains("LocalTableScan")) &&
      "isFinalPlan=true".r.findAllIn(plan.toString).length == 1
  }
  val (sparkNoRows, sparkNoRowsPlan) = rankAboveAverage("fare = 150.5", comet = false, nativeScan = false)
  val (sparkOneRow, _) = rankAboveAverage("fare > 0", comet = false, nativeScan = false)
  for ((label, nativeScan) <- Seq(("JVM scan under Comet", false), ("native scan", true))) {
    val (noRows, noRowsPlan) = rankAboveAverage("fare = 150.5", comet = true, nativeScan = nativeScan)
    val (oneRow, oneRowPlan) = rankAboveAverage("fare > 0", comet = true, nativeScan = nativeScan)
    check(s"empty grouped aggregate ($label) rows match Spark with Comet off (n=${sparkNoRows.length})",
      sparkNoRows.isEmpty && noRows.isEmpty, s"spark n=${sparkNoRows.length} comet n=${noRows.length}")
    check(s"empty grouped aggregate ($label) becomes an empty relation before its subquery runs",
      plannedWithGroupedCometAggregate(noRowsPlan) && aggregateDropped(noRowsPlan) &&
        aggregateDropped(sparkNoRowsPlan),
      s"comet plan\n$noRowsPlan\nspark plan\n$sparkNoRowsPlan")
    check(s"grouped aggregate with rows ($label) rows match Spark with Comet off (n=${sparkOneRow.length})",
      sparkOneRow.length == 1 && sparkOneRow.sameElements(oneRow),
      s"spark=${sparkOneRow.mkString(";")} comet=${oneRow.mkString(";")}")
    check(s"grouped aggregate with rows ($label) stays a native aggregate",
      PlanWalk.collect(oneRowPlan) { case node if isGroupedCometAggregate(node) => node }.nonEmpty,
      oneRowPlan.toString)
  }
  // A global aggregate over an empty stage still returns its one row (SPARK-44040).
  {
    def globalAggregate(comet: Boolean, nativeScan: Boolean): Array[String] = {
      spark.conf.set("spark.comet.enabled", comet.toString)
      spark.conf.set("spark.comet.scan.hudi.enabled", nativeScan.toString)
      val rows = spark.sql("select count(*), sum(fare) from native_cow where fare = 150.5")
        .collect().map(_.toString)
      spark.conf.set("spark.comet.enabled", "true")
      spark.conf.set("spark.comet.scan.hudi.enabled", "false")
      rows
    }
    val sparkRows = globalAggregate(comet = false, nativeScan = false)
    val jvmRows = globalAggregate(comet = true, nativeScan = false)
    val nativeRows = globalAggregate(comet = true, nativeScan = true)
    check("global aggregate over an empty stage returns one row on the JVM and native scans",
      sparkRows.sameElements(Array("[0,null]")) && jvmRows.sameElements(sparkRows) &&
        nativeRows.sameElements(sparkRows),
      s"spark=${sparkRows.mkString(";")} jvm=${jvmRows.mkString(";")} native=${nativeRows.mkString(";")}")
  }

  if (failures > 0 || checksRun != expectedChecks) {
    println(s"::error::native hudi scan probes: failures=$failures checksRun=$checksRun expected=$expectedChecks")
    System.exit(1)
  }
}

System.exit(0)
