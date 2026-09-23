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

package org.apache.spark.sql.hudi.dml.schema

import org.apache.hudi.{HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType

import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * The pushVariantIntoScan legs of master's TestVariantShreddingMixedLayouts (#19688, #19783),
 * ported to release-1.2.1 over unshredded data only, since this branch has no shredding writer.
 * spark.sql.variant.pushVariantIntoScan is swept on and off: both arms expect the very same rows,
 * and only the plan tells them apart (whether the rule rewrote the variant into a projection
 * struct inside the scan). Record types are swept too, which on table version 9 gives avro
 * (AVRO) and parquet (SPARK) log blocks on the MOR legs.
 *
 * Every leg runs to the end and its verdict is recorded, so one wrong result does not hide the
 * others; a leg that crashes the JVM ends the run, so the leg filter below lets a suspect leg run
 * in its own JVM. VARIANT_LEGS / VARIANT_SKIP_LEGS are comma-separated key prefixes over
 * "<scope>:<tableType>:<pushVariantIntoScan>:<recordType>", e.g. "nested:mor:true".
 */
class TestVariantPushVariantIntoScan extends HoodieSparkSqlTestBase {

  private val SPARK_4_1_GATE = "PushVariantIntoScan is on by default from Spark 4.1"

  // ids 0-2 keep their inserted value, id 3 is updated and id 4's variant is nulled by the update.
  private def merged(id: Int): String = if (id < 3) s"x$id" else if (id == 3) "y3" else null
  private def mergedJson(id: Int): String = Option(merged(id)).map(k => s"""{"k":"$k"}""").orNull

  private def prefixes(name: String): Seq[String] =
    sys.env.get(name).toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)

  private def legSelected(key: String): Boolean = {
    val only = prefixes("VARIANT_LEGS")
    val skip = prefixes("VARIANT_SKIP_LEGS")
    (only.isEmpty || only.exists(key.startsWith)) && !skip.exists(key.startsWith)
  }

  private def variantProjectionPushedIntoScan(sql: String): Boolean = {
    def containsProjection(dataType: DataType): Boolean = dataType match {
      case st: StructType =>
        SparkAdapterSupport.sparkAdapter.isVariantProjectionStruct(st) ||
          st.fields.exists(f => containsProjection(f.dataType))
      case _ => false
    }
    val scans = spark.sql(sql).queryExecution.sparkPlan.collect { case scan: FileSourceScanExec => scan }
    assert(scans.nonEmpty, s"expected a file scan in the plan of: $sql")
    scans.exists(_.requiredSchema.fields.exists(f => containsProjection(f.dataType)))
  }

  private def assertPushed(sql: String, pushed: Boolean, leg: String): Unit = {
    val verdict = if (pushed) "should have" else "must not have"
    assert(variantProjectionPushedIntoScan(sql) == pushed,
      s"[$leg] PushVariantIntoScan $verdict rewritten the variant into a projection struct")
  }

  /**
   * Sweeps table type x pushVariantIntoScan x record type, runs `body` once per leg on a fresh
   * table, and fails at the end with every leg that failed.
   */
  private def sweep(scope: String)(body: (String, String, String, Boolean) => Unit): Unit = {
    val failures = ListBuffer.empty[String]
    var ran = 0
    // The conf-off arm runs first so that, when a conf-on leg crashes the JVM, every other verdict
    // is already in the log.
    Seq("cow", "mor").foreach { tableType =>
      Seq("false", "true").foreach { pushIntoScan =>
        Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK).foreach { recordType =>
          val key = s"$scope:$tableType:$pushIntoScan:$recordType"
          if (legSelected(key)) {
            ran += 1
            withSQLConf("spark.sql.variant.pushVariantIntoScan" -> pushIntoScan) {
              withRecordType(Seq(recordType))(withTempDir { tmp =>
                val tableName = generateTableName
                val leg = s"$key, $tableName"
                // scalastyle:off println
                println(s"LEG START $key")
                Try(body(tableName, tmp.getCanonicalPath, tableType, pushIntoScan.toBoolean)) match {
                  case Success(_) => println(s"LEG PASS $key")
                  case Failure(e) =>
                    val msg = Option(e.getMessage).getOrElse(e.toString).linesIterator.take(3).mkString(" | ")
                    println(s"LEG FAIL $key: ${e.getClass.getSimpleName}: $msg")
                    failures += s"[$leg] ${e.getClass.getSimpleName}: $msg"
                }
                // scalastyle:on println
              })
            }
          }
        }
      }
    }
    assume(ran > 0, s"no $scope leg selected by VARIANT_LEGS / VARIANT_SKIP_LEGS")
    assert(failures.isEmpty, s"${failures.size} of $ran $scope legs failed:\n" + failures.mkString("\n"))
  }

  private def createTable(tableName: String, tablePath: String, tableType: String, variantColumnDdl: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  $variantColumnDdl,
         |  ts long
         |) using hudi
         | location '$tablePath'
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  type = '$tableType',
         |  hoodie.compact.inline = 'false'
         | )
       """.stripMargin)
  }

  test("top-level variant_get projections and filters under pushVariantIntoScan on and off") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    sweep("toplevel") { (tableName, tablePath, tableType, pushed) =>
      createTable(tableName, tablePath, tableType, "v variant")
      spark.sql(s"insert into $tableName select cast(id as int) as id, " +
        """parse_json(concat('{"k":"x', id, '"}')) as v, 1000L as ts from range(0, 5, 1, 1)""")
      // ids >= 3 go through a log file on MOR (a rewritten base file on COW); id 4 is nulled.
      spark.sql(s"update $tableName set " +
        """v = parse_json(case when id = 4 then cast(null as string) """ +
        """else concat('{"k":"y', id, '"}') end), ts = 1001 """ +
        "where id >= 3")

      checkAnswer(s"select id, variant_get(v, '$$.k', 'string') from $tableName order by id")(
        (0 until 5).map(id => Seq(id, merged(id))): _*)
      checkAnswer(s"select id from $tableName where variant_get(v, '$$.k', 'string') = 'y3'")(Seq(3))
      checkAnswer(s"select id from $tableName where variant_get(v, '$$.k', 'string') = 'x1'")(Seq(1))
      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        (0 until 5).map(id => Seq(id, mergedJson(id))): _*)
      // The rule rewrites `v is null` onto the projection struct itself, so a null variant
      // projected as a struct OF nulls instead of a NULL struct loses this row.
      checkAnswer(s"select id from $tableName where v is null")(Seq(4))
      checkAnswer(s"select count(*) from $tableName where v is not null")(Seq(4))
      assertPushed(s"select id, variant_get(v, '$$.k', 'string') from $tableName", pushed, tableName)
    }
  }

  test("nested variant_get projections and filters under pushVariantIntoScan on and off") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // MOR with a log file is the #19783 case: before that fix the internal reader applied the
    // PushVariantIntoScan projection to TOP-LEVEL fields only, so the merged row still held a raw
    // VariantVal at s.inner while the plan read that memory as the projected struct s.inner.0
    // (SIGBUS / InternalError / OOM, or silent nulls).
    sweep("nested") { (tableName, tablePath, tableType, pushed) =>
      createTable(tableName, tablePath, tableType, "s struct<inner: variant>")
      spark.sql(s"insert into $tableName select cast(id as int) as id, " +
        """named_struct('inner', parse_json(concat('{"k":"x', id, '"}'))) as s, """ +
        "1000L as ts from range(0, 5, 1, 1)")
      spark.sql(s"update $tableName set " +
        """s = named_struct('inner', parse_json(case when id = 4 then cast(null as string) """ +
        """else concat('{"k":"y', id, '"}') end)), ts = 1001 """ +
        "where id >= 3")

      checkAnswer(s"select id, variant_get(s.inner, '$$.k', 'string') from $tableName order by id")(
        (0 until 5).map(id => Seq(id, merged(id))): _*)
      checkAnswer(s"select id from $tableName where variant_get(s.inner, '$$.k', 'string') = 'y3'")(Seq(3))
      checkAnswer(s"select id from $tableName where variant_get(s.inner, '$$.k', 'string') = 'x1'")(Seq(1))
      checkAnswer(s"select id, cast(s.inner as string) from $tableName order by id")(
        (0 until 5).map(id => Seq(id, mergedJson(id))): _*)
      checkAnswer(s"select id from $tableName where s.inner is null")(Seq(4))
      checkAnswer(s"select count(*) from $tableName where s.inner is not null")(Seq(4))
      // The whole struct: no extraction, nothing is rewritten, native VariantType at depth.
      val wholeStruct = spark.sql(s"select id, s from $tableName order by id").collect()
      assert(wholeStruct.length == 5, s"[$tableName] whole-struct read should return 5 rows")
      wholeStruct.foreach { row =>
        val id = row.getInt(0)
        assert(!row.isNullAt(1), s"[$tableName] whole-struct read nulled out s for id $id")
        val inner = row.getStruct(1).getAs[Any]("inner")
        assert(Option(inner).map(_.toString).orNull == mergedJson(id),
          s"[$tableName] whole-struct read of s.inner for id $id should be ${mergedJson(id)}, got $inner")
      }
      assertPushed(s"select id, variant_get(s.inner, '$$.k', 'string') from $tableName", pushed, tableName)
    }
  }
}
