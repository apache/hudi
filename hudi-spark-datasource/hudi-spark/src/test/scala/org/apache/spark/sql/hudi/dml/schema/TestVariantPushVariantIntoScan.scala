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

import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.SaveMode
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
 * others. The wide-projection, partial-update and bootstrap checks are the regression tests of
 * #20041 (row writers typed over the projected shape).
 */
class TestVariantPushVariantIntoScan extends HoodieSparkSqlTestBase {

  private val SPARK_4_1_GATE = "PushVariantIntoScan is on by default from Spark 4.1"

  // ids 0-2 keep their inserted value, id 3 is updated and id 4's variant is nulled by the update.
  private def merged(id: Int): String = if (id < 3) s"x$id" else if (id == 3) "y3" else null
  private def mergedJson(id: Int): String = Option(merged(id)).map(k => s"""{"k":"$k"}""").orNull

  // Eight pushed paths of which a row holds one, so seven members of the projection struct are
  // null. A row writer typed over VariantType instead of the projected shape reads that struct's
  // null bitset as a variant length and throws NegativeArraySizeException (#20040).
  private def widePaths(column: String): String =
    (s"try_variant_get($column, '$$.k', 'string')" +:
      ('b' to 'h').map(c => s"try_variant_get($column, '$$.$c', 'bigint')")).mkString(", ")

  private def wideRow(id: Int): Seq[Any] = Seq(id, merged(id)) ++ Seq.fill(7)(null)

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
      checkAnswer(s"select id, ${widePaths("v")} from $tableName order by id")(
        (0 until 5).map(wideRow): _*)
      assertPushed(s"select id, variant_get(v, '$$.k', 'string') from $tableName", pushed, tableName)
      if (tableType == "mor") {
        // A MERGE INTO that assigns ts alone writes a partial log block on MOR, so the read rebuilds
        // id 1 field by field (SparkRecordMergingUtils.mergePartialRecords) with v taken from the
        // base-file record, which carries the projection struct on the pushed arm.
        withSQLConf("hoodie.spark.sql.merge.into.partial.updates" -> "true") {
          spark.sql(s"merge into $tableName t using (select 1 as id, 1003L as ts) s on t.id = s.id " +
            "when matched then update set ts = s.ts")
        }
        checkAnswer(s"select id, variant_get(v, '$$.k', 'string'), ts from $tableName where id = 1")(
          Seq(1, "x1", 1003L))
        checkAnswer(s"select id, ${widePaths("v")} from $tableName where id = 1")(wideRow(1))
      }
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

  test("bootstrapped tables read the projection through the skeleton and data file join") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // The record type is not swept: a METADATA_ONLY bootstrap on the SPARK record type fails in the
    // skeleton write (HUDI-5807), so every leg runs on the default record type.
    Seq("cow", "mor").foreach { tableType =>
      Seq("false", "true").foreach { pushIntoScan =>
        withSQLConf("spark.sql.variant.pushVariantIntoScan" -> pushIntoScan) {
          withTempDir { tmp =>
            val tableName = generateTableName
            val leg = s"bootstrap $tableType pushVariantIntoScan=$pushIntoScan, $tableName"
            val srcPath = s"${tmp.getCanonicalPath}/source"
            val tablePath = s"${tmp.getCanonicalPath}/hudi"
            spark.sql("""select cast(id as int) as id, parse_json(concat('{"a":', id, '}')) as v, """ +
              "1000L as ts from range(0, 10, 1, 1)")
              .write.parquet(srcPath)
            val writeOpts = Map(
              DataSourceWriteOptions.TABLE_TYPE.key ->
                (if (tableType == "mor") DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
                 else DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL),
              HoodieWriteConfig.TBL_NAME.key -> tableName,
              DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
              DataSourceWriteOptions.ORDERING_FIELDS.key -> "ts",
              DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getName,
              // col stats is not supported with bootstrap operation
              HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false")
            spark.emptyDataFrame.write.format("hudi")
              .options(writeOpts)
              .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
              .option(HoodieBootstrapConfig.BASE_PATH.key, srcPath)
              .mode(SaveMode.Overwrite)
              .save(tablePath)
            assert(createMetaClient(spark, tablePath).getTableConfig.getBootstrapBasePath.isPresent,
              s"[$leg] table should be bootstrapped from $srcPath")
            // COW stays read-only: an upsert would rewrite the file group into a regular base file and
            // drop the skeleton and data file join. On MOR the upsert lands in a log file, so the merged
            // read joins skeleton, source file and log.
            if (tableType == "mor") {
              spark.sql("""select cast(id as int) as id, case when id = 9 then null """ +
                """else parse_json(concat('{"a":', 100 + id, '}')) end as v, """ +
                "1001L as ts from range(5, 10, 1, 1)")
                .write.format("hudi")
                .options(writeOpts)
                .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
                .mode(SaveMode.Append)
                .save(tablePath)
            }
            val view = s"${tableName}_view"
            spark.read.format("hudi").load(tablePath).createOrReplaceTempView(view)

            val projectedSql = s"select id, variant_get(v, '$$.a', 'bigint') from $view order by id"
            if (tableType == "mor") {
              checkAnswer(projectedSql)(
                (0 until 9).map(id => Seq(id, if (id < 5) id.toLong else 100L + id)) :+ Seq(9, null): _*)
            } else {
              checkAnswer(projectedSql)((0 until 10).map(id => Seq(id, id.toLong)): _*)
            }
            checkAnswer(s"select _hoodie_record_key, variant_get(v, '$$.a', 'bigint') from $view " +
              "where id in (2, 7) order by id")(
              Seq("2", 2L), Seq("7", if (tableType == "mor") 107L else 7L))
            checkAnswer(s"select count(*) from $view where v is null")(
              Seq(if (tableType == "mor") 1L else 0L))
            // A wide read over the join: seven of the eight pushed paths are absent from the row.
            val wide = ('a' to 'h').map(c => s"try_variant_get(v, '$$.$c', 'bigint')").mkString(", ")
            checkAnswer(s"select _hoodie_record_key, $wide from $view where id = 2")(
              Seq("2", 2L) ++ Seq.fill(7)(null))
            assertPushed(projectedSql, pushIntoScan.toBoolean, leg)
            spark.catalog.dropTempView(view)
          }
        }
      }
    }
  }
}
