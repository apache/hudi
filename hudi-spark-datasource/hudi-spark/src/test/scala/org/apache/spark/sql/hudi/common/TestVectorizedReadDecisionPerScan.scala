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

package org.apache.spark.sql.hudi.common

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{FileSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import java.io.File

/**
 * Spark caches the resolved relation of a table per session, so every statement on the table shares one
 * HoodieFileGroupReaderBasedFileFormat instance, and it asks that instance about batch support only for scans
 * narrower than spark.sql.codegen.maxFields. A wider scan therefore has to decide about vectorized reading for
 * its own schema when its reader is built, and no scan may change spark.sql.parquet.enableVectorizedReader,
 * which Spark reads to decide whether the scan output still needs an UnsafeRow conversion.
 */
class TestVectorizedReadDecisionPerScan extends HoodieSparkSqlTestBase with AdaptiveSparkPlanHelper {

  private val vectorizedReaderConf = "spark.sql.parquet.enableVectorizedReader"
  // Leaf fields of the payload struct; with them the table exceeds spark.sql.codegen.maxFields, so a
  // select * scan is planned row-based without a supportBatch call
  private val payloadFieldCount = 120

  Seq("cow", "mor").foreach { tableType =>
    test(s"Test wide scan after narrow scan decides vectorized read per scan for $tableType table") {
      withSQLConf(vectorizedReaderConf -> "true", "spark.sql.codegen.maxFields" -> "100") {
        withTempDir { tmp =>
          val tableName = generateTableName
          createWideTable(tableName, tableType, tmp)

          // The narrow scan is asked supportBatch and may read vectorized
          checkAnswer(s"select count(*) from $tableName")(Seq(3))
          assertResult("true")(spark.conf.get(vectorizedReaderConf))

          // The wide scan is never asked, and must not inherit the narrow scan's answer
          assert(!scanIsBatched(s"select * from $tableName"))
          checkWideScan(tableName)
          assertResult("true")(spark.conf.get(vectorizedReaderConf))

          // With the vectorized reader disabled Spark skips the UnsafeRow conversion of the scan output, so a
          // wide scan handed a leftover vectorized reader fails with
          // ClassCastException: ColumnarBatchRow cannot be cast to UnsafeRow
          withSQLConf(vectorizedReaderConf -> "false") {
            checkWideScan(tableName)
            assertResult("false")(spark.conf.get(vectorizedReaderConf))
          }
        }
      }
    }

    test(s"Test wide scan keeps vectorized reads enabled for later scans for $tableType table") {
      withSQLConf(vectorizedReaderConf -> "true", "spark.sql.codegen.maxFields" -> "100") {
        withTempDir { tmp =>
          val tableName = generateTableName
          createWideTable(tableName, tableType, tmp)

          checkWideScan(tableName)
          assertResult("true")(spark.conf.get(vectorizedReaderConf))

          // Only COW scans return batches; MOR merges rows, so its scans are never columnar
          if (tableType == "cow") {
            assert(scanIsBatched(s"select count(*) from $tableName"))
          }
          checkAnswer(s"select count(*) from $tableName")(Seq(3))
          assertResult("true")(spark.conf.get(vectorizedReaderConf))
        }
      }
    }
  }

  private def createWideTable(tableName: String, tableType: String, tmp: File): Unit = {
    val payloadType = (1 to payloadFieldCount).map(i => s"f$i int").mkString("struct<", ", ", ">")
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  payload $payloadType,
         |  ts long
         |) using hudi
         | location '${tmp.getCanonicalPath}/$tableName'
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         | )
       """.stripMargin)
    val rows = (1 to 3).map { id =>
      val payload = (1 to payloadFieldCount).map(i => s"'f$i', ${id * 1000 + i}").mkString("named_struct(", ", ", ")")
      s"select $id as id, 'a$id' as name, $payload as payload, ${id * 1000}L as ts"
    }
    spark.sql(s"insert into $tableName ${rows.mkString(" union all ")}")
    assert(WholeStageCodegenExec.isTooManyFields(spark.sessionState.conf, spark.table(tableName).schema),
      "select * must be too wide for whole-stage codegen, otherwise Spark asks supportBatch for it as well")
  }

  /** Runs select * so that the scan stays wide, and checks the rows that came back. */
  private def checkWideScan(tableName: String): Unit = {
    val actual = spark.sql(s"select * from $tableName").collect().map { row =>
      val payload = row.getAs[Row]("payload")
      (row.getAs[Int]("id"), row.getAs[String]("name"), payload.getAs[Int]("f1"), payload.getAs[Int](s"f$payloadFieldCount"))
    }.sorted.toSeq
    assertResult(Seq((1, "a1", 1001, 1120), (2, "a2", 2001, 2120), (3, "a3", 3001, 3120)))(actual)
  }

  private def scanIsBatched(sql: String): Boolean = {
    val plan = spark.sql(sql).queryExecution.executedPlan
    collectFirst(plan) { case scan: FileSourceScanExec => scan.supportsColumnar }
      .getOrElse(fail(s"No FileSourceScanExec in the plan of [$sql]:\n$plan"))
  }
}
