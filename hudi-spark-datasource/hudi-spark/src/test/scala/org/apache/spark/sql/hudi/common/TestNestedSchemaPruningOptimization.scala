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

import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertEquals

class TestNestedSchemaPruningOptimization extends HoodieSparkSqlTestBase {

  // NOTE: We disable WCE once for the whole suite so the executed plans stay a plain Project over the
  //       scan node (no WholeStageCodegenExec wrapper); every test here pattern-matches the scan to
  //       read its required schema, so this keeps the plan-inspection helpers free of side effects.
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)
  }

  test("Test NestedSchemaPruning optimization successful") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        createTableWithNestedStructSchema(tableType, tableName, tablePath)

        // Only "item.name" is referenced, so the read schema prunes "item" down to a single leaf
        val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")

        val expectedSchema = StructType(Seq(
          StructField("id", IntegerType, nullable = true),
          StructField("item", StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
        ))

        assertPrunedReadSchema(selectDF, tableName, expectedSchema)

        checkAnswer(s"SELECT id, item.name FROM $tableName")(Seq(1, "a1"))
      }
    }
  }

  test("Test nested schema pruning with DefaultHoodieRecordPayload") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // NOTE: On the file-group-reader based read path the payload class does not affect nested
      //       schema pruning, so the read schema is pruned the same way as with the default payload
      createTableWithNestedStructSchema("mor", tableName, tablePath,
        Map(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> "org.apache.hudi.common.model.DefaultHoodieRecordPayload"))

      val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")

      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("item", StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
      ))

      assertPrunedReadSchema(selectDF, tableName, expectedSchema)

      checkAnswer(s"SELECT id, item.name FROM $tableName")(Seq(1, "a1"))
    }
  }

  test("Test nested schema pruning with array, map and partition columns") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // NOTE: Meta fields stay enabled here since the SQL UPDATE on MOR needs them to locate the file group
      createTableWithNestedStructSchema("mor", tableName, tablePath,
        extraColumns = ", array(named_struct('k', 'k0', 'v', 'v0')) AS tags," +
          " map('m0', named_struct('a', 1, 'b', 2)) AS props",
        partitionCol = "part",
        populateMetaFields = true)

      // The update writes a log file, so the pruned reads below merge base and log records
      // (the historically fragile path of HUDI-5443)
      spark.sql(s"UPDATE $tableName SET ts = 123457 WHERE id = 1")

      // Only "item.name" is referenced, so "item" is pruned down to a single leaf and the
      // unreferenced "tags" (array<struct>) and "props" (map<string,struct>) columns are dropped
      val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")
      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("item", StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
      ))
      assertPrunedReadSchema(selectDF, tableName, expectedSchema)

      // Partition column values must survive the pruned read (see #18570)
      checkAnswer(s"SELECT id, item.name, part FROM $tableName")(Seq(1, "a1", "p1"))

      // Projecting through the array keeps only the referenced leaf of the element struct
      val tagsDF = spark.sql(s"SELECT tags.k FROM $tableName")
      val expectedTagsSchema = StructType(Seq(
        StructField("tags",
          ArrayType(StructType(Seq(StructField("k", StringType, nullable = false))), containsNull = false),
          nullable = true)
      ))
      assertPrunedReadSchema(tagsDF, tableName, expectedTagsSchema)
      checkAnswer(s"SELECT tags.k FROM $tableName")(Seq(Seq("k0")))

      // Projecting through the map keeps only the referenced leaf of the value struct
      val propsDF = spark.sql(s"SELECT props['m0'].a FROM $tableName")
      val expectedPropsSchema = StructType(Seq(
        StructField("props",
          MapType(StringType, StructType(Seq(StructField("a", IntegerType, nullable = false))), valueContainsNull = false),
          nullable = true)
      ))
      assertPrunedReadSchema(propsDF, tableName, expectedPropsSchema)
      checkAnswer(s"SELECT props['m0'].a FROM $tableName")(Seq(1))

      // Filter on a nested field combined with pruning
      checkAnswer(s"SELECT id, item.price FROM $tableName WHERE item.name = 'a1'")(Seq(1, 10))
    }
  }

  test("Test no nested schema pruning when disabled") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      createTableWithNestedStructSchema("mor", tableName, tablePath)

      // With the optimizer flag off no nested schema pruning happens, so "item" keeps "price" even
      // though only "item.name" is projected
      val expectedItemStruct = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("price", IntegerType, nullable = false)
      ))

      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")
        assertEquals(expectedItemStruct, prunedStructTypeOf(selectDF, "item"))
        selectDF.count
      }
    }
  }

  private def assertPrunedReadSchema(selectDF: DataFrame,
                                     tableName: String,
                                     expectedSchema: StructType): Unit = {
    val fileScan = fileScanOf(selectDF)
    assertEquals(tableName, fileScan.tableIdentifier.get.table)
    assertEquals(expectedSchema, fileScan.requiredSchema)
  }

  private def prunedStructTypeOf(selectDF: DataFrame, fieldName: String): StructType =
    fileScanOf(selectDF).requiredSchema(fieldName).dataType.asInstanceOf[StructType]

  private def fileScanOf(selectDF: DataFrame): FileSourceScanExec =
    selectDF.queryExecution.executedPlan match {
      case ProjectExec(_, fileScan: FileSourceScanExec) => fileScan
      case other => fail(s"Unexpected plan shape (expected Project over FileSourceScanExec):\n$other")
    }

  private def createTableWithNestedStructSchema(tableType: String,
                                                tableName: String,
                                                tablePath: String,
                                                opts: Map[String, String] = Map.empty,
                                                extraColumns: String = "",
                                                partitionCol: String = "",
                                                populateMetaFields: Boolean = false): Unit = {
    val partitionedByClause = if (partitionCol.nonEmpty) s"PARTITIONED BY ($partitionCol)" else ""
    val partitionSelectExpr = if (partitionCol.nonEmpty) s", 'p1' AS $partitionCol" else ""
    val optsClause = if (opts.nonEmpty) "," + opts.map { case (k, v) => s"'$k' = '$v'" }.mkString(",") else ""
    spark.sql(
      s"""
         |CREATE TABLE $tableName USING HUDI
         |$partitionedByClause
         |TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.populate.meta.fields = '$populateMetaFields'
         |  $optsClause
         |)
         |LOCATION '$tablePath'
         |AS SELECT
         |  1 AS id,
         |  named_struct('name', 'a1', 'price', 10) AS item$extraColumns,
         |  123456 AS ts$partitionSelectExpr
             """.stripMargin)
  }
}
