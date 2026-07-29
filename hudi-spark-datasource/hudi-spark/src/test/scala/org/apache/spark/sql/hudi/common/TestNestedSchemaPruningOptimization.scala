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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

class TestNestedSchemaPruningOptimization extends HoodieSparkSqlTestBase with SparkAdapterSupport {

  private def explain(plan: LogicalPlan): String = {
    val explainCommand = sparkAdapter.getCatalystPlanUtils.createExplainCommand(plan, extended = true)
    executePlan(explainCommand)
      .executeCollect()
      .map(_.getString(0))
      .mkString("\n")
  }

  private def executePlan(plan: LogicalPlan): SparkPlan =
    spark.sessionState.executePlan(plan).executedPlan

  test("Test NestedSchemaPruning optimization successful") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        createTableWithNestedStructSchema(tableType, tableName, tablePath)

        val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")

        val expectedSchema = StructType(Seq(
          StructField("id", IntegerType, nullable = true),
          StructField("item" , StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
        ))

        val expectedReadSchemaClause = "ReadSchema: struct<id:int,item:struct<name:string>>"
        val hint =
          s"""
             |Following is expected to be present in the plan (where ReadSchema has properly pruned nested structs, which
             |is an optimization performed by NestedSchemaPruning rule):
             |
             |== Physical Plan ==
             |*(1) Project [id#45, item#46.name AS name#55]
             |+- FileScan parquet default.h0[id#45,item#46] Batched: false, DataFilters: [], Format: Parquet, Location: HoodieFileIndex(1 paths)[file:/private/var/folders/kb/cnff55vj041g2nnlzs5ylqk00000gn/T/spark-7137..., PartitionFilters: [], PushedFilters: [], $expectedReadSchemaClause
             |]
             |""".stripMargin

        assertPrunedReadSchema(selectDF, tableName, expectedSchema, expectedReadSchemaClause, hint)

        // Execute the query to make sure it's working as expected (smoke test)
        selectDF.count
      }
    }
  }

  test("Test NestedSchemaPruning optimization unsuccessful") {
    withTempDir { tmp =>
      // TODO add cow
      Seq("mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        // NOTE: Set of opts that will make [[NestedSchemaPruning]] ineffective
        val (writeOpts, readOpts): (Map[String, String], Map[String, String]) =
          tableType match {
            case "cow" =>
              (Map(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key -> "true"),
                Map(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key -> "true"))

            case "mor" =>
              (Map(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> "org.apache.hudi.common.model.DefaultHoodieRecordPayload"),
                Map.empty)
          }

        createTableWithNestedStructSchema(tableType, tableName, tablePath, writeOpts)

        val selectDF = withSQLConf(readOpts.toSeq: _*) {
          spark.sql(s"SELECT id, item.name FROM $tableName")
        }

        val expectedSchema = StructType(Seq(
          StructField("id", IntegerType, nullable = true),
          StructField("item",
            StructType(Seq(
              StructField("name", StringType, nullable = false))), nullable = true)
        ))

        val expectedReadSchemaClause = "ReadSchema: struct<id:int,item:struct<name:string,price:int>>"

        // NOTE: We're disabling WCE to simplify resulting plan
        spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)

        // NOTE: Unfortunately, we can't use pattern-matching to extract required fields, due to a need to maintain
        //       compatibility w/ Spark 2.4
        selectDF.queryExecution.executedPlan match {
          // COW
          case ProjectExec(_, fileScan: FileSourceScanExec) =>
            val tableIdentifier = fileScan.tableIdentifier
            val requiredSchema = fileScan.requiredSchema

            assertEquals(tableName, tableIdentifier.get.table)
            assertEquals(expectedSchema, requiredSchema)

          // MOR
          case ProjectExec(_, dataScan: RowDataSourceScanExec) =>
            // NOTE: This is temporary solution to assert for Spark 2.4, until it's deprecated
            val explainedPlan = explain(selectDF.queryExecution.logical)
            assertTrue(explainedPlan.contains(expectedReadSchemaClause))

            val tableIdentifier = dataScan.tableIdentifier
            //val requiredSchema = dataScan.requiredSchema

            assertEquals(tableName, tableIdentifier.get.table)
            //assertEquals(expectedSchema, requiredSchema, hint)
        }

        // Execute the query to make sure it's working as expected (smoke test)
        selectDF.count
      }
    }
  }

  test("Test NestedSchemaPruning prunes nested struct when array and map columns are present") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      createTableWithComplexNestedSchema(tableName, tablePath)

      // Only a single nested sub-field is projected, so "item" is pruned down to just "name" and the
      // unreferenced "tags" (array<struct>) and "props" (map<string,struct>) columns are dropped.
      // Traversing the full data schema exercises the array and map branches of countLeaves.
      val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")

      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("item", StructType(Seq(StructField("name", StringType, nullable = false))), nullable = true)
      ))
      val expectedReadSchemaClause = "ReadSchema: struct<id:int,item:struct<name:string>>"

      assertPrunedReadSchema(selectDF, tableName, expectedSchema, expectedReadSchemaClause)

      // Execute the query to make sure it's working as expected (smoke test)
      selectDF.count
    }
  }

  test("Test NestedSchemaPruning is a no-op when all nested sub-fields are selected") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      createTableWithNestedStructSchema("mor", tableName, tablePath)

      // Every leaf is projected, so the pruned schema has the same leaf count as the data schema and
      // the rule leaves "item" untouched (the countLeaves comparison is an equality, not a >).
      val selectDF = spark.sql(s"SELECT id, item.name, item.price, ts FROM $tableName")

      val expectedItemStruct = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("price", IntegerType, nullable = false)
      ))
      assertEquals(expectedItemStruct, nestedFieldOf(selectDF, "item"))

      selectDF.count
    }
  }

  test("Test NestedSchemaPruning is a no-op when nested schema pruning is disabled") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      createTableWithNestedStructSchema("mor", tableName, tablePath)

      // With the optimizer flag off the rule short-circuits, so "item" keeps "price" even though only
      // "item.name" is projected.
      val expectedItemStruct = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("price", IntegerType, nullable = false)
      ))

      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "false") {
        val selectDF = spark.sql(s"SELECT id, item.name FROM $tableName")
        assertEquals(expectedItemStruct, nestedFieldOf(selectDF, "item"))
        selectDF.count
      }
    }
  }

  private def assertPrunedReadSchema(selectDF: DataFrame,
                                     tableName: String,
                                     expectedSchema: StructType,
                                     expectedReadSchemaClause: String,
                                     hint: String = ""): Unit = {
    // NOTE: We're disabling WCE to simplify resulting plan
    spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)

    // NOTE: Unfortunately, we can't use pattern-matching to extract required fields, due to a need to maintain
    //       compatibility w/ Spark 2.4
    selectDF.queryExecution.executedPlan match {
      // COW
      case ProjectExec(_, fileScan: FileSourceScanExec) =>
        assertEquals(tableName, fileScan.tableIdentifier.get.table)
        assertEquals(expectedSchema, fileScan.requiredSchema, hint)

      // MOR
      case ProjectExec(_, dataScan: RowDataSourceScanExec) =>
        // NOTE: This is temporary solution to assert for Spark 2.4, until it's deprecated
        val explainedPlan = explain(selectDF.queryExecution.logical)
        assertTrue(explainedPlan.contains(expectedReadSchemaClause))

        assertEquals(tableName, dataScan.tableIdentifier.get.table)
        assertEquals(expectedSchema, dataScan.requiredSchema, hint)
    }
  }

  private def nestedFieldOf(selectDF: DataFrame, fieldName: String): StructType = {
    // NOTE: We're disabling WCE to simplify resulting plan
    spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, false)

    val requiredSchema = selectDF.queryExecution.executedPlan match {
      case ProjectExec(_, fileScan: FileSourceScanExec) => fileScan.requiredSchema
      case ProjectExec(_, dataScan: RowDataSourceScanExec) => dataScan.requiredSchema
      case fileScan: FileSourceScanExec => fileScan.requiredSchema
      case dataScan: RowDataSourceScanExec => dataScan.requiredSchema
    }
    requiredSchema(fieldName).dataType.asInstanceOf[StructType]
  }

  private def createTableWithComplexNestedSchema(tableName: String,
                                                 tablePath: String): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName USING HUDI TBLPROPERTIES (
         |  type = 'mor',
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.populate.meta.fields = 'false'
         |)
         |LOCATION '$tablePath'
         |AS SELECT
         |  1 AS id,
         |  named_struct('name', 'a1', 'price', 10) AS item,
         |  array(named_struct('k', 'k0', 'v', 'v0')) AS tags,
         |  map('m0', named_struct('a', 1, 'b', 2)) AS props,
         |  123456 AS ts
             """.stripMargin)
  }

  private def createTableWithNestedStructSchema(tableType: String,
                                                tableName: String,
                                                tablePath: String,
                                                opts: Map[String, String] = Map.empty): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE $tableName USING HUDI TBLPROPERTIES (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  hoodie.populate.meta.fields = 'false'
         |  ${if (opts.nonEmpty) "," + opts.map{ case (k, v) => s"'$k' = '$v'" }.mkString(",") else ""}
         |)
         |LOCATION '$tablePath'
         |AS SELECT 1 AS id, named_struct('name', 'a1', 'price', 10) AS item, 123456 AS ts
             """.stripMargin)
  }
}
