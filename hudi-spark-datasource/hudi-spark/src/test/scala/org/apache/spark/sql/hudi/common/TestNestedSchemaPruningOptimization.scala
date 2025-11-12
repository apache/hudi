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
            assertEquals(expectedSchema, requiredSchema, hint)

          // MOR
          case ProjectExec(_, dataScan: RowDataSourceScanExec) =>
            // NOTE: This is temporary solution to assert for Spark 2.4, until it's deprecated
            val explainedPlan = explain(selectDF.queryExecution.logical)
            assertTrue(explainedPlan.contains(expectedReadSchemaClause))

            val tableIdentifier = dataScan.tableIdentifier
            val requiredSchema = dataScan.requiredSchema

            assertEquals(tableName, tableIdentifier.get.table)
            assertEquals(expectedSchema, requiredSchema, hint)
        }

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
