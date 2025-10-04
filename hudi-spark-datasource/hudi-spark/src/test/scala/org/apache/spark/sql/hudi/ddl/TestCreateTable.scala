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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.PartitionPathEncodeUtils.escapePathName
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.testutils.Assertions
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.{disableComplexKeygenValidation, getLastCommitMetadata}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

import scala.collection.JavaConverters._

class TestCreateTable extends HoodieSparkSqlTestBase {

  test("Test Create Managed Hoodie Table") {
    val databaseName = "hudi_database"
    spark.sql(s"create database if not exists $databaseName")
    spark.sql(s"use $databaseName")

    val tableName = generateTableName
    // Create a managed table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | tblproperties (
         |   hoodie.database.name = "databaseName",
         |   hoodie.table.name = "tableName",
         |   primaryKey = 'id',
         |   orderingFields = 'ts',
         |   hoodie.datasource.write.operation = 'upsert'
         | )
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    assertResult(tableName)(table.identifier.table)
    assertResult("hudi")(table.provider.get)
    assertResult(CatalogTableType.MANAGED)(table.tableType)
    assertResult(
      HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
        ++ Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("price", DoubleType),
        StructField("ts", LongType))
    )(table.schema.fields)
    assertFalse(table.properties.contains(HoodieTableConfig.DATABASE_NAME.key()))
    assertFalse(table.properties.contains(HoodieTableConfig.NAME.key()))
    assertFalse(table.properties.contains(OPERATION.key()))

    val tablePath = table.storage.properties("path")
    val metaClient = createMetaClient(spark, tablePath)
    val tableConfig = metaClient.getTableConfig
    assertResult(databaseName)(tableConfig.getDatabaseName)
    assertResult(tableName)(tableConfig.getTableName)
    assertFalse(tableConfig.contains(OPERATION.key()))

    val schemaOpt = tableConfig.getTableCreateSchema
    assertTrue(schemaOpt.isPresent, "Table create schema should be persisted")
    assertFalse(schemaOpt.get().getFields.asScala.exists(f => HoodieRecord.HOODIE_META_COLUMNS.contains(f.name())),
      "Table create schema should not include metadata fields")

    spark.sql("use default")
  }

  test("Test Create Hoodie Table With Options") {
    val tableName = generateTableName
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  dt string
         | ) using hudi
         | partitioned by (dt)
         | options (
         |   hoodie.database.name = "databaseName",
         |   hoodie.table.name = "tableName",
         |   primaryKey = 'id',
         |   orderingFields = 'ts',
         |   hoodie.datasource.write.operation = 'upsert'
         | )
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    assertResult(table.properties("type"))("cow")
    assertResult(table.properties("primaryKey"))("id")
    assertResult(table.properties("orderingFields"))("ts")
    assertResult(tableName)(table.identifier.table)
    assertResult("hudi")(table.provider.get)
    assertResult(CatalogTableType.MANAGED)(table.tableType)
    assertResult(
      HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
        ++ Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("price", DoubleType),
        StructField("ts", LongType),
        StructField("dt", StringType))
    )(table.schema.fields)
    assertFalse(table.properties.contains(HoodieTableConfig.DATABASE_NAME.key()))
    assertFalse(table.properties.contains(HoodieTableConfig.NAME.key()))
    assertFalse(table.properties.contains(OPERATION.key()))

    val tablePath = table.storage.properties("path")
    val metaClient = createMetaClient(spark, tablePath)
    val tableConfig = metaClient.getTableConfig.getProps.asScala.toMap
    assertResult(true)(tableConfig.contains(HoodieTableConfig.CREATE_SCHEMA.key))
    assertResult("dt")(tableConfig(HoodieTableConfig.PARTITION_FIELDS.key))
    assertResult("id")(tableConfig(HoodieTableConfig.RECORDKEY_FIELDS.key))
    assertResult("ts")(tableConfig(HoodieTableConfig.ORDERING_FIELDS.key))
    assertResult(KeyGeneratorType.SIMPLE.name())(tableConfig(HoodieTableConfig.KEY_GENERATOR_TYPE.key))
    assertResult("default")(tableConfig(HoodieTableConfig.DATABASE_NAME.key()))
    assertResult(tableName)(tableConfig(HoodieTableConfig.NAME.key()))
    assertFalse(tableConfig.contains(OPERATION.key()))
  }

  test("Test Create External Hoodie Table") {
    withTempDir { tmp =>
      // Test create cow table.
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id,name',
           |  type = 'cow'
           | )
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))

      assertResult(tableName)(table.identifier.table)
      assertResult("hudi")(table.provider.get)
      assertResult(CatalogTableType.EXTERNAL)(table.tableType)
      assertResult(
        HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
          ++ Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("price", DoubleType),
          StructField("ts", LongType))
      )(table.schema.fields)
      assertResult(table.properties("type"))("cow")
      assertResult(table.properties("primaryKey"))("id,name")

      spark.sql(s"drop table $tableName")
      // Test create mor partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | partitioned by (dt)
           | tblproperties (
           |  primaryKey = 'id',
           |  type = 'mor'
           | )
           | location '${tmp.getCanonicalPath}/h0'
       """.stripMargin)
      val table2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assertResult(table2.properties("type"))("mor")
      assertResult(table2.properties("primaryKey"))("id")
      assertResult(Seq("dt"))(table2.partitionColumnNames)
      assertResult(classOf[HoodieParquetRealtimeInputFormat].getCanonicalName)(table2.storage.inputFormat.get)

      // Test create a external table with an existing table in the path
      val tableName3 = generateTableName
      spark.sql(
        s"""
           |create table $tableName3
           |using hudi
           |location '${tmp.getCanonicalPath}/h0'
         """.stripMargin)
      val table3 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName3))
      assertResult(table3.properties("type"))("mor")
      assertResult(table3.properties("primaryKey"))("id")
      assertResult(
        HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
          ++ Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("price", DoubleType),
          StructField("ts", LongType),
          StructField("dt", StringType)
        )
      )(table3.schema.fields)
    }
  }

  test("Test Table Column Validate") {
    withTempDir { tmp =>
      val tableName = generateTableName
      assertThrows[IllegalArgumentException] {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id1',
             |  type = 'cow'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      }

      assertThrows[IllegalArgumentException] {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts1',
             |  type = 'cow'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      }

      assertThrows[IllegalArgumentException] {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  type = 'cow1'
             | )
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      }
    }
  }

  test("Test Create Table As Select") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        // Create Non-Partitioned table
        val tableName1 = generateTableName
        spark.sql(
          s"""
             | create table $tableName1 using hudi
             | tblproperties(
             |    primaryKey = 'id',
             |    type = '$tableType'
             | )
             | location '${tmp.getCanonicalPath}/$tableName1'
             | AS
             | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
       """.stripMargin)

        assertResult(WriteOperationType.BULK_INSERT) {
          getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName1").getOperationType
        }
        checkAnswer(s"select id, name, price, ts from $tableName1")(
          Seq(1, "a1", 10.0, 1000)
        )

        // Create Partitioned table
        val tableName2 = generateTableName
        spark.sql(
          s"""
             | create table $tableName2 using hudi
             | partitioned by (dt)
             | tblproperties(
             |    primaryKey = 'id',
             |    type = '$tableType'
             | )
             | location '${tmp.getCanonicalPath}/$tableName2'
             | AS
             | select 1 as id, 'a1' as name, 10 as price, '2021-04-01' as dt
         """.stripMargin
        )

        assertResult(WriteOperationType.BULK_INSERT) {
          getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName2").getOperationType
        }
        checkAnswer(s"select id, name, price, dt from $tableName2")(
          Seq(1, "a1", 10, "2021-04-01")
        )

        // Create Partitioned table with timestamp data type
        val tableName3 = generateTableName
        // CTAS failed with null primaryKey
        assertThrows[Exception] {
          spark.sql(
            s"""
               | create table $tableName3 using hudi
               | partitioned by (dt)
               | tblproperties(
               |    primaryKey = 'id',
               |    type = '$tableType'
               | )
               | location '${tmp.getCanonicalPath}/$tableName3'
               | AS
               | select null as id, 'a1' as name, 10 as price, '2021-05-07' as dt
               |
             """.stripMargin
          )
        }
        // Create table with timestamp type partition
        spark.sql(
          s"""
             | create table $tableName3 using hudi
             | partitioned by (dt)
             | tblproperties(
             |    primaryKey = 'id',
             |    type = '$tableType'
             | )
             | location '${tmp.getCanonicalPath}/$tableName3'
             | AS
             | select cast('2021-05-06 00:00:00' as timestamp) as dt, 1 as id, 'a1' as name, 10 as
             | price
         """.stripMargin
        )

        assertResult(WriteOperationType.BULK_INSERT) {
          getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName3").getOperationType
        }
        checkAnswer(s"select id, name, price, cast(dt as string) from $tableName3")(
          Seq(1, "a1", 10, "2021-05-06 00:00:00")
        )

        // Create table with date type partition
        val tableName4 = generateTableName
        spark.sql(
          s"""
             | create table $tableName4 using hudi
             | partitioned by (dt)
             | tblproperties(
             |    primaryKey = 'id',
             |    type = '$tableType'
             | )
             | location '${tmp.getCanonicalPath}/$tableName4'
             | AS
             | select cast('2021-05-06' as date) as dt, 1 as id, 'a1' as name, 10 as
             | price
         """.stripMargin
        )

        assertResult(WriteOperationType.BULK_INSERT) {
          getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName4").getOperationType
        }
        checkAnswer(s"select id, name, price, cast(dt as string) from $tableName4")(
          Seq(1, "a1", 10, "2021-05-06")
        )
      }
    }
  }

  test("Test create table like") {
    // 1. Test create table from an existing HUDI table
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { sourceTable =>
          spark.sql(
            s"""
               |create table $sourceTable (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               |  primaryKey = 'id,name',
               |  type = '$tableType'
               | )
               | location '${tmp.getCanonicalPath}/$sourceTable'""".stripMargin)

          // 1.1 Test Managed table
          withTable(generateTableName) { targetTable =>
            spark.sql(
              s"""
                 |create table $targetTable
                 |like $sourceTable
                 |using hudi""".stripMargin)

            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(targetTable))

            assertResult(targetTable)(table.identifier.table)
            assertResult("hudi")(table.provider.get)
            assertResult(CatalogTableType.MANAGED)(table.tableType)
            assertResult(
              HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
                ++ Seq(
                StructField("id", IntegerType),
                StructField("name", StringType),
                StructField("price", DoubleType),
                StructField("ts", LongType))
            )(table.schema.fields)
            assertResult(tableType)(table.properties("type"))
            assertResult("id,name")(table.properties("primaryKey"))

            // target table already exist
            assertThrows[IllegalArgumentException] {
              spark.sql(
                s"""
                   |create table $targetTable
                   |like $sourceTable
                   |using hudi""".stripMargin)
            }

            // should ignore if the table already exist
            spark.sql(
              s"""
                 |create table if not exists $targetTable
                 |like $sourceTable
                 |using hudi""".stripMargin)
          }

          // 1.2 Test External table
          withTable(generateTableName) { targetTable =>
            spark.sql(
              s"""
                 |create table $targetTable
                 |like $sourceTable
                 |using hudi
                 |location '${tmp.getCanonicalPath}/$targetTable'""".stripMargin)
            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(targetTable))
            assertResult(CatalogTableType.EXTERNAL)(table.tableType)
          }


          // 1.3 New target table options should override source table's
          withTable(generateTableName) { targetTable =>
            spark.sql(
              s"""
                 |create table $targetTable
                 |like $sourceTable
                 |using hudi
                 |tblproperties (primaryKey = 'id')""".stripMargin)
            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(targetTable))
            assertResult("id")(table.properties("primaryKey"))
          }
        }
      }
    }

    // 2. Test create table from an existing non-HUDI table
    withTempDir { tmp =>
      withTable(generateTableName) { sourceTable =>
        spark.sql(
          s"""
             |create table $sourceTable (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using parquet
             | tblproperties (
             |  non.hoodie.property='value'
             | )
             | location '${tmp.getCanonicalPath}/$sourceTable'""".stripMargin)

        withTable(generateTableName) { targetTable =>
          spark.sql(
            s"""
               |create table $targetTable
               |like $sourceTable
               |using hudi
               |tblproperties (
               | primaryKey = 'id,name',
               | type = 'cow'
               |)""".stripMargin)
          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(targetTable))

          assertResult(targetTable)(table.identifier.table)
          assertResult("hudi")(table.provider.get)
          assertResult(CatalogTableType.MANAGED)(table.tableType)
          assertResult(
            HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
              ++ Seq(
              StructField("id", IntegerType),
              StructField("name", StringType),
              StructField("price", DoubleType),
              StructField("ts", LongType))
          )(table.schema.fields)

          // Should not include non.hoodie.property
          assertResult(4)(table.properties.size)
          assertResult("cow")(table.properties("type"))
          assertResult("id,name")(table.properties("primaryKey"))
          assertResult("hudi")(table.properties("provider"))
        }
      }
    }
  }

  test("Test Create Table As Select With Auto record key gen") {
    withTempDir { tmp =>
      // Create Non-Partitioned table
      val tableName1 = generateTableName
      spark.sql(
        s"""
           | create table $tableName1 using hudi
           | tblproperties(
           |    type = 'cow'
           | )
           | location '${tmp.getCanonicalPath}/$tableName1'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
       """.stripMargin)

      assertResult(WriteOperationType.BULK_INSERT) {
        getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName1").getOperationType
      }
      checkAnswer(s"select id, name, price, ts from $tableName1")(
        Seq(1, "a1", 10.0, 1000)
      )

      // Create Partitioned table
      val tableName2 = generateTableName
      spark.sql(
        s"""
           | create table $tableName2 using hudi
           | partitioned by (dt)
           | tblproperties(
           |    type = 'cow'
           | )
           | location '${tmp.getCanonicalPath}/$tableName2'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, '2021-04-01' as dt
         """.stripMargin
      )

      assertResult(WriteOperationType.BULK_INSERT) {
        getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName2").getOperationType
      }
      checkAnswer(s"select id, name, price, dt from $tableName2")(
        Seq(1, "a1", 10, "2021-04-01")
      )
    }
  }

  test("Test Create Table As Select For existing table path") {
    val tableName1 = generateTableName
    spark.sql(
      s"""
         |create table $tableName1 (
         |  id int,
         |  name string,
         |  ts long
         |) using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |""".stripMargin)
    val tableLocation = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName1)).location
    spark.sql(s"drop table $tableName1")

    val tableName2 = generateTableName
    spark.sql(
      s"""
         |create table $tableName2 (
         |  id int,
         |  name string,
         |  ts long
         |) using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         |)
         |location '$tableLocation'
     """.stripMargin)

    checkExceptionContain(
      s"""
         |create table $tableName1
         |using hudi
         |as select * from $tableName2
         |""".stripMargin
    )("Can not create the managed table")
    assertResult(true)(existsPath(tableLocation.toString))
  }

  test("Test Create ro/rt Table In The Right Way") {
    withTempDir { tmp =>
      val parentPath = tmp.getCanonicalPath
      val tableName1 = generateTableName
      spark.sql("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + "=upsert")
      spark.sql(
        s"""
           |create table $tableName1 (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  type = 'mor'
           | )
           | location '$parentPath/$tableName1'
       """.stripMargin)
      spark.sql(s"insert into $tableName1 values (1, 'a1', 1000)")
      spark.sql(s"insert into $tableName1 values (1, 'a2', 1100)")

      // drop ro and rt table, and recreate them
      val roTableName1 = tableName1 + "_ro"
      val rtTableName1 = tableName1 + "_rt"
      spark.sql(
        s"""
           |create table $roTableName1
           |using hudi
           |tblproperties (
           | 'hoodie.query.as.ro.table' = 'true'
           |)
           |location '$parentPath/$tableName1'
           |""".stripMargin
      )
      val roCatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(roTableName1))
      assertResult(roCatalogTable.properties("type"))("mor")
      assertResult(roCatalogTable.properties("primaryKey"))("id")
      assertResult(roCatalogTable.properties("orderingFields"))("ts")
      assertResult(roCatalogTable.storage.properties("hoodie.query.as.ro.table"))("true")
      checkAnswer(s"select id, name, ts from $roTableName1")(
        Seq(1, "a1", 1000)
      )

      spark.sql(
        s"""
           |create table $rtTableName1
           |using hudi
           |tblproperties (
           | 'hoodie.query.as.ro.table' = 'false'
           |)
           |location '$parentPath/$tableName1'
           |""".stripMargin
      )
      val rtCatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(rtTableName1))
      assertResult(rtCatalogTable.properties("type"))("mor")
      assertResult(rtCatalogTable.properties("primaryKey"))("id")
      assertResult(rtCatalogTable.properties("orderingFields"))("ts")
      assertResult(rtCatalogTable.storage.properties("hoodie.query.as.ro.table"))("false")
      checkAnswer(s"select id, name, ts from $rtTableName1")(
        Seq(1, "a2", 1100)
      )
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }

  test("Test Create ro/rt Table In The Wrong Way") {
    withTempDir { tmp =>
      val parentPath = tmp.getCanonicalPath

      // test the case that create rt/rt table on cow table
      val tableName1 = generateTableName
      spark.sql(
        s"""
           |create table $tableName1 (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  type = 'cow'
           | )
           | location '$parentPath/$tableName1'
     """.stripMargin)
      spark.sql(s"insert into $tableName1 values (1, 'a1', 1000)")
      spark.sql(s"insert into $tableName1 values (1, 'a2', 1100)")

      val roTableName1 = tableName1 + "_ro"
      checkExceptionContain(
        s"""
           |create table $roTableName1
           |using hudi
           |tblproperties (
           | 'hoodie.query.as.ro.table' = 'true'
           |)
           |location '$parentPath/$tableName1'
           |""".stripMargin
      )("Creating ro/rt table should only apply to a mor table.")

      // test the case that create rt/rt table on a nonexistent table
      val tableName2 = generateTableName
      val rtTableName2 = tableName2 + "_rt"
      checkExceptionContain(
        s"""
           |create table $rtTableName2
           |using hudi
           |tblproperties (
           | 'hoodie.query.as.ro.table' = 'true'
           |)
           |location '$parentPath/$tableName2'
           |""".stripMargin
      )("Creating ro/rt table need the existence of the base table.")

      // test the case that CTAS
      val tableName3 = generateTableName
      checkExceptionContain(
        s"""
           | create table $tableName3 using hudi
           | tblproperties(
           |    primaryKey = 'id',
           |    orderingFields = 'ts',
           |    type = 'mor',
           |    'hoodie.query.as.ro.table' = 'true'
           | )
           | location '$parentPath/$tableName3'
           | AS
           | select 1 as id, 'a1' as name, 1000 as ts
           | """.stripMargin
      )("Not support CTAS for the ro/rt table")
    }
  }

  test("Test Create Table As Select With Tblproperties For Filter Props") {
    Seq("cow", "mor").foreach { tableType =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName using hudi
           | partitioned by (dt)
           | tblproperties(
           |    hoodie.database.name = "databaseName",
           |    hoodie.table.name = "tableName",
           |    primaryKey = 'id',
           |    orderingFields = 'ts',
           |    hoodie.datasource.write.operation = 'upsert',
           |    type = '$tableType'
           | )
           | AS
           | select 1 as id, 'a1' as name, 10 as price, '2021-04-01' as dt, 1000 as ts
         """.stripMargin
      )
      checkAnswer(s"select id, name, price, dt from $tableName")(
        Seq(1, "a1", 10, "2021-04-01")
      )
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assertFalse(table.properties.contains(HoodieTableConfig.DATABASE_NAME.key()))
      assertFalse(table.properties.contains(HoodieTableConfig.NAME.key()))
      assertFalse(table.properties.contains(OPERATION.key()))

      val tablePath = table.storage.properties("path")
      val metaClient = createMetaClient(spark, tablePath)
      val tableConfig = metaClient.getTableConfig.getProps.asScala.toMap
      assertResult("default")(tableConfig(HoodieTableConfig.DATABASE_NAME.key()))
      assertResult(tableName)(tableConfig(HoodieTableConfig.NAME.key()))
      assertFalse(tableConfig.contains(OPERATION.key()))
    }
  }

  test("Test Create Table As Select With Options For Filter Props") {
    Seq("cow", "mor").foreach { tableType =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName using hudi
           | partitioned by (dt)
           | options(
           |    hoodie.database.name = "databaseName",
           |    hoodie.table.name = "tableName",
           |    primaryKey = 'id',
           |    orderingFields = 'ts',
           |    hoodie.datasource.write.operation = 'upsert',
           |    type = '$tableType'
           | )
           | AS
           | select 1 as id, 'a1' as name, 10 as price, '2021-04-01' as dt, 1000 as ts
         """.stripMargin
      )
      checkAnswer(s"select id, name, price, dt from $tableName")(
        Seq(1, "a1", 10, "2021-04-01")
      )
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assertFalse(table.properties.contains(HoodieTableConfig.DATABASE_NAME.key()))
      assertFalse(table.properties.contains(HoodieTableConfig.NAME.key()))
      assertFalse(table.properties.contains(OPERATION.key()))

      val tablePath = table.storage.properties("path")
      val metaClient = createMetaClient(spark, tablePath)
      val tableConfig = metaClient.getTableConfig.getProps.asScala.toMap
      assertResult("default")(tableConfig(HoodieTableConfig.DATABASE_NAME.key()))
      assertResult(tableName)(tableConfig(HoodieTableConfig.NAME.key()))
      assertFalse(tableConfig.contains(OPERATION.key()))
    }
  }

  test("Test Create Table As Select when 'spark.sql.datetime.java8API.enabled' enables") {
    try {
      // enable spark.sql.datetime.java8API.enabled
      // and use java.time.Instant to replace java.sql.Timestamp to represent TimestampType.
      spark.conf.set("spark.sql.datetime.java8API.enabled", value = true)

      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName
           |using hudi
           |partitioned by(dt)
           |options(type = 'cow', primaryKey = 'id')
           |as
           |select 1 as id, 'a1' as name, 10 as price, cast('2021-05-07 00:00:00' as timestamp) as dt
           |""".stripMargin
      )

      checkAnswer(s"select id, name, price, cast(dt as string) from $tableName")(
        Seq(1, "a1", 10, "2021-05-07 00:00:00")
      )

    } finally {
      spark.conf.set("spark.sql.datetime.java8API.enabled", value = false)
    }
  }

  test("Test Create Table From Existing Hoodie Table") {
    withTempDir { tmp =>
      val databaseName = "hudi_database"
      spark.sql(s"create database if not exists $databaseName")
      spark.sql(s"use $databaseName")

      Seq("2021-08-02", "2021/08/02").foreach { partitionValue =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        import spark.implicits._
        val df = Seq((1, "a1", 10, 1000, partitionValue)).toDF("id", "name", "value", "ts", "dt")
        // Write a table by spark dataframe.
        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, s"original_$tableName")
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // Create a table over the existing table.
        // Fail to create table if only specify partition columns, no table schema.
        checkExceptionContain(
          s"""
             |create table $tableName using hudi
             |partitioned by (dt)
             |location '$tablePath'
             |""".stripMargin
        )("It is not allowed to specify partition columns when the table schema is not defined.")

        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             |""".stripMargin)
        checkAnswer(s"select id, name, value, ts, dt from $tableName")(
          Seq(1, "a1", 10, 1000, partitionValue)
        )
        // Check the missing properties for spark sql
        val metaClient = createMetaClient(spark, tablePath)
        val properties = metaClient.getTableConfig.getProps.asScala.toMap
        assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
        assertResult("dt")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
        assertResult("ts")(properties(HoodieTableConfig.ORDERING_FIELDS.key))
        assertResult("hudi_database")(metaClient.getTableConfig.getDatabaseName)
        assertResult(s"original_$tableName")(metaClient.getTableConfig.getTableName)

        // Test insert into
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, '$partitionValue')")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("1", partitionValue, 1, "a1", 10, 1000, partitionValue),
          Seq("2", partitionValue, 2, "a2", 10, 1000, partitionValue)
        )
        // Test merge into
        spark.sql(
          s"""
             |merge into $tableName h0
             |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts, '$partitionValue' as dt) s0
             |on h0.id = s0.id
             |when matched then update set *
             |""".stripMargin)
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("1", partitionValue, 1, "a1", 11, 1001, partitionValue),
          Seq("2", partitionValue, 2, "a2", 10, 1000, partitionValue)
        )
        // Test update
        spark.sql(s"update $tableName set value = value + 1 where id = 2")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("1", partitionValue, 1, "a1", 11, 1001, partitionValue),
          Seq("2", partitionValue, 2, "a2", 11, 1000, partitionValue)
        )
        // Test delete
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, dt from $tableName order by id")(
          Seq("2", partitionValue, 2, "a2", 11, 1000, partitionValue)
        )
      }
    }
  }

  test("Test Create Table From Existing Hoodie Table For Multi-Level Partitioned Table") {
    withTempDir { tmp =>
      Seq("2021-08-02", "2021/08/02").foreach { day =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        import spark.implicits._
        val df = Seq((1, "a1", 10, 1000, day, 12)).toDF("id", "name", "value", "ts", "day", "hh")
        // Write a table by spark dataframe.
        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "day,hh")
          .option(URL_ENCODE_PARTITIONING.key, "true")
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // Create a table over the existing table.
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             |""".stripMargin)
        checkAnswer(s"select _hoodie_record_key, id, name, value, ts, day, hh from $tableName")(
          Seq("id:1", 1, "a1", 10, 1000, day, 12)
        )
        // Check the missing properties for spark sql
        val metaClient = createMetaClient(spark, tablePath)
        val properties = metaClient.getTableConfig.getProps.asScala.toMap
        assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
        assertResult("day,hh")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
        assertResult("ts")(properties(HoodieTableConfig.ORDERING_FIELDS.key))

        val escapedPathPart = escapePathName(day)

        val query = s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, day, hh from $tableName order by id"
        // Test insert into
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, '$day', 12)")
        checkAnswer(query)(
          Seq("id:1", s"$escapedPathPart/12", 1, "a1", 10, 1000, day, 12),
          Seq("id:2", s"$escapedPathPart/12", 2, "a2", 10, 1000, day, 12))

        // Test merge into
        spark.sql(
          s"""
             |merge into $tableName h0
             |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts, '$day' as day, 12 as hh) s0
             |on h0.id = s0.id
             |when matched then update set *
             |""".stripMargin)
        checkAnswer(query)(
          Seq("id:1", s"$escapedPathPart/12", 1, "a1", 11, 1001, day, 12),
          Seq("id:2", s"$escapedPathPart/12", 2, "a2", 10, 1000, day, 12))

        // Test update
        spark.sql(s"update $tableName set value = value + 1 where id = 2")
        checkAnswer(query)(
          Seq("id:1", s"$escapedPathPart/12", 1, "a1", 11, 1001, day, 12),
          Seq("id:2", s"$escapedPathPart/12", 2, "a2", 11, 1000, day, 12))

        // Test delete
        spark.sql(s"delete from $tableName where id = 1")
        checkAnswer(query)(
          Seq("id:2", s"$escapedPathPart/12", 2, "a2", 11, 1000, day, 12))
      }
    }
  }

  test("Test Create Table with Complex Key Generator and Key Encoding") {
    withTempDir { tmp =>
      Seq((false, 6), (true, 6), (false, 8), (true, 8), (false, 9), (true, 9)).foreach { params =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        val encodeSingleKeyFieldValue = params._1
        val tableVersion = params._2
        import spark.implicits._
        // The COMPLEX_KEYGEN_NEW_ENCODING config only works for table version 8 and below
        val keyPrefix = if (encodeSingleKeyFieldValue && tableVersion < 9) "" else "id:"
        val df = Seq((1, "a1", 10, 1000, "2025-07-29", 12)).toDF("id", "name", "value", "ts", "day", "hh")
        // Write a table by spark dataframe.
        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(ORDERING_FIELDS.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "day,hh")
          .option(URL_ENCODE_PARTITIONING.key, "true")
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key, tableVersion.toString)
          .option(
            HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key,
            encodeSingleKeyFieldValue.toString)
          .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, (tableVersion >= 9).toString)
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // Create a table over the existing table.
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             |""".stripMargin)
        checkAnswer(s"select _hoodie_record_key, id, name, value, ts, day, hh from $tableName")(
          Seq(keyPrefix + "1", 1, "a1", 10, 1000, "2025-07-29", 12)
        )
        spark.sql(
          s"""
             |ALTER TABLE $tableName
             |SET TBLPROPERTIES (hoodie.write.complex.keygen.new.encoding = '$encodeSingleKeyFieldValue',
             | hoodie.write.table.version = '$tableVersion')
             |""".stripMargin)
        // Check the missing properties for spark sql
        val metaClient = createMetaClient(spark, tablePath)
        val properties = metaClient.getTableConfig.getProps.asScala.toMap
        assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
        assertResult("day,hh")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
        assertResult("ts")(properties(HoodieTableConfig.ORDERING_FIELDS.key))

        val query = s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, day, hh from $tableName order by id"

        // Test insert into
        writeAndValidateWithComplexKeyGenerator(
          spark, tableVersion, tableName,
          s"insert into $tableName values(2, 'a2', 10, 1000, '2025-07-29', 12)", query
        )(
          Seq(keyPrefix + "1", "2025-07-29/12", 1, "a1", 10, 1000, "2025-07-29", 12)
        )(
          Seq(keyPrefix + "1", "2025-07-29/12", 1, "a1", 10, 1000, "2025-07-29", 12),
          Seq(keyPrefix + "2", "2025-07-29/12", 2, "a2", 10, 1000, "2025-07-29", 12)
        )

        // Test merge into
        writeAndValidateWithComplexKeyGenerator(
          spark, tableVersion, tableName,
          s"""
             |merge into $tableName h0
             |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts, '2025-07-29' as day, 12 as hh) s0
             |on h0.id = s0.id
             |when matched then update set *
             |""".stripMargin,
          query
        )(
          Seq(keyPrefix + "1", "2025-07-29/12", 1, "a1", 10, 1000, "2025-07-29", 12),
          Seq(keyPrefix + "2", "2025-07-29/12", 2, "a2", 10, 1000, "2025-07-29", 12)
        )(
          Seq(keyPrefix + "1", "2025-07-29/12", 1, "a1", 11, 1001, "2025-07-29", 12),
          Seq(keyPrefix + "2", "2025-07-29/12", 2, "a2", 10, 1000, "2025-07-29", 12)
        )

        // Test update
        writeAndValidateWithComplexKeyGenerator(
          spark, tableVersion, tableName,
          s"update $tableName set value = value + 1 where id = 2", query
        )(
          Seq(keyPrefix + "1", "2025-07-29/12", 1, "a1", 11, 1001, "2025-07-29", 12),
          Seq(keyPrefix + "2", "2025-07-29/12", 2, "a2", 10, 1000, "2025-07-29", 12)
        )(
          Seq(keyPrefix + "1", "2025-07-29/12", 1, "a1", 11, 1001, "2025-07-29", 12),
          Seq(keyPrefix + "2", "2025-07-29/12", 2, "a2", 11, 1000, "2025-07-29", 12)
        )

        // Test delete
        writeAndValidateWithComplexKeyGenerator(
          spark, tableVersion, tableName,
          s"delete from $tableName where id = 1", query
        )(
          Seq(keyPrefix + "1", "2025-07-29/12", 1, "a1", 11, 1001, "2025-07-29", 12),
          Seq(keyPrefix + "2", "2025-07-29/12", 2, "a2", 11, 1000, "2025-07-29", 12)
        )(
          Seq(keyPrefix + "2", "2025-07-29/12", 2, "a2", 11, 1000, "2025-07-29", 12)
        )
      }
    }
  }

  test("Test Create Table with Complex Key Generator with multiple partition fields and record key fields") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      import spark.implicits._
      val df = Seq((1, "a1", 10, 1000, "2025-07-29", 12)).toDF("id", "name", "value", "ts", "day", "hh")
      // Write a table by spark dataframe.
      df.write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, tableName)
        .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
        .option(RECORDKEY_FIELD.key, "id,name")
        .option(ORDERING_FIELDS.key, "ts")
        .option(PARTITIONPATH_FIELD.key, "day,hh")
        .option(URL_ENCODE_PARTITIONING.key, "true")
        .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
        .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Create a table over the existing table.
      spark.sql(
        s"""
           |create table $tableName using hudi
           |location '$tablePath'
           |""".stripMargin)
      checkAnswer(s"select _hoodie_record_key, id, name, value, ts, day, hh from $tableName")(
        Seq("id:1,name:a1", 1, "a1", 10, 1000, "2025-07-29", 12)
      )
      // Check the missing properties for spark sql
      val metaClient = createMetaClient(spark, tablePath)
      val properties = metaClient.getTableConfig.getProps.asScala.toMap
      assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
      assertResult("id,name")(properties(HoodieTableConfig.RECORDKEY_FIELDS.key))
      assertResult("day,hh")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
      assertResult("ts")(properties(HoodieTableConfig.ORDERING_FIELDS.key))

      val query = s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts, day, hh from $tableName order by id"

      // Test insert into
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, '2025-07-29', 12)")
      checkAnswer(query)(
        Seq("id:1,name:a1", "2025-07-29/12", 1, "a1", 10, 1000, "2025-07-29", 12),
        Seq("id:2,name:a2", "2025-07-29/12", 2, "a2", 10, 1000, "2025-07-29", 12)
      )

      // Test merge into
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts, '2025-07-29' as day, 12 as hh) s0
           |on h0.id = s0.id
           |when matched then update set *
           |""".stripMargin)
      checkAnswer(query)(
        Seq("id:1,name:a1", "2025-07-29/12", 1, "a1", 11, 1001, "2025-07-29", 12),
        Seq("id:2,name:a2", "2025-07-29/12", 2, "a2", 10, 1000, "2025-07-29", 12)
      )

      // Test update
      spark.sql(s"update $tableName set value = value + 1 where id = 2")
      checkAnswer(query)(
        Seq("id:1,name:a1", "2025-07-29/12", 1, "a1", 11, 1001, "2025-07-29", 12),
        Seq("id:2,name:a2", "2025-07-29/12", 2, "a2", 11, 1000, "2025-07-29", 12)
      )

      // Test delete
      spark.sql(s"delete from $tableName where id = 1")
      checkAnswer(query)(
        Seq("id:2,name:a2", "2025-07-29/12", 2, "a2", 11, 1000, "2025-07-29", 12)
      )
    }
  }

  test("Test Create Table From Existing Hoodie Table For None Partitioned Table") {
    withTempDir { tmp =>
      // Write a table by spark dataframe.
      val tableName = generateTableName
      import spark.implicits._
      val df = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "ts")
      df.write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, tableName)
        .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
        .option(RECORDKEY_FIELD.key, "id")
        .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
        .option(PARTITIONPATH_FIELD.key, "")
        .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
        .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
        .mode(SaveMode.Overwrite)
        .save(tmp.getCanonicalPath)

      // Create a table over the existing table.
      spark.sql(
        s"""
           |create table $tableName using hudi
           |location '${tmp.getCanonicalPath}'
           |""".stripMargin)
      checkAnswer(s"select id, name, value, ts from $tableName")(
        Seq(1, "a1", 10, 1000)
      )
      // Check the missing properties for spark sql
      val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
      val properties = metaClient.getTableConfig.getProps.asScala.toMap
      assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
      assertResult("ts")(properties(HoodieTableConfig.ORDERING_FIELDS.key))

      // Test insert into
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, value, ts from $tableName order by id")(
        Seq("1", "", 1, "a1", 10, 1000),
        Seq("2", "", 2, "a2", 10, 1000)
      )
      // Test merge into
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (select 1 as id, 'a1' as name, 11 as value, 1001 as ts) s0
           |on h0.id = s0.id
           |when matched then update set *
           |""".stripMargin)
      checkAnswer(s"select id, name, value, ts from $tableName order by id")(
        Seq(1, "a1", 11, 1001),
        Seq(2, "a2", 10, 1000)
      )
      // Test update
      spark.sql(s"update $tableName set value = value + 1 where id = 2")
      checkAnswer(s"select id, name, value, ts from $tableName order by id")(
        Seq(1, "a1", 11, 1001),
        Seq(2, "a2", 11, 1000)
      )
      // Test delete
      spark.sql(s"delete from $tableName where id = 1")
      checkAnswer(s"select id, name, value, ts from $tableName order by id")(
        Seq(2, "a2", 11, 1000)
      )
    }
  }

  test("Test Create Table Existing In Catalog") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double
         |) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin
    )

    spark.sql(s"alter table $tableName add columns(ts bigint)")

    // Check "create table if not exists" works after schema evolution.
    spark.sql(
      s"""
         |create table if not exists $tableName (
         | id int,
         | name string,
         | price double
         |) using hudi
         |tblproperties(primaryKey = 'id')
         |""".stripMargin
    )
  }

  test("Test create table with comment") {
    val tableName = generateTableName
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | comment "This is a simple hudi table"
         | tblproperties (
         |   primaryKey = 'id',
         |   orderingFields = 'ts'
         | )
       """.stripMargin)
    val shown = spark.sql(s"show create table $tableName").head.getString(0)
    assertResult(true)(shown.contains("COMMENT 'This is a simple hudi table'"))
  }

  test("Test CTAS using an illegal definition -- a COW table with compaction enabled.") {
    val tableName = generateTableName
    checkExceptionContain(
      s"""
         | create table $tableName using hudi
         | tblproperties(
         |    primaryKey = 'id',
         |    type = 'cow',
         |    hoodie.compact.inline='true'
         | )
         | AS
         | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
         |""".stripMargin)("Compaction is not supported on a CopyOnWrite table")
    val dbPath = spark.sessionState.catalog.getDatabaseMetadata("default").locationUri.getPath
    val tablePath = s"${dbPath}/${tableName}"
    assertResult(false)(existsPath(tablePath))
  }

  test("Test Create Non-Hudi Table(Parquet Table)") {
    val databaseName = "test_database"
    spark.sql(s"create database if not exists $databaseName")
    spark.sql(s"use $databaseName")

    val tableName = generateTableName
    // Create a managed table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using parquet
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    assertResult(tableName)(table.identifier.table)
    assertResult("parquet")(table.provider.get)
    assertResult(CatalogTableType.MANAGED)(table.tableType)
    assertResult(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("price", DoubleType),
        StructField("ts", LongType))
    )(table.schema.fields)

    spark.sql("use default")
  }

  test("Test Infer KegGenClazz") {
    def checkKeyGenerator(targetGenerator: String, tableName: String) = {
      val tablePath = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).location.getPath
      val metaClient = createMetaClient(spark, tablePath)
      val realKeyGenerator = metaClient.getTableConfig.getKeyGeneratorClassName
      assertResult(targetGenerator)(realKeyGenerator)
    }

    val tableName = generateTableName

    // Test Nonpartitioned table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | comment "This is a simple hudi table"
         | tblproperties (
         |   primaryKey = 'id',
         |   orderingFields = 'ts'
         | )
       """.stripMargin)
    checkKeyGenerator("org.apache.hudi.keygen.NonpartitionedKeyGenerator", tableName)
    spark.sql(s"drop table $tableName")

    // Test single partitioned table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | comment "This is a simple hudi table"
         | partitioned by (ts)
         | tblproperties (
         |   primaryKey = 'id',
         |   orderingFields = 'ts'
         | )
       """.stripMargin)
    checkKeyGenerator("org.apache.hudi.keygen.SimpleKeyGenerator", tableName)
    spark.sql(s"drop table $tableName")

    // Test single partitioned dual record keys table
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using hudi
         | comment "This is a simple hudi table"
         | partitioned by (ts)
         | tblproperties (
         |   primaryKey = 'id,name',
         |   orderingFields = 'ts'
         | )
       """.stripMargin)
    checkKeyGenerator("org.apache.hudi.keygen.ComplexKeyGenerator", tableName)
    spark.sql(s"drop table $tableName")
  }

  test("Test CTAS not de-duplicating (by default)") {
    withRecordType() {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tableName USING hudi
             | LOCATION '${tmp.getCanonicalPath}/$tableName'
             | TBLPROPERTIES (
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
             | AS SELECT * FROM (
             |  SELECT 1 as id, 'a1' as name, 10 as price, 1000 as ts
             |  UNION ALL
             |  SELECT 1 as id, 'a1' as name, 11 as price, 1001 as ts
             | )
       """.stripMargin)

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000),
          Seq(1, "a1", 11.0, 1001)
        )
      }
    }
  }

  test("Test init HoodieCatalogTable class for non-Hudi table") {
    val tableName = generateTableName
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         | ) using parquet
       """.stripMargin)
    val exception = intercept[IllegalArgumentException] {
      spark.sql(s"""call show_commits(table => '$tableName', limit => 10)""").collect()
    }
    assertTrue(exception.getMessage.contains(s"""$tableName is not a Hudi table"""))
  }

  test("Test hoodie table schema consistency for non-Avro data types") {
    val tableName = generateTableName
    spark.sql(
      s"""
         | create table $tableName (
         |  id tinyint,
         |  name varchar(10),
         |  price double,
         |  ts long
         | ) using hudi
         | tblproperties (
         |   primaryKey = 'id',
         |   orderingFields = 'ts'
         | )
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    val hoodieCatalogTable = new HoodieCatalogTable(spark, table)
    val hoodieSchema = HoodieSqlCommonUtils.getTableSqlSchema(hoodieCatalogTable.metaClient, true)
    assertResult(hoodieSchema.get)(table.schema)
  }

  test("Test Create Hoodie Table With Options in different case") {
    val tableName = generateTableName
    spark.sql(
      s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  dt string
         | ) using hudi
         | partitioned by (dt)
         | options (
         |   hoodie.database.name = "databaseName",
         |   hoodie.table.name = "tableName",
         |   PRIMARYKEY = 'id',
         |   orderingFields = 'ts',
         |   hoodie.datasource.write.operation = 'upsert'
         | )
       """.stripMargin)
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    assertResult(table.properties("type"))("cow")
    assertResult(table.properties("primaryKey"))("id")
    assertResult(table.properties("orderingFields"))("ts")
  }

  test("Test Create Hoodie Table with existing hoodie.properties") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // drop the table without purging hdfs directory
      spark.sql(s"drop table $tableName".stripMargin)

      val tableSchemaAfterCreate1 = createMetaClient(spark, tablePath).getTableConfig.getTableCreateSchema

      // avro schema name and namespace should not change should not change
      spark.newSession().sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      val tableSchemaAfterCreate2 = createMetaClient(spark, tablePath).getTableConfig.getTableCreateSchema

      assertResult(tableSchemaAfterCreate1.get)(tableSchemaAfterCreate2.get)
    }
  }

  test("Test Create Hoodie Table with base file format") {
    // Parquet
    Seq("cow", "mor").foreach { tableType =>
      withTable(generateTableName) { tableName =>
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  orderingFields = 'ts',
             |  hoodie.table.base.file.format = 'PARQUET'
             | )
       """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
        assertResult(table.storage.serde.get)("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        assertResult(table.storage.inputFormat.get)(
          if (tableType.equals("mor")) "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat"
          else "org.apache.hudi.hadoop.HoodieParquetInputFormat")
        assertResult(table.storage.outputFormat.get)("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
      }
    }

    // Orc
    withTable(generateTableName) { tableName =>
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey ='id',
           |  type = 'cow',
           |  orderingFields = 'ts',
           |  hoodie.table.base.file.format = 'ORC'
           | )
       """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assertResult(table.storage.serde.get)("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
      assertResult(table.storage.inputFormat.get)("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
      assertResult(table.storage.outputFormat.get)("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
    }
  }

  test("Test Create Hoodie Table with table configs") {
    Seq("COPY_ON_WRITE", "MERGE_ON_READ").foreach { tableType =>
      withTable(generateTableName) { tableName =>
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             |  hoodie.table.recordkey.fields ='id',
             |  hoodie.table.type = '$tableType',
             |  hoodie.table.ordering.fields = 'ts'
             | )
       """.stripMargin)
        val hoodieCatalogTable = HoodieCatalogTable(spark, TableIdentifier(tableName))
        assertResult(Array("id"))(hoodieCatalogTable.primaryKeys)
        assertResult(tableType)(hoodieCatalogTable.tableTypeName)
        assertResult(java.util.Collections.singletonList[String]("ts"))(hoodieCatalogTable.orderingFields)
      }
    }
  }

  test("Test Create Hoodie Table With Multiple Partitions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}"
      // throws error if order in partition by different from that in create table
      assertThrows[IllegalArgumentException] {
        spark.sql(
          s"""
             | create table $tableName (
             |    ts BIGINT,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    city STRING,
             |    state STRING
             |) using hudi
             | options(
             |    primaryKey = 'id'
             |)
             |PARTITIONED BY (state, city)
             |location '$tablePath';
       """.stripMargin)
      }
      // otherwise successful
      spark.sql(
        s"""
           | create table $tableName (
           |    ts BIGINT,
           |    id STRING,
           |    rider STRING,
           |    driver STRING,
           |    fare DOUBLE,
           |    city STRING,
           |    state STRING
           |) using hudi
           | options(
           |    primaryKey = 'id'
           |)
           |PARTITIONED BY (city, state)
           |location '$tablePath';
       """.stripMargin)
      disableComplexKeygenValidation(spark, tableName)
      // insert and validate
      spark.sql(s"insert into $tableName values(1695332066,'trip3','rider-E','driver-O',93.50,'austin','texas')")
      checkAnswer(s"select ts, id, rider, driver, fare, city, state from $tableName")(
        Seq(1695332066, "trip3", "rider-E", "driver-O", 93.50, "austin", "texas")
      )
    }
  }

  test("Test Create Table In Inconsistent Schemes") {
    withTempDir { tmp =>
      val parentPath = tmp.getCanonicalPath

      // test the case that create same table after change schema
      val tableName1 = generateTableName
      spark.sql(
        s"""
           |create table $tableName1 (
           |  id int,
           |  name string,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  type = 'cow'
           | )
           | location '$parentPath/$tableName1'
     """.stripMargin)
      spark.sql(s"insert into $tableName1 values (1, 'a1', 1000)")
      spark.sql(s"insert into $tableName1 values (1, 'a2', 1100)")
      spark.sql(s"drop table $tableName1")

      checkExceptionContain(
        s"""
           |create table $tableName1 (
           |  id int,
           |  name map<string, string>,
           |  ts long
           |) using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts',
           |  type = 'cow'
           | )
           | location '$parentPath/$tableName1'
     """.stripMargin
      )("Failed to create catalog table in metastore")
    }
  }

  test("Test Create Table with Same Value for Partition and Precombine") {
    withSQLConf("hoodie.parquet.small.file.limit" -> "0",
      DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key -> "upsert") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          // simple partition path
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  orderingFields = 'ts'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(1, 'a2', 10, 1000)")
          checkAnswer(s"select id, name from $tableName")(
            Seq(1, "a2")
          )
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a2", 10.0, 1000)
          )

          // complex keygen, variable timestamp partition is precombine
          val tableName2 = generateTableName
          val basePath2 = s"${tmp.getCanonicalPath}/$tableName2"
          spark.sql(
            s"""
               |create table $tableName2 (
               |  id int,
               |  name string,
               |  price double,
               |  segment string,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  orderingFields = 'ts',
               |  'hoodie.datasource.write.partitionpath.field' = 'segment:simple,ts:timestamp',
               |  'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.CustomKeyGenerator',
               |  'hoodie.keygen.timebased.timestamp.type' = 'SCALAR',
               |  'hoodie.keygen.timebased.output.dateformat' = 'YYYY',
               |  'hoodie.keygen.timebased.timestamp.scalar.time.unit' = 'seconds'
               | )
               | partitioned by(segment,ts)
               | location '$basePath2'
       """.stripMargin)
          spark.sql(s"insert into $tableName2 values(1, 'a1', 10, 'seg1', 1000)")
          spark.sql(s"insert into $tableName2 values(1, 'a2', 10, 'seg1', 1000)")
          checkAnswer(s"select id, name from $tableName2")(
            Seq(1, "a2")
          )
          checkAnswer(s"select id, name, price, segment, ts from $tableName2")(
            Seq(1, "a2", 10.0, "seg1", 1000)
          )

          // complex keygen, simple partition is precombine
          val tableName3 = generateTableName
          val basePath3 = s"${tmp.getCanonicalPath}/$tableName3"
          spark.sql(
            s"""
               |create table $tableName3 (
               |  id int,
               |  name string,
               |  price double,
               |  segment string,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  orderingFields = 'segment',
               |  'hoodie.datasource.write.partitionpath.field' = 'segment:simple,ts:timestamp',
               |  'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.CustomKeyGenerator',
               |  'hoodie.keygen.timebased.timestamp.type' = 'SCALAR',
               |  'hoodie.keygen.timebased.output.dateformat' = 'YYYY',
               |  'hoodie.keygen.timebased.timestamp.scalar.time.unit' = 'seconds'
               | )
               | partitioned by(segment,ts)
               | location '$basePath3'
       """.stripMargin)
          spark.sql(s"insert into $tableName3 values(1, 'a1', 10, 'seg1', 1000)")
          spark.sql(s"insert into $tableName3 values(1, 'a2', 10, 'seg1', 1000)")
          checkAnswer(s"select id, name from $tableName3")(
            Seq(1, "a2")
          )
          checkAnswer(s"select id, name, price, segment, ts from $tableName3")(
            Seq(1, "a2", 10.0, "seg1", 1000)
          )
        }
      }
    }
  }

  def writeAndValidateWithComplexKeyGenerator(spark: SparkSession,
                                              tableVersion: Int,
                                              tableName: String,
                                              dmlToWrite: String,
                                              query: String)(
                                               expectedRowsBefore: Seq[Any]*)(expectedRowsAfter: Seq[Any]*): Unit = {
    if (tableVersion < 9) {
      // By default, the complex key generator validation is enabled and should throw exception on DML
      Assertions.assertComplexKeyGeneratorValidationThrows(() => spark.sql(dmlToWrite), "ingestion")
      // Query should still succeed
      checkAnswer(query)(expectedRowsBefore: _*)
      // Disabling the complex key generator validation should let write succeed
      HoodieSparkSqlTestBase.disableComplexKeygenValidation(spark, tableName)
    }
    spark.sql(dmlToWrite)
    HoodieSparkSqlTestBase.enableComplexKeygenValidation(spark, tableName)
    checkAnswer(query)(expectedRowsAfter: _*)
  }
}
