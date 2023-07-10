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

package org.apache.hudi.functional

import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.spark.sql
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineUtils

import scala.collection.JavaConversions._

class TestInsertIntoOperation extends HoodieSparkSqlTestBase {

  /*

  /**
   * asserts if number of commits = count
   * returns true if last commit is bulk insert
   */
  def assertCommitCountAndIsLastBulkInsert(basePath: String, count: Int): Boolean = {
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf())
      .build()
    val timeline = metaClient.getActiveTimeline.getAllCommitsTimeline
    assert(timeline.countInstants() == count)
    val latestCommit = timeline.lastInstant()
    assert(latestCommit.isPresent)
    assert(latestCommit.get().isCompleted)
    val metadata = TimelineUtils.getCommitMetadata(latestCommit.get(), timeline)
    metadata.getOperationType.equals(WriteOperationType.BULK_INSERT)
  }

  def createTable(tableName: String, writeOptions: String, tableBasePath: String): Unit = {
    spark.sql(
      s"""
         | create table $tableName (
         |  timestamp long,
         |  _row_key string,
         |  rider string,
         |  driver string,
         |  begin_lat double,
         |  begin_lon double,
         |  end_lat double,
         |  end_lon double,
         |  fare STRUCT<
         |    amount: double,
         |    currency: string >,
         |  _hoodie_is_deleted boolean,
         |  partition_path string
         |) using hudi
         | partitioned by (partition_path)
         | $writeOptions
         | location '$tableBasePath'
         |
    """.stripMargin)
  }

  def generateInserts(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): sql.DataFrame = {
    val recs = dataGen.generateInsertsNestedExample(instantTime, n)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs), 2))
  }

  def doInsert(dataGen: HoodieTestDataGenerator, tableName: String, instantTime: String): Unit = {
    generateInserts(dataGen, instantTime, 100).select("timestamp", "_row_key", "rider", "driver",
      "begin_lat", "begin_lon", "end_lat", "end_lon", "fare", "_hoodie_is_deleted", "partition_path").
      createOrReplaceTempView("insert_temp_table")
    spark.sql(s"insert into $tableName select * from insert_temp_table")
  }

  test("No configs set") {
    withTempDir( { basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  primaryKey = '_row_key',
           |  preCombineField = 'timestamp',
           |  hoodie.database.name = "databaseName",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))

    })
  }

  test("No configs set pkless") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  preCombineField = 'timestamp',
           |  hoodie.database.name = "databaseName",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))

    })
  }

  test("No configs set pkless no precombine") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  hoodie.database.name = "databaseName",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))

    })
  }


  test("No configs set pkless cow") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  type = 'cow',
           |  hoodie.database.name = "databaseName",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))

    })
  }

  test("No configs set pkless mor") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  type = 'mor',
           |  hoodie.database.name = "databaseName",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))

    })
  }

  test("Set upsert sql op in tblproperties") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  primaryKey = '_row_key',
           |  preCombineField = 'timestamp',
           |  hoodie.database.name = "databaseName",
           |  hoodie.sql.insert.mode = "upsert",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))

    })
  }

  test("Set disable bulk insert in tblproperties") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  primaryKey = '_row_key',
           |  preCombineField = 'timestamp',
           |  hoodie.database.name = "databaseName",
           |  hoodie.sql.insert.mode = "upsert",
           |  hoodie.sql.bulk.insert.enable = "false",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))
    })
  }

  test("Test fallback to deduce") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  primaryKey = '_row_key',
           |  preCombineField = 'timestamp',
           |  hoodie.database.name = "databaseName",
           |  hoodie.sql.bulk.insert.enable = "true",
           |  hoodie.sql.insert.mode = "upsert",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      generateInserts(dataGen, "000", 100).select("timestamp", "_row_key", "rider", "driver",
        "begin_lat", "begin_lon", "end_lat", "end_lon", "fare", "_hoodie_is_deleted", "partition_path").
        createOrReplaceTempView("insert_temp_table")
      checkException(s"insert into $tableName select * from insert_temp_table")("Table with primaryKey can only use bulk insert in non-strict mode.")
    })
  }

  test("Set upsert sql op in conf") {
    withTempDir({ basePath =>
      try {
        val tableName = generateTableName
        val tableBasePath = basePath.getCanonicalPath + "/" + tableName
        val writeOptions =
          s"""
             |tblproperties (
             |  primaryKey = '_row_key',
             |  preCombineField = 'timestamp',
             |  hoodie.database.name = "databaseName",
             |  hoodie.table.name = "$tableName"
             | )""".stripMargin
        createTable(tableName, writeOptions, tableBasePath)
        spark.conf.set("hoodie.sql.insert.mode", "upsert")
        //Insert Operation
        val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
        doInsert(dataGen, tableName, "000")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
        doInsert(dataGen, tableName, "001")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))
        spark.conf.set("hoodie.sql.insert.mode", "non-strict")
        doInsert(dataGen, tableName, "002")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 3))
        spark.conf.set("hoodie.sql.insert.mode", "upsert")
        doInsert(dataGen, tableName, "003")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 4))
      } finally {
        spark.conf.unset("hoodie.sql.insert.mode")
      }
    })
  }

  test("Test no precombine") {
    withTempDir({ basePath =>
      try {
        val tableName = generateTableName
        val tableBasePath = basePath.getCanonicalPath + "/" + tableName
        val writeOptions =
          s"""
             |tblproperties (
             |  primaryKey = '_row_key',
             |  preCombineField = 'timestamp',
             |  hoodie.database.name = "databaseName",
             |  hoodie.table.name = "$tableName"
             | )""".stripMargin
        createTable(tableName, writeOptions, tableBasePath)
        spark.conf.set("hoodie.sql.insert.mode", "upsert")
        //Insert Operation
        val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
        doInsert(dataGen, tableName, "000")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
        doInsert(dataGen, tableName, "001")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))
        spark.conf.set("hoodie.sql.insert.mode", "non-strict")
        doInsert(dataGen, tableName, "002")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 3))
        spark.conf.set("hoodie.sql.insert.mode", "upsert")
        doInsert(dataGen, tableName, "003")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 4))
      } finally {
        spark.conf.unset("hoodie.sql.insert.mode")
      }
    })
  }

  test("Set non-strict sql op in tblproperties") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  primaryKey = '_row_key',
           |  preCombineField = 'timestamp',
           |  hoodie.database.name = "databaseName",
           |  hoodie.sql.insert.mode = "non-strict",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))

    })
  }

  test("Set non-strict sql op in conf") {
    withTempDir({ basePath =>
      try {
        val tableName = generateTableName
        val tableBasePath = basePath.getCanonicalPath + "/" + tableName
        val writeOptions =
          s"""
             |tblproperties (
             |  primaryKey = '_row_key',
             |  preCombineField = 'timestamp',
             |  hoodie.database.name = "databaseName",
             |  hoodie.table.name = "$tableName"
             | )""".stripMargin
        createTable(tableName, writeOptions, tableBasePath)
        spark.conf.set("hoodie.sql.insert.mode", "non-strict")
        //Insert Operation
        val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
        doInsert(dataGen, tableName, "000")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
        doInsert(dataGen, tableName, "001")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))
        spark.conf.set("hoodie.sql.insert.mode", "upsert")
        doInsert(dataGen, tableName, "002")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 3))
        spark.conf.set("hoodie.sql.insert.mode", "non-strict")
        doInsert(dataGen, tableName, "003")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 4))
      } finally {
        spark.conf.unset("hoodie.sql.insert.mode")
      }
    })
  }

  test("Set bulk insert enable sql op in conf") {
    withTempDir({ basePath =>
      try {
        val tableName = generateTableName
        val tableBasePath = basePath.getCanonicalPath + "/" + tableName
        val writeOptions =
          s"""
             |tblproperties (
             |  primaryKey = '_row_key',
             |  preCombineField = 'timestamp',
             |  hoodie.database.name = "databaseName",
             |  hoodie.table.name = "$tableName"
             | )""".stripMargin
        createTable(tableName, writeOptions, tableBasePath)
        spark.conf.set("hoodie.sql.bulk.insert.enable", "true")
        //Insert Operation
        val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
        doInsert(dataGen, tableName, "000")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
        doInsert(dataGen, tableName, "001")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))
        spark.conf.set("hoodie.sql.bulk.insert.enable", "false")
        doInsert(dataGen, tableName, "002")
        assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 3))
        spark.conf.set("hoodie.sql.bulk.insert.enable", "true")
        doInsert(dataGen, tableName, "003")
        assert(assertCommitCountAndIsLastBulkInsert(tableBasePath, 4))
      } finally {
        spark.conf.unset("hoodie.sql.bulk.insert.enable")
      }
    })
  }

  test("Set drop dupe in tblproperties") {
    withTempDir({ basePath =>
      val tableName = generateTableName
      val tableBasePath = basePath.getCanonicalPath + "/" + tableName
      val writeOptions =
        s"""
           |tblproperties (
           |  primaryKey = '_row_key',
           |  preCombineField = 'timestamp',
           |  hoodie.database.name = "databaseName",
           |  hoodie.datasource.write.insert.drop.duplicates = "true",
           |  hoodie.table.name = "$tableName"
           | )""".stripMargin
      createTable(tableName, writeOptions, tableBasePath)
      //Insert Operation
      val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
      doInsert(dataGen, tableName, "000")
      assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 1))
      doInsert(dataGen, tableName, "001")
      assert(!assertCommitCountAndIsLastBulkInsert(tableBasePath, 2))
    })
  }
   */
}
