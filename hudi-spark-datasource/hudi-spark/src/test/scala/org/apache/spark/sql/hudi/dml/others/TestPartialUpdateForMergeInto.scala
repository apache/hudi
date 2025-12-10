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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.table.log.HoodieLogFileReader
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.CompactionUtils
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieNotSupportedException

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getMetaClientAndFileSystemView
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.util.{Collections, List, Optional}

import scala.collection.JavaConverters._

class TestPartialUpdateForMergeInto extends HoodieSparkSqlTestBase {

  test("Test partial update with COW and Avro log format") {
    testPartialUpdate("cow", "avro")
  }

  test("Test partial update with COW and Avro log format and commit time ordering") {
    testPartialUpdate("cow", "avro", commitTimeOrdering = true)
  }

  test("Test partial update with MOR and Avro log format") {
    testPartialUpdate("mor", "avro")
  }

  test("Test partial update with MOR and avro log format and commit time ordering") {
    testPartialUpdate("mor", "avro", commitTimeOrdering = true)
  }

  test("Test partial update with MOR and Parquet log format") {
    testPartialUpdate("mor", "parquet")
  }

  test("Test partial update with MOR and Parquet log format and commit time ordering") {
    testPartialUpdate("mor", "parquet", commitTimeOrdering = true)
  }

  test("Test partial update and insert with COW and Avro log format") {
    testPartialUpdateWithInserts("cow", "avro")
  }

  test("Test partial update and insert with MOR and Avro log format") {
    testPartialUpdateWithInserts("mor", "avro")
  }

  test("Test partial update and insert with MOR and Avro log format and commit time ordering") {
    testPartialUpdateWithInserts("mor", "avro", commitTimeOrdering = true)
  }

  test("Test partial update and insert with MOR and Parquet log format") {
    testPartialUpdateWithInserts("mor", "parquet")
  }

  test("Test partial update and insert with MOR and Parquet log format and commit time ordering") {
    testPartialUpdateWithInserts("mor", "parquet", commitTimeOrdering = true)
  }

  test("Test partial update with schema on read enabled") {
    withSQLConf(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key() -> "true") {
      try {
        testPartialUpdate("mor", "parquet")
        fail("Expected exception to be thrown")
      } catch {
        case t: Throwable => assertTrue(t.isInstanceOf[HoodieNotSupportedException])
      }
    }
  }

  test("Test fallback to full update with MOR even if partial updates are enabled") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {

        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='mor',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$basePath'
        """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")

        // Update all fields
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as _ts, 'a1: updated' as description
             |union select 3 as id, 'a3' as name, 25 as price, 1260 as _ts, 'a3: updated' as description) s0
             |on t0.id = s0.id
             |when matched then update set *
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: updated"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: updated")
        )

        validateLogBlock(basePath, 1, Seq(Seq("id", "name", "price", "_ts", "description")), false)
      }
    }
  }

  test("Test MERGE INTO with inserts only on MOR table when partial updates are enabled") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true",
        // Write inserts to log block
        HoodieIndexConfig.INDEX_TYPE.key() -> "INMEMORY"
      ) {
        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='mor',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$basePath'
        """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")

        // Inserts only
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as _ts, 'a1: updated' as description
             |union select 3 as id, 'a3' as name, 25 as price, 1260 as _ts, 'a3: updated' as description
             |union select 4 as id, 'a4' as name, 60 as price, 1270 as _ts, 'a4: desc4' as description) s0
             |on t0.id = s0.id
             |when not matched then insert *
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 10.0, 1000, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 30.0, 1250, "a3: desc3"),
          Seq(4, "a4", 60.0, 1270, "a4: desc4")
        )

        validateLogBlock(
          basePath,
          2,
          Seq(Seq("id", "name", "price", "_ts", "description"), Seq("id", "name", "price", "_ts", "description")),
          false)
      }
    }
  }

  test("Test MERGE INTO with partial updates containing non-existent columns on COW table") {
    testPartialUpdateWithNonExistentColumns("cow")
  }

  test("Test MERGE INTO with partial updates containing non-existent columns on MOR table") {
    testPartialUpdateWithNonExistentColumns("mor")
  }

  def testPartialUpdateWithNonExistentColumns(tableType: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true") {

        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '$basePath'
        """.stripMargin)
        val structFields = scala.collection.immutable.List(
          StructField("id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true),
          StructField("_ts", IntegerType, nullable = true),
          StructField("description", StringType, nullable = true))

        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")
        validateTableSchema(tableName, structFields)

        // Partial updates using MERGE INTO statement with changed fields: "price", "_ts"
        // This is OK since the "UPDATE SET" clause does not contain the new column
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12.0 as price, 1001 as ts, 'x' as new_col
             |union select 3 as id, 'a3' as name, 25.0 as price, 1260 as ts, 'y' as new_col) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)

        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )

        // Partial updates using MERGE INTO statement with changed fields: "price", "_ts", "new_col"
        // This throws an error since the "UPDATE SET" clause contains the new column
        checkExceptionContain(
          s"""
             |merge into $tableName
             |using ( select 1 as id, 'a1' as name, 12.0 as price, 1001 as ts, 'x' as new_col
             |union select 3 as id, 'a3' as name, 25.0 as price, 1260 as ts, 'y' as new_col) s0
             |on $tableName.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts, new_col = s0.new_col
             |""".stripMargin)(getExpectedUnresolvedColumnExceptionMessage("new_col", tableName))
      }
    }
  }

  def testPartialUpdate(tableType: String,
                        logDataBlockFormat: String): Unit = {
    testPartialUpdate(tableType, logDataBlockFormat, commitTimeOrdering = false)
  }

  def testPartialUpdate(tableType: String,
                        logDataBlockFormat: String,
                        commitTimeOrdering: Boolean): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        val mergeMode = if (commitTimeOrdering) {
          RecordMergeMode.COMMIT_TIME_ORDERING.name()
        } else {
          RecordMergeMode.EVENT_TIME_ORDERING.name()
        }

        val preCombineString = if (commitTimeOrdering) {
          ""
        } else {
          "preCombineField = '_ts',"
        }
        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | $preCombineString
             | recordMergeMode = '$mergeMode'
             |)
             |location '$basePath'
        """.stripMargin)
        val structFields = scala.collection.immutable.List(
          StructField("id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true),
          StructField("_ts", IntegerType, nullable = true),
          StructField("description", StringType, nullable = true))
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")
        validateTableSchema(tableName, structFields)

        // Partial updates using MERGE INTO statement with changed fields: "price" and "_ts"
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 1.0 as price, 100 as ts
             |union select 1 as id, 12.0 as price, 999 as ts
             |union select 3 as id, 25.0 as price, 1260 as ts) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)
        validateTableSchema(tableName, structFields)
        if (commitTimeOrdering) {
          checkAnswer(s"select id, name, price, _ts, description from $tableName")(
            Seq(1, "a1", 12.0, 999, "a1: desc1"),
            Seq(2, "a2", 20.0, 1200, "a2: desc2"),
            Seq(3, "a3", 25.0, 1260, "a3: desc3")
          )
        } else {
          checkAnswer(s"select id, name, price, _ts, description from $tableName")(
            Seq(1, "a1", 10.0, 1000, "a1: desc1"),
            Seq(2, "a2", 20.0, 1200, "a2: desc2"),
            Seq(3, "a3", 25.0, 1260, "a3: desc3")
          )
        }
        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 1, Seq(Seq("price", "_ts")), true)
        }

        // TODO: [HUDI-9375] get rid of this update and fix the rest of the test accordingly
        // showcase the difference between event time and commit time ordering
        // Partial updates using MERGE INTO statement with changed fields: "price" and "_ts"
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12.0 as price, 1001 as ts) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )
        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 2, Seq(Seq("price", "_ts"), Seq("price", "_ts")), true)
        }

        // Partial updates using MERGE INTO statement with changed fields: "description" and "_ts"
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 'a1: updated desc1' as new_description, 1023 as _ts
             |union select 2 as id, 'a2' as name, 'a2: updated desc2' as new_description, 1270 as _ts) s0
             |on t0.id = s0.id
             |when matched then update set description = s0.new_description, _ts = s0._ts
             |""".stripMargin)

        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1023, "a1: updated desc1"),
          Seq(2, "a2", 20.0, 1270, "a2: updated desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3")
        )

        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 3, Seq(Seq("price", "_ts"), Seq("price", "_ts"), Seq("_ts", "description")), true)

          withSQLConf(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "4") {
            // Partial updates that trigger compaction
            spark.sql(
              s"""
                 |merge into $tableName t0
                 |using ( select 2 as id, '_a2' as name, 18.0 as _price, 1275 as _ts
                 |union select 3 as id, '_a3' as name, 28.0 as _price, 1280 as _ts) s0
                 |on t0.id = s0.id
                 |when matched then update set price = s0._price, _ts = s0._ts
                 |""".stripMargin)
            validateCompactionExecuted(basePath)
            validateTableSchema(tableName, structFields)
            checkAnswer(s"select id, name, price, _ts, description from $tableName")(
              Seq(1, "a1", 12.0, 1023, "a1: updated desc1"),
              Seq(2, "a2", 18.0, 1275, "a2: updated desc2"),
              Seq(3, "a3", 28.0, 1280, "a3: desc3")
            )
          }

          // trigger one more MIT and do inline clustering
          withSQLConf(
            HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
            HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "3"
          ) {
            spark.sql(
              s"""
                 |merge into $tableName t0
                 |using ( select 2 as id, '_a2' as name, 48.0 as _price, 1275 as _ts
                 |union select 3 as id, '_a3' as name, 58.0 as _price, 1280 as _ts) s0
                 |on t0.id = s0.id
                 |when matched then update set price = s0._price, _ts = s0._ts
                 |""".stripMargin)

            validateClusteringExecuted(basePath)
            validateTableSchema(tableName, structFields)
            checkAnswer(s"select id, name, price, _ts, description from $tableName")(
              Seq(1, "a1", 12.0, 1023, "a1: updated desc1"),
              Seq(2, "a2", 48.0, 1275, "a2: updated desc2"),
              Seq(3, "a3", 58.0, 1280, "a3: desc3")
            )
          }
        }
      }

      if (tableType.equals("cow")) {
        // No preCombine field
        val tableName2 = generateTableName
        val basePath2 = tmp.getCanonicalPath + "/" + tableName2
        spark.sql(
          s"""
             |create table $tableName2 (
             | id int,
             | name string,
             | price double
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id'
             |)
             |location '$basePath2'
        """.stripMargin)
        spark.sql(s"insert into $tableName2 values(1, 'a1', 10)")

        withSQLConf(
          HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true",
          HoodieClusteringConfig.INLINE_CLUSTERING.key() -> "true",
          HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key() -> "1"
        ) {
          spark.sql(
            s"""
               |merge into $tableName2 t0
               |using ( select 1 as id, 'a2' as name, 12.0 as price) s0
               |on t0.id = s0.id
               |when matched then update set price = s0.price
               |""".stripMargin)

          validateClusteringExecuted(basePath2)
          checkAnswer(s"select id, name, price from $tableName2")(
            Seq(1, "a1", 12.0)
          )
        }
      }
    }
  }

  def testPartialUpdateWithInserts(tableType: String,
                                   logDataBlockFormat: String): Unit = {
    testPartialUpdateWithInserts(tableType, logDataBlockFormat, commitTimeOrdering = false)
  }

  def testPartialUpdateWithInserts(tableType: String,
                                   logDataBlockFormat: String,
                                   commitTimeOrdering: Boolean): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = tmp.getCanonicalPath + "/" + tableName
      withSQLConf(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key() -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key() -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> "true") {
        val mergeMode = if (commitTimeOrdering) {
          RecordMergeMode.COMMIT_TIME_ORDERING.name()
        } else {
          RecordMergeMode.EVENT_TIME_ORDERING.name()
        }

        val preCombineString = if (commitTimeOrdering) {
          ""
        } else {
          "preCombineField = '_ts',"
        }

        // Create a table with five data fields
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts int,
             | description string
             |) using hudi
             |tblproperties(
             | type ='$tableType',
             | primaryKey = 'id',
             | $preCombineString
             | recordMergeMode = '$mergeMode'
             |)
             |location '$basePath'
        """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), (3, 'a3', 30, 1250, 'a3: desc3')")

        // Partial updates with changed fields: "price" and "_ts" and inserts using MERGE INTO statement
        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as _ts, '' as description
             |union select 3 as id, 'a3' as name, 25 as price, 1260 as _ts, '' as description
             |union select 4 as id, 'a4' as name, 70 as price, 1270 as _ts, 'a4: desc4' as description) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0._ts
             |when not matched then insert *
             |""".stripMargin)

        checkAnswer(s"select id, name, price, _ts, description from $tableName")(
          Seq(1, "a1", 12.0, 1001, "a1: desc1"),
          Seq(2, "a2", 20.0, 1200, "a2: desc2"),
          Seq(3, "a3", 25.0, 1260, "a3: desc3"),
          Seq(4, "a4", 70.0, 1270, "a4: desc4")
        )

        if (tableType.equals("mor")) {
          validateLogBlock(basePath, 1, Seq(Seq("price", "_ts")), true)
        }
      }
    }
  }

  test("Test MergeInto Exception") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |tblproperties(
         | type = 'cow',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)

    val failedToResolveErrorMessage = "No matching assignment found for target table ordering field `_ts"

    checkExceptionContain(
      s"""
         |merge into $tableName t0
         |using ( select 1 as id, 'a1' as name, 12 as price) s0
         |on t0.id = s0.id
         |when matched then update set price = s0.price
      """.stripMargin)(failedToResolveErrorMessage)

    val tableName2 = generateTableName
    spark.sql(
      s"""
         |create table $tableName2 (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |tblproperties(
         | type = 'mor',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)
  }

  test("Partial updates for table version 6 and 8 handled gracefully in MIT") {
    Seq(HoodieTableVersion.SIX.versionCode(), HoodieTableVersion.EIGHT.versionCode()).foreach(
      tableVersion => withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | CREATE TABLE $tableName (
             |   record_key STRING,
             |   name STRING,
             |   age INT,
             |   department STRING,
             |   salary DOUBLE,
             |   ts BIGINT
             | ) USING hudi
             | PARTITIONED BY (department)
             | LOCATION '${tmp.getCanonicalPath}'
             | TBLPROPERTIES (
             |   type = 'mor',
             |   hoodie.write.table.version = '$tableVersion',
             |   hoodie.index.type = 'GLOBAL_SIMPLE',
             |   hoodie.index.global.index.enable = 'true',
             |   hoodie.bloom.index.use.metadata = 'true',
             |   primaryKey = 'record_key',
             |   preCombineField = 'ts')""".stripMargin)

        spark.sql(
          s"""
             | INSERT INTO $tableName
             | SELECT * FROM (
             |   SELECT 'emp_001' as record_key, 'John Doe' as name, 30 as age,
             |          'Sales' as department, 80000.0 as salary, 1598886000 as ts
             |   UNION ALL
             |   SELECT 'emp_002', 'Jane Smith', 28, 'Sales', 75000.0, 1598886001
             |   UNION ALL
             |   SELECT 'emp_003', 'Bob Wilson', 35, 'Marketing', 85000.0, 1598886002
             |)""".stripMargin)

        spark.sql(
          s"""
             | UPDATE $tableName
             | SET
             |     ts = 1598000000
             | WHERE record_key = 'emp_001'
             |""".stripMargin)

        spark.sql(
          s"""
             | CREATE OR REPLACE TEMPORARY VIEW source_updates AS
             | SELECT * FROM (
             |   SELECT 'emp_001' as record_key, 'John Doe' as name, 30 as age,
             |          'Engineering' as department, CAST(95000.0 as DOUBLE) as salary, cast(1598886200 as BIGINT) as ts
             |   UNION ALL
             |   SELECT 'emp_004', 'Alice Brown', 29, 'Engineering', CAST(82000.0 as DOUBLE), cast(1598886201 as BIGINT)
             |)""".stripMargin)

        spark.sql(
          s"""
             | MERGE INTO $tableName t
             | USING source_updates s
             | ON t.record_key = s.record_key
             | WHEN MATCHED THEN
             |   UPDATE SET
             |     record_key = s.record_key,
             |     department = s.department,
             |     salary = s.salary,
             |     ts = s.ts
             | WHEN NOT MATCHED THEN
             |   INSERT *""".stripMargin)
      }
    )
  }

  test("Test MergeInto Partial Updates should fail with CUSTOM payload and merge mode") {
    withTempDir { tmp =>
      withSQLConf(
        "hoodie.index.type" -> "GLOBAL_SIMPLE",
        DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key() -> "upsert") {
        val tableName = generateTableName
        spark.sql(
          s"""
             | CREATE TABLE $tableName (
             |   record_key STRING,
             |   name STRING,
             |   age INT,
             |   department STRING,
             |   salary DOUBLE,
             |   ts BIGINT
             | ) USING hudi
             | PARTITIONED BY (department)
             | LOCATION '${tmp.getCanonicalPath}'
             | TBLPROPERTIES (
             |   type = 'mor',
             |   primaryKey = 'record_key',
             |   hoodie.write.record.merge.mode = 'CUSTOM',
             |   hoodie.datasource.write.payload.class = 'org.apache.hudi.common.testutils.reader.HoodieRecordTestPayload',
             |   preCombineField = 'ts')""".stripMargin)

        spark.sql(
          s"""
             | INSERT INTO $tableName
             | SELECT * FROM (
             |   SELECT 'emp_001' as record_key, 'John Doe' as name, 30 as age,
             |          'Sales' as department, 80000.0 as salary, 1598886000 as ts
             |   UNION ALL
             |   SELECT 'emp_002', 'Jane Smith', 28, 'Sales', 75000.0, 1598886001
             |   UNION ALL
             |   SELECT 'emp_003', 'Bob Wilson', 35, 'Marketing', 85000.0, 1598886002
             |)""".stripMargin)

        val failedToResolveError = "MERGE INTO field resolution error: No matching assignment found for target table"
        checkExceptionContain(
          s"""
             |merge into $tableName t0
             |using ( SELECT 'emp_001' as record_key, 'John Doe' as name, 35 as age, cast(1598886200 as BIGINT) as ts) s0
             |on t0.record_key = s0.record_key
             |when matched then update set age = s0.age
      """.stripMargin)(failedToResolveError)
      }
    }
  }

  def validateLogBlock(basePath: String,
                       expectedNumLogFile: Int,
                       changedFields: Seq[Seq[String]],
                       isPartial: Boolean): Unit = {
    val (metaClient, fsView) = getMetaClientAndFileSystemView(basePath)
    val fileSlice: Optional[FileSlice] = fsView.getAllFileSlices("")
      .filter((fileSlice: FileSlice) => {
        HoodieTestUtils.getLogFileListFromFileSlice(fileSlice).size() == expectedNumLogFile
      })
      .findFirst()
    assertTrue(fileSlice.isPresent)
    val logFilePathList: List[String] = HoodieTestUtils.getLogFileListFromFileSlice(fileSlice.get)
    Collections.sort(logFilePathList)

    val tableSchema = new TableSchemaResolver(metaClient).getTableSchema()
    for (i <- 0 until expectedNumLogFile) {
      val logReader = new HoodieLogFileReader(
        metaClient.getStorage, new HoodieLogFile(logFilePathList.get(i)),
        tableSchema.toAvroSchema, 1024 * 1024, false, false,
        "id", null)
      assertTrue(logReader.hasNext)
      val logBlockHeader = logReader.next().getLogBlockHeader
      assertTrue(logBlockHeader.containsKey(HeaderMetadataType.SCHEMA))
      if (isPartial) {
        assertTrue(logBlockHeader.containsKey(HeaderMetadataType.IS_PARTIAL))
        assertTrue(logBlockHeader.get(HeaderMetadataType.IS_PARTIAL).toBoolean)
      } else {
        assertFalse(logBlockHeader.containsKey(HeaderMetadataType.IS_PARTIAL))
      }
      val actualSchema = HoodieSchema.parse(logBlockHeader.get(HeaderMetadataType.SCHEMA))
      val expectedSchema = HoodieSchema.fromAvroSchema(HoodieAvroUtils.addMetadataFields(HoodieAvroUtils.generateProjectionSchema(
        tableSchema.toAvroSchema, changedFields(i).asJava), false))
      assertEquals(expectedSchema, actualSchema)
    }
  }

  def validateClusteringExecuted(basePath: String): Unit = {
    val storageConf = HoodieTestUtils.getDefaultStorageConf
    val metaClient: HoodieTableMetaClient =
      HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    val lastCommit = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, lastCommit.getAction)
  }

  def validateCompactionExecuted(basePath: String): Unit = {
    val storageConf = HoodieTestUtils.getDefaultStorageConf
    val metaClient: HoodieTableMetaClient =
      HoodieTableMetaClient.builder.setConf(storageConf).setBasePath(basePath).build
    val lastCommit = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    assertEquals(HoodieTimeline.COMMIT_ACTION, lastCommit.getAction)
    CompactionUtils.getCompactionPlan(metaClient, lastCommit.requestedTime())
  }
}
