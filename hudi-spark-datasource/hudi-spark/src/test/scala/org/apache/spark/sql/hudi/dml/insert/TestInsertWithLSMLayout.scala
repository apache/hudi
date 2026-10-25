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

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.DataSourceWriteOptions.{ENABLE_ROW_WRITER, SPARK_SQL_INSERT_INTO_OPERATION}
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getLastCommitMetadata
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

class TestInsertWithLSMLayout extends HoodieSparkSqlTestBase {

  test("Test INSERT INTO with LSM layout") {
    Seq("cow", "mor").foreach { tableType =>
      Seq(WriteOperationType.INSERT, WriteOperationType.UPSERT).foreach { operation =>
        withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> operation.value()) {
          withTempDir { tmp =>
            val tableName = generateTableName
            val tablePath = s"${tmp.getCanonicalPath}/$tableName"
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  ts long
                 |) using hudi
                 |location '$tablePath'
                 |tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id',
                 |  preCombineField = 'ts',
                 |  '${HoodieTableConfig.TABLE_STORAGE_LAYOUT.key}' = '${HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue}',
                 |  '${HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key}' = 'parquet'
                 |)
                 |""".stripMargin)

            spark.sql(
              s"""
                 |insert into $tableName values
                 |  (3, 'name-3', 30.0, 1000),
                 |  (1, 'name-1', 10.0, 1000),
                 |  (2, 'name-2', 20.0, 1000)
                 |""".stripMargin)

            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
              Seq(1, "name-1", 10.0, 1000),
              Seq(2, "name-2", 20.0, 1000),
              Seq(3, "name-3", 30.0, 1000))
            assertResult(operation) {
              getLastCommitMetadata(spark, tablePath).getOperationType
            }
            assertEquals(
              HoodieTableConfig.TableStorageLayout.LSM_TREE,
              createMetaClient(spark, tablePath).getTableConfig.getTableStorageLayout)
          }
        }
      }
    }
  }

  test("Test bulk insert overwrite with LSM row writer") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      Seq("cow", "mor").foreach { tableType =>
        withTempDir { tmp =>
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id string,
               |  name string,
               |  ts long,
               |  dt string
               |) using hudi
               |location '$tablePath'
               |partitioned by (dt)
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  '${ENABLE_ROW_WRITER.key}' = 'true',
               |  '${HoodieTableConfig.TABLE_STORAGE_LAYOUT.key}' = '${HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue}',
               |  '${HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key}' = 'parquet'
               |)
               |""".stripMargin)

          spark.sql(
            s"""
               |insert into $tableName values
               |  ('😀-old-p1', 'old', 1, 'p1'),
               |  ('Ａ-old-p1', 'old', 1, 'p1'),
               |  ('middle-old-p2', 'keep', 1, 'p2')
               |""".stripMargin)

          spark.sql(
            s"""
               |insert overwrite table $tableName partition(dt='p1') values
               |  ('😀-partition-overwrite', 'new', 2),
               |  ('Ａ-partition-overwrite', 'new', 2),
               |  ('middle-partition-overwrite', 'new', 2)
               |""".stripMargin)
          checkAnswer(s"select id, name, ts, dt from $tableName order by id")(
            Seq("middle-old-p2", "keep", 1, "p2"),
            Seq("middle-partition-overwrite", "new", 2, "p1"),
            Seq("Ａ-partition-overwrite", "new", 2, "p1"),
            Seq("😀-partition-overwrite", "new", 2, "p1"))
          assertResult(WriteOperationType.INSERT_OVERWRITE) {
            getLastCommitMetadata(spark, tablePath).getOperationType
          }
          assertBaseFilesSorted(tablePath, WriteOperationType.INSERT_OVERWRITE)

          spark.sql(
            s"""
               |insert overwrite table $tableName values
               |  ('😀-table-overwrite', 'table', 3, 'p3'),
               |  ('Ａ-table-overwrite', 'table', 3, 'p3'),
               |  ('middle-table-overwrite', 'table', 3, 'p3')
               |""".stripMargin)
          checkAnswer(s"select id, name, ts, dt from $tableName order by id")(
            Seq("middle-table-overwrite", "table", 3, "p3"),
            Seq("Ａ-table-overwrite", "table", 3, "p3"),
            Seq("😀-table-overwrite", "table", 3, "p3"))
          assertResult(WriteOperationType.INSERT_OVERWRITE_TABLE) {
            getLastCommitMetadata(spark, tablePath).getOperationType
          }
          assertBaseFilesSorted(tablePath, WriteOperationType.INSERT_OVERWRITE_TABLE)
        }
      }
    }
  }

  test("Test bulk insert with LSM HoodieRecord writer") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id string,
             |  name string,
             |  ts long,
             |  dt string
             |) using hudi
             |location '$tablePath'
             |partitioned by (dt)
             |tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  '${ENABLE_ROW_WRITER.key}' = 'false',
             |  '${HoodieWriteConfig.BULK_INSERT_SORT_MODE.key}' =
             |    '${BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT.name}',
             |  '${HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key}' = '2',
             |  '${HoodieTableConfig.TABLE_STORAGE_LAYOUT.key}' =
             |    '${HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue}'
             |)
             |""".stripMargin)

        spark.sql(
          s"""
             |insert into $tableName values
             |  ('😀-p1', 'emoji', 1, 'p1'),
             |  ('Ａ-p1', 'full-width', 1, 'p1'),
             |  ('middle-p1', 'ascii', 1, 'p1'),
             |  ('😀-p2', 'emoji', 1, 'p2'),
             |  ('Ａ-p2', 'full-width', 1, 'p2')
             |""".stripMargin)

        checkAnswer(s"select id, name, ts, dt from $tableName order by id")(
          Seq("middle-p1", "ascii", 1, "p1"),
          Seq("Ａ-p1", "full-width", 1, "p1"),
          Seq("Ａ-p2", "full-width", 1, "p2"),
          Seq("😀-p1", "emoji", 1, "p1"),
          Seq("😀-p2", "emoji", 1, "p2"))
        assertResult(WriteOperationType.BULK_INSERT) {
          getLastCommitMetadata(spark, tablePath).getOperationType
        }
        assertEquals(
          HoodieTableConfig.TableStorageLayout.LSM_TREE,
          createMetaClient(spark, tablePath).getTableConfig.getTableStorageLayout)
        assertBaseFilesSorted(tablePath, WriteOperationType.BULK_INSERT)
      }
    }
  }

  private def assertBaseFilesSorted(tablePath: String, operationType: WriteOperationType): Unit = {
    val pathStream = Files.walk(Paths.get(tablePath))
    val baseFiles = try {
      pathStream.iterator().asScala
        .filter(Files.isRegularFile(_))
        .filter(path => path.getFileName.toString.endsWith(".parquet"))
        .filterNot(path => path.toString.contains("/.hoodie/"))
        .map(_.toString)
        .toList
    } finally {
      pathStream.close()
    }
    assertFalse(baseFiles.isEmpty)
    baseFiles.foreach { baseFile =>
      val actualKeys = spark.read.parquet(baseFile)
        .select(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .collect()
        .map(_.getString(0))
        .toSeq
      val expectedKeys = actualKeys.sortWith(
        (left, right) => StringUtils.compareUtf8Bytes(left, right) < 0)
      assertEquals(expectedKeys, actualKeys, s"$operationType output is not sorted: $baseFile")
    }
  }
}
