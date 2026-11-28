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

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.io.File

/**
 * TIMESTAMP partition paths written through Spark SQL, on a new table and on one created before
 * `hoodie.datasource.write.partitionpath.timestamp.fractional.seconds` existed.
 */
class TestTimestampPartitionFractionalSeconds extends HoodieSparkSqlTestBase {

  private val fractionalSecondsKey = HoodieTableConfig.TIMESTAMP_PARTITION_FRACTIONAL_SECONDS.key

  private val values =
    """(1, 'a', TIMESTAMP '2024-02-29 10:30:00.123456'),
      |(2, 'b', TIMESTAMP '2024-02-29 10:30:00.654321'),
      |(3, 'c', TIMESTAMP '2024-02-29 10:30:00')""".stripMargin

  /** The partition directories under a base path, as the writer named them, without the column prefix. */
  private def partitionDirs(basePath: String): Seq[String] =
    new File(basePath).listFiles().filter(f => f.isDirectory && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      .map(_.getName.stripPrefix("ts=")).sorted.toSeq

  private def createTable(tableName: String, basePath: String): Unit =
    spark.sql(
      s"""
         |CREATE TABLE $tableName (id INT, name STRING, ts TIMESTAMP) USING hudi
         | TBLPROPERTIES (primaryKey = 'id', type = 'cow')
         | PARTITIONED BY (ts)
         | LOCATION '$basePath'
       """.stripMargin)

  private def rows(tableName: String): Seq[String] =
    spark.sql(s"SELECT id, name, CAST(ts AS STRING) FROM $tableName ORDER BY id").collect().map(_.mkString("|")).toSeq

  private def idsAt(tableName: String, ts: String): Seq[Int] =
    spark.sql(s"SELECT id FROM $tableName WHERE ts = TIMESTAMP '$ts' ORDER BY id").collect().map(_.getInt(0)).toSeq

  test("Test a new table keeps the fractional seconds of TIMESTAMP partition values") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      assertResult("true")(createMetaClient(spark, basePath).getTableConfig.getString(fractionalSecondsKey))

      spark.sql(s"INSERT INTO $tableName VALUES $values")
      // Spark names the directory of a TIMESTAMP partition value the same way
      val sparkPath = s"${tmp.getCanonicalPath}/spark_$tableName"
      spark.sql(s"SELECT * FROM VALUES $values AS t(id, name, ts)").write.partitionBy("ts").parquet(sparkPath)
      assertResult(Seq("2024-02-29 10%3A30%3A00", "2024-02-29 10%3A30%3A00.123456", "2024-02-29 10%3A30%3A00.654321"))(
        partitionDirs(basePath))
      assertResult(partitionDirs(sparkPath))(partitionDirs(basePath))

      val expected = Seq("1|a|2024-02-29 10:30:00.123456", "2|b|2024-02-29 10:30:00.654321", "3|c|2024-02-29 10:30:00")
      assertResult(expected)(rows(tableName))
      // Partition values parsed from the path match the row values, so pruning on the exact value keeps the row
      assertResult(Seq(1))(idsAt(tableName, "2024-02-29 10:30:00.123456"))
      assertResult(Seq(3))(idsAt(tableName, "2024-02-29 10:30:00"))
      withSQLConf(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key -> "true") {
        assertResult(expected)(rows(tableName))
      }

      // An update finds each row under the path it was written to, rather than adding a second copy
      spark.sql(s"UPDATE $tableName SET name = 'a2' WHERE id = 1")
      withSQLConf("hoodie.spark.sql.insert.into.operation" -> "upsert") {
        spark.sql(s"INSERT INTO $tableName VALUES (2, 'b2', TIMESTAMP '2024-02-29 10:30:00.654321')")
      }
      withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
        spark.sql(s"INSERT INTO $tableName VALUES (4, 'd', TIMESTAMP '2024-02-29 10:30:00.5')")
      }
      assertResult(Seq("1|a2|2024-02-29 10:30:00.123456", "2|b2|2024-02-29 10:30:00.654321",
        "3|c|2024-02-29 10:30:00", "4|d|2024-02-29 10:30:00.5"))(rows(tableName))
      assertResult(Seq(4))(idsAt(tableName, "2024-02-29 10:30:00.5"))
      assert(partitionDirs(basePath).contains("2024-02-29 10%3A30%3A00.5"))
    })
  }

  test("Test a table without the config keeps truncating TIMESTAMP partition values") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      // As a table created before the config existed
      val metaClient = createMetaClient(spark, basePath)
      HoodieTableConfig.delete(metaClient.getStorage, metaClient.getMetaPath,
        java.util.Collections.singleton(fractionalSecondsKey))
      spark.sessionState.catalog.refreshTable(spark.sessionState.sqlParser.parseTableIdentifier(tableName))

      spark.sql(s"INSERT INTO $tableName VALUES $values")
      assertResult(Seq("2024-02-29 10%3A30%3A00"))(partitionDirs(basePath))
      assertResult(Seq("1|a|2024-02-29 10:30:00", "2|b|2024-02-29 10:30:00", "3|c|2024-02-29 10:30:00"))(rows(tableName))
      assert(!createMetaClient(spark, basePath).getTableConfig.contains(fractionalSecondsKey))

      // Turning it on would move a row with fractional seconds to a new path
      withSQLConf(fractionalSecondsKey -> "true") {
        checkExceptionContain(s"INSERT INTO $tableName VALUES (4, 'd', TIMESTAMP '2024-02-29 10:30:00.5')")(
          "can only be set when a table is created")
        // An overwrite skips that check, and still writes the layout the table records
        spark.sql(s"INSERT OVERWRITE TABLE $tableName VALUES (5, 'e', TIMESTAMP '2024-02-29 10:30:00.5')")
      }
      assertResult(Seq("2024-02-29 10%3A30%3A00"))(partitionDirs(basePath))
      assertResult(Seq("5|e|2024-02-29 10:30:00"))(rows(tableName))

      // A table without the config truncates, so a CREATE TABLE over it cannot ask for fractional seconds
      checkExceptionContain(
        s"""
           |CREATE TABLE ${tableName}_2 (id INT, name STRING, ts TIMESTAMP) USING hudi
           | TBLPROPERTIES (primaryKey = 'id', type = 'cow', '$fractionalSecondsKey' = 'true')
           | PARTITIONED BY (ts)
           | LOCATION '$basePath'
         """.stripMargin)("can only be set when a table is created")
    })
  }
}
