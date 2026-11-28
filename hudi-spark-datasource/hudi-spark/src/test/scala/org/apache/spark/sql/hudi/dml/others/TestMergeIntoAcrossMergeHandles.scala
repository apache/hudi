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

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.io.HoodieWriteMergeHandle

import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertTrue

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

/**
 * MERGE INTO updates of existing keys merged into base files, run through every merge handle in
 * [[HoodieSparkSqlTestBase.mergeHandleClasses]], on COW and MOR tables.
 *
 * A merge handle must write the merge result as-is. The merged row is in the table schema
 * (nullable columns), while [[ExpressionPayload]] decodes its bytes with the MERGE source schema
 * and re-applies the NOT MATCHED clause, so the merge result must not be wrapped in an
 * ExpressionPayload again, which HoodieWriteMergeHandle used to do.
 *
 * The tables use the SIMPLE index, which cannot index log files, so MOR updates to a small file
 * group are merged into its base file through the merge handle rather than appended to a log file.
 */
class TestMergeIntoAcrossMergeHandles extends HoodieSparkSqlTestBase {

  private val tableTypes = Seq("cow", "mor")

  private def createTableWithRows(tableName: String, location: String, tableType: String): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  name string,
         |  city string,
         |  rating int,
         |  ts bigint,
         |  country string
         |) using hudi
         | partitioned by (country)
         | tblproperties (
         |   type = '$tableType',
         |   primaryKey = 'country,name',
         |   preCombineField = 'ts'
         | )
         | location '$location'
       """.stripMargin)
    spark.sql(
      s"""
         |insert into $tableName values
         |  ('a1', 'phoenix', 1, 1000, 'US'),
         |  ('a2', 'nyc', 2, 1000, 'US'),
         |  ('a3', 'toronto', 3, 1000, 'CA')
       """.stripMargin)
  }

  private def checkRows(tableName: String)(expected: Seq[Any]*): Unit =
    checkAnswer(s"select name, city, rating, ts, country from $tableName order by name")(expected: _*)

  /** Asserts the writes merged into base files: on MOR an append to a log file would bypass the merge handle. */
  private def assertNoLogFiles(location: String): Unit = {
    val logFiles = Files.walk(Paths.get(location)).iterator().asScala
      .map(_.toString)
      .filter(path => !path.contains("/.hoodie/") && path.contains(".log."))
      .toList
    assertTrue(logFiles.isEmpty, s"Expected updates to be merged into base files, found log files: $logFiles")
  }

  /** Registers the test for each table type and merge handle, with the SIMPLE index. */
  private def testOnTableTypesAndMergeHandles(testName: String)(testFun: String => Unit): Unit =
    tableTypes.foreach { tableType =>
      testWithMergeHandles(s"$testName ($tableType)") {
        withSQLConf(HoodieIndexConfig.INDEX_TYPE.key -> "SIMPLE") {
          testFun(tableType)
        }
      }
    }

  /** Registers a view whose columns are all nullable, as a scan of a table gives. */
  private def createNullableSourceView(viewName: String, rows: Row*): Unit = {
    val sourceSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("rating", IntegerType, nullable = true),
      StructField("ts", LongType, nullable = true),
      StructField("country", StringType, nullable = true)))
    spark.createDataFrame(rows.asJava, sourceSchema).createOrReplaceTempView(viewName)
  }

  test("Test MergeInto preserves the commit time of an ignored row") {
    withRecordType(Seq(HoodieRecordType.AVRO))(withTempDir { tmp =>
      withSQLConf(
        HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key() -> classOf[HoodieWriteMergeHandle[_, _, _, _]].getName,
        HoodieIndexConfig.INDEX_TYPE.key() -> "SIMPLE",
        SPARK_SQL_OPTIMIZED_WRITES.key() -> "false") {
        val tableName = generateTableName
        createTableWithRows(tableName, s"${tmp.getCanonicalPath}/$tableName", "cow")
        val ignoredCommitQuery = s"select _hoodie_commit_time from $tableName where name = 'a2'"
        val originalCommit = spark.sql(ignoredCommitQuery).head().getString(0)

        val sourceView = s"${tableName}_source"
        createNullableSourceView(sourceView,
          Row("a1", "scottsdale", 9, 2000L, "US"),
          Row("a2", "ignored", -1, 2000L, "US"))

        spark.sql(
          s"""
             |merge into $tableName t
             |using $sourceView s
             |on t.country = s.country and t.name = s.name
             |when matched and s.rating > 0 then update set *
           """.stripMargin)

        checkRows(tableName)(
          Seq("a1", "scottsdale", 9, 2000L, "US"),
          Seq("a2", "nyc", 2, 1000L, "US"),
          Seq("a3", "toronto", 3, 1000L, "CA"))
        checkAnswer(ignoredCommitQuery)(Seq(originalCommit))
      }
    })
  }

  testOnTableTypesAndMergeHandles("Test MergeInto update with a non-nullable source") { tableType =>
    withTempDir { tmp =>
      val tableName = generateTableName
      val location = s"${tmp.getCanonicalPath}/$tableName"
      createTableWithRows(tableName, location, tableType)

      // A subquery of constants: every source column is NOT NULL, the table's are nullable.
      spark.sql(
        s"""
           |merge into $tableName t
           |using (
           |  select 'a1' as name, 'scottsdale' as city, 9 as rating, 2000L as ts, 'US' as country
           |  union all
           |  select 'a4', 'sf', 5, 2001L, 'US'
           |) s
           |on t.country = s.country and t.name = s.name
           |when matched then update set *
           |when not matched then insert *
         """.stripMargin)

      checkRows(tableName)(
        Seq("a1", "scottsdale", 9, 2000L, "US"),
        Seq("a2", "nyc", 2, 1000L, "US"),
        Seq("a3", "toronto", 3, 1000L, "CA"),
        Seq("a4", "sf", 5, 2001L, "US"))
      assertNoLogFiles(location)
    }
  }

  testOnTableTypesAndMergeHandles("Test MergeInto update with a nullable source") { tableType =>
    withTempDir { tmp =>
      val tableName = generateTableName
      val location = s"${tmp.getCanonicalPath}/$tableName"
      createTableWithRows(tableName, location, tableType)

      // Source columns match the table's nullability, so this passed even before the fix.
      val sourceView = s"${tableName}_source"
      createNullableSourceView(sourceView,
        Row("a1", "scottsdale", 9, 2000L, "US"),
        Row("a4", "sf", 5, 2001L, "US"))

      spark.sql(
        s"""
           |merge into $tableName t
           |using $sourceView s
           |on t.country = s.country and t.name = s.name
           |when matched then update set *
           |when not matched then insert *
         """.stripMargin)

      checkRows(tableName)(
        Seq("a1", "scottsdale", 9, 2000L, "US"),
        Seq("a2", "nyc", 2, 1000L, "US"),
        Seq("a3", "toronto", 3, 1000L, "CA"),
        Seq("a4", "sf", 5, 2001L, "US"))
      assertNoLogFiles(location)
    }
  }

  testOnTableTypesAndMergeHandles("Test MergeInto update is not filtered by the NOT MATCHED condition") { tableType =>
    withTempDir { tmp =>
      val tableName = generateTableName
      val location = s"${tmp.getCanonicalPath}/$tableName"
      createTableWithRows(tableName, location, tableType)

      // A nullable source, so the source and table schemas agree and only the clause handling is exercised.
      val sourceView = s"${tableName}_source"
      createNullableSourceView(sourceView,
        Row("a1", "scottsdale", 9, 2000L, "US"),
        Row("a4", "sf", 50, 2001L, "US"))

      // The NOT MATCHED condition is false for the matched key's source row, so it must not affect its update.
      spark.sql(
        s"""
           |merge into $tableName t
           |using $sourceView s
           |on t.country = s.country and t.name = s.name
           |when matched then update set *
           |when not matched and s.rating > 10 then insert *
         """.stripMargin)

      checkRows(tableName)(
        Seq("a1", "scottsdale", 9, 2000L, "US"),
        Seq("a2", "nyc", 2, 1000L, "US"),
        Seq("a3", "toronto", 3, 1000L, "CA"),
        Seq("a4", "sf", 50, 2001L, "US"))
      assertNoLogFiles(location)
    }
  }
}
