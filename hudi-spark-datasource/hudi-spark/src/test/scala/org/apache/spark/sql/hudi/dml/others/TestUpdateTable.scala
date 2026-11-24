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
import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.HoodieSparkUtils.gteqSpark3_4
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.model.{HoodieRecordMerger, HoodieTableType}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.procedure.CustomSparkRecordMergerForClustering
import org.junit.jupiter.api.Assertions.assertEquals

class TestUpdateTable extends HoodieSparkSqlTestBase {

  // (ordering field type, older literal, newer literal, older value as string, newer value as string)
  private val logicalTypeOrderingCases = Seq(
    ("timestamp", "timestamp'2026-10-01 00:00:00'", "timestamp'2026-10-02 00:00:00'",
      "2026-10-01 00:00:00", "2026-10-02 00:00:00"),
    ("date", "date'2026-10-01'", "date'2026-10-02'", "2026-10-01", "2026-10-02"),
    ("decimal(10,2)", "cast(1.50 as decimal(10,2))", "cast(2.50 as decimal(10,2))", "1.50", "2.50"))

  logicalTypeOrderingCases.foreach { case (orderingType, oldLiteral, newLiteral, oldStr, newStr) =>
    test(s"Test Update Table With ${orderingType.toUpperCase} Ordering Field") {
      withTempDir { tmp =>
        Seq(true, false).foreach { sparkSqlOptimizedWrites =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  v int,
               |  ts $orderingType,
               |  dt string
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = 'cow',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 1, $oldLiteral, 'p1'), (2, 2, $oldLiteral, 'p1')")

          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {
            spark.sql(s"update $tableName set v = v * 10 where id = 1")
            checkAnswer(s"select id, v, cast(ts as string), dt from $tableName order by id")(
              Seq(1, 10, oldStr, "p1"),
              Seq(2, 2, oldStr, "p1")
            )

            spark.sql(s"update $tableName set v = v + 1, ts = $newLiteral where id = 2")
            checkAnswer(s"select id, v, cast(ts as string), dt from $tableName order by id")(
              Seq(1, 10, oldStr, "p1"),
              Seq(2, 3, newStr, "p1")
            )
          }
        }
      }
    }
  }

  test("Test Update Table") {
    withRecordType()(withTempDir { tmp =>
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName
          // create table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
         """.stripMargin)

          // insert data to table
          spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 10.0, 1000)
          )

          // test with optimized sql writes enabled / disabled.
          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {

            // update data
            spark.sql(s"update $tableName set price = 20 where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 20.0, 1000)
            )

            // update data
            spark.sql(s"update $tableName set price = price * 2 where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 40.0, 1000)
            )
          }
        }
      }
    })
  }

  test("Test Update Table With Row Merge Handle") {
    // Same statements as above with the merge handle pinned to HoodieWriteMergeHandle, so the write
    // builds payload-carrying records and merges on the ordering value each record carries.
    withRecordType()(withTempDir { tmp =>
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  '${HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key()}' = 'org.apache.hudi.io.HoodieWriteMergeHandle'
               | )
         """.stripMargin)

          spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 10.0, 1000)
          )

          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {

            // the ordering column is not assigned, so the update must take effect
            spark.sql(s"update $tableName set price = 20 where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 20.0, 1000)
            )

            spark.sql(s"update $tableName set price = price * 2 where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 40.0, 1000)
            )
          }
        }
      }
    })
  }

  test("Test Update Table Without Primary Key") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq(true, false).foreach { isPartitioned =>
          val tableName = generateTableName
          val partitionedClause = if (isPartitioned) {
            "PARTITIONED BY (name)"
          } else {
            ""
          }
          // create table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  price double,
               |  ts long,
               |  name string
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | $partitionedClause
            """.stripMargin)

          // insert data to table
          spark.sql(s"insert into $tableName select 1,10, 1000, 'a1'")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 10.0, 1000)
          )

          // test with optimized sql writes enabled.
          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> "true") {

            // update data
            spark.sql(s"update $tableName set price = 20 where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 20.0, 1000)
            )

            // update data
            spark.sql(s"update $tableName set price = price * 2 where id = 1")
            checkAnswer(s"select id, name, price, ts from $tableName")(
              Seq(1, "a1", 40.0, 1000)
            )

            // verify default compaction w/ MOR
            if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
              spark.sql(s"update $tableName set price = price * 2 where id = 1")
              spark.sql(s"update $tableName set price = price * 2 where id = 1")
              spark.sql(s"update $tableName set price = price * 2 where id = 1")
              // verify compaction is complete
              val metaClient = createMetaClient(spark, tmp.getCanonicalPath + "/" + tableName)
              assertEquals(metaClient.getActiveTimeline.getLastCommitMetadataWithValidData.get.getLeft.getAction, "commit")
            }
          }
        }
      }
    })
  }

  test("Test Update Table On Non-PK Condition") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach {tableType =>
        /** non-partitioned table */
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        // insert data to table
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 1000)")

        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 1000)
        )

        // update data on non-pk condition
        spark.sql(s"update $tableName set price = 11.0, ts = 1001 where name = 'a1'")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 11.0, 1001),
          Seq(2, "a2", 20.0, 1000)
        )

        /** partitioned table */
        val ptTableName = generateTableName + "_pt"
        // create table
        spark.sql(
          s"""
             |create table $ptTableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  pt string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$ptTableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by (pt)
          """.stripMargin)

        // insert data to table
        spark.sql(
          s"""
             |insert into $ptTableName
             |values (1, 'a1', 10.0, 1000, "2021"), (2, 'a2', 20.0, 1000, "2021"), (3, 'a2', 30.0, 1000, "2022")
             |""".stripMargin)

        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021"),
          Seq(2, "a2", 20.0, 1000, "2021"),
          Seq(3, "a2", 30.0, 1000, "2022")
        )

        // update data on non-pk condition
        spark.sql(s"update $ptTableName set price = price * 1.1, ts = ts + 1 where name = 'a2'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 10.0, 1000, "2021"),
          Seq(2, "a2", 22.0, 1001, "2021"),
          Seq(3, "a2", 33.0, 1001, "2022")
        )

        spark.sql(s"update $ptTableName set price = price + 5, ts = ts + 1 where pt = '2021'")
        checkAnswer(s"select id, name, price, ts, pt from $ptTableName")(
          Seq(1, "a1", 15.0, 1001, "2021"),
          Seq(2, "a2", 27.0, 1002, "2021"),
          Seq(3, "a2", 33.0, 1001, "2022")
        )
      }
    })
  }

  test("Test ignoring case for Update Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  ID int,
             |  NAME string,
             |  PRICE double,
             |  TS long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | options (
             |  type = '$tableType',
             |  primaryKey = 'ID',
             |  preCombineField = 'TS'
             | )
       """.stripMargin)
        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set PRICE = 20 where ID = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 20.0, 1000)
        )

        // update data
        spark.sql(s"update $tableName set PRICE = PRICE * 2 where ID = 1")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 40.0, 1000)
        )
      }
    }
  }

  test("Test decimal type") {
    withTempDir { tmp =>
      Seq(true, false).foreach { sparkSqlOptimizedWrites =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  ff decimal(38, 10)
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
     """.stripMargin)

        // insert data to table
        spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000, 10.0")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // test with optimized sql writes enabled / disabled.
        withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {
          spark.sql(s"update $tableName set price = 22 where id = 1")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 22.0, 1000)
          )
        }
      }
    }
  }

  // Test update table with clustering
  test("Test Update Table with Clustering") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  partition long
             |) using hudi
             | partitioned by (partition)
             | location '$basePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.clustering.keygen.class = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
             | )
       """.stripMargin)

        // insert data
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002, 1002)")

        // update data
        spark.sql(s"update $tableName set price = 20 where id > 1")
        checkAnswer(s"select id, name, price, ts from $tableName where id > 1")(
          Seq(2, "a2", 20.0, 1001),
          Seq(3, "a3", 20.0, 1002)
        )

        // update data
        spark.sql(s"update $tableName set price = price * 2 where id = 2")
        checkAnswer(s"select id, name, price, ts from $tableName where id = 2")(
          Seq(2, "a2", 40.0, 1001)
        )

        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))
        // Generate the first clustering plan
        val firstScheduleInstant = client.scheduleClustering(HOption.empty()).get()
        checkAnswer(s"call show_clustering(path => '$basePath', show_involved_partition => true)")(
          Seq(firstScheduleInstant, 3, HoodieInstant.State.REQUESTED.name(), "partition=1000,partition=1001,partition=1002")
        )
        // Do clustering for all the clustering plan
        checkAnswer(s"call run_clustering(path => '$basePath', order => 'partition')")(
          Seq(firstScheduleInstant, 3, HoodieInstant.State.COMPLETED.name(), "*")
        )
      }
    }
  }

  test("Test Update Table With Primary Key and Partition Key Updates error out") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        // create table with primary key and partition
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  pt string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by (pt)
       """.stripMargin)

        // Insert initial data
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 1000, '2021')")

        // Verify initial state
        checkAnswer(s"select id, name, price, ts, pt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2021")
        )

        // Try to update primary key (should fail)
        val e1 = intercept[AnalysisException] {
          spark.sql(s"update $tableName set id = 2 where id = 1")
        }

        if (gteqSpark3_4) {
          assert(e1.getMessage.contains(s"Detected disallowed assignment clause in UPDATE statement for record key field `id`" +
            s" for table `spark_catalog.default.$tableName`. Please remove the assignment clause to avoid the error."))
        } else {
          assert(e1.getMessage.contains(s"Detected disallowed assignment clause in UPDATE statement for record key field `id`" +
            s" for table `default.$tableName`. Please remove the assignment clause to avoid the error."))
        }

        // Try to update partition column (should fail)
        val e2 = intercept[AnalysisException] {
          spark.sql(s"update $tableName set pt = '2022' where id = 1")
        }
        if (gteqSpark3_4) {
          assert(e2.getMessage.contains(s"Detected disallowed assignment clause in UPDATE statement for partition field `pt`" +
            s" for table `spark_catalog.default.$tableName`. Please remove the assignment clause to avoid the error."))
        } else {
          assert(e2.getMessage.contains(s"Detected disallowed assignment clause in UPDATE statement for partition field `pt`" +
            s" for table `default.$tableName`. Please remove the assignment clause to avoid the error."))
        }

        // Verify data remains unchanged after failed updates
        checkAnswer(s"select id, name, price, ts, pt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2021")
        )

        // Verify normal update still works
        spark.sql(s"update $tableName set price = 20.0 where id = 1")
        checkAnswer(s"select id, name, price, ts, pt from $tableName")(
          Seq(1, "a1", 20.0, 1000, "2021")
        )
      }
    })
  }

  test("Test Update Partitioned Table Without Primary Key") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = '$tableType',
             |  preCombineField = 'price'
             |)
             |partitioned by (ts)
             |""".stripMargin)

        // test with optimized sql writes enabled.
        withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> "true") {

          // insert data to table
          spark.sql(
            s"""
               |insert into $tableName
               |values
               |  (1, 'a1', cast(10.0 as double), 1000),
               |  (2, 'a2', cast(20.0 as double), 1000),
               |  (3, 'a2', cast(30.0 as double), 1000)
               |""".stripMargin)
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000),
            Seq(3, "a2", 30.0, 1000)
          )

          // Update the row with id = 2 by setting price to 25.0
          spark.sql(s"update $tableName set price = cast(25.0 as double) where id = 2")

          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 25.0, 1000),
            Seq(3, "a2", 30.0, 1000)
          )
        }
      }
    }
  }

  test("Test Update Table moves a record to the partition it gives it in a table without a record key") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             | create table $tableName (id int, name string, dep string) using hudi
             | tblproperties (type = '$tableType')
             | partitioned by (dep)
             | location '$tablePath'
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 'd1'), (2, 'a2', 'd1'), (3, 'a3', 'd2')")

        // The compaction is left to the end of the test
        withSQLConf(HoodieCompactionConfig.INLINE_COMPACT.key -> "false") {
          // id 1 moves to a new partition and id 2 to one that holds a record, id 3 stays
          spark.sql(s"update $tableName set dep = 'd3', name = 'a1u' where id = 1")
          spark.sql(s"update $tableName set dep = 'd2' where id = 2")
          spark.sql(s"update $tableName set name = 'a3u' where id = 3")
          checkPartitionedRows(tableName, "dep")(Seq(1, "a1u", "d3"), Seq(2, "a2", "d2"), Seq(3, "a3u", "d2"))

          // A statement that moves some of the records it updates
          spark.sql(s"update $tableName set dep = if(id = 2, 'd1', dep), name = concat(name, 'v') where id in (1, 2)")
        }
        val expected = Seq(Seq(1, "a1uv", "d3"), Seq(2, "a2v", "d1"), Seq(3, "a3u", "d2"))
        checkPartitionedRows(tableName, "dep")(expected: _*)
        if (tableType == "mor") {
          compactAndCheck(tableName)
          checkPartitionedRows(tableName, "dep")(expected: _*)
        }
      }
    })
  }

  test("Test Update Table moves a record across partitions only when its update applies under event time ordering") {
    withTempDir { tmp =>
      // A table without a record key, and one with a record key and a global index that moves records
      Seq("cow", "mor").flatMap(tableType => Seq((tableType, ""), (tableType, "primaryKey = 'id', 'hoodie.index.type' = 'GLOBAL_SIMPLE',")))
        .foreach { case (tableType, keyProps) =>
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (id int, name string, ts long, dep string) using hudi
               | tblproperties (type = '$tableType', $keyProps preCombineField = 'ts')
               | partitioned by (dep)
               | location '${tmp.getCanonicalPath}/$tableName'
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'a1', 100, 'd1'), (2, 'a2', 100, 'd1')")
          val unchanged = Seq(Seq(1, "a1", 100L, "d1", "dep=d1"), Seq(2, "a2", 100L, "d1", "dep=d1"))

          withSQLConf(HoodieCompactionConfig.INLINE_COMPACT.key -> "false") {
            // An update with a lower ordering value than the record leaves it as it is, whether or not it also gives
            // the record another partition
            spark.sql(s"update $tableName set ts = 90, name = 'a1u' where id = 1")
            checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName order by id")(unchanged: _*)
            spark.sql(s"update $tableName set ts = 90, name = 'a1u', dep = 'd2' where id = 1")
            checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName order by id")(unchanged: _*)

            // A statement that applies the update of id 2 only, which it moves
            spark.sql(s"update $tableName set ts = if(id = 1, 90, 110), name = concat(name, 'u'), dep = 'd2' where id in (1, 2)")
          }
          val expected = Seq(Seq(1, "a1", 100L, "d1", "dep=d1"), Seq(2, "a2u", 110L, "d2", "dep=d2"))
          checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName order by id")(expected: _*)
          if (tableType == "mor") {
            compactAndCheck(tableName)
            checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName order by id")(expected: _*)
          }
        }
    }
  }

  test("Test Update Table keeps a record in its partition when its update has a lower nested ordering value") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (id int, name string, attributes struct<ts: long, src: string>, dep string) using hudi
             | tblproperties (type = '$tableType', orderingFields = 'attributes.ts')
             | partitioned by (dep)
             | location '${tmp.getCanonicalPath}/$tableName'
           """.stripMargin)
        spark.sql(s"insert into $tableName select 1, 'a1', named_struct('ts', 100L, 'src', 's1'), 'd1'")

        withSQLConf(HoodieCompactionConfig.INLINE_COMPACT.key -> "false") {
          // A lower nested ordering value leaves the record as it is, as the same update without a partition change does
          spark.sql(s"update $tableName set attributes = named_struct('ts', 90L, 'src', 's2'), name = 'a1u', dep = 'd2' where id = 1")
        }
        checkAnswer(s"select id, name, attributes.ts, dep, _hoodie_partition_path from $tableName")(Seq(1, "a1", 100L, "d1", "dep=d1"))
      }
    }
  }

  test("Test Update Table refuses to move a record across partitions when a custom merger decides whether its update applies") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           | create table $tableName (id int, name string, ts long, dep string) using hudi
           | tblproperties (
           |   type = 'cow',
           |   orderingFields = 'ts',
           |   'hoodie.write.record.merge.mode' = 'CUSTOM',
           |   'hoodie.write.record.merge.strategy.id' = '${HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID}',
           |   '${HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY}' = '${classOf[CustomSparkRecordMergerForClustering].getName}')
           | partitioned by (dep)
           | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 100, 'd1')")

      checkExceptionContain(s"update $tableName set ts = 110, dep = 'd2' where id = 1")(
        s"UPDATE cannot change the partition field(s) `dep` of table `spark_catalog.default.$tableName` under the " +
          "CUSTOM merge mode, as it cannot tell whether the merge applies the update to a record.")
      checkAnswer(s"select id, name, ts, dep, _hoodie_partition_path from $tableName")(Seq(1, "a1", 100L, "d1", "dep=d1"))
    }
  }

  test("Test UPDATE on VECTOR column preserves custom-type metadata") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id bigint,
           |  embedding VECTOR(3)
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id'
           | )
         """.stripMargin)

      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, array(cast(0.1 as float), cast(0.2 as float), cast(0.3 as float)))
           """.stripMargin)

      // Assigning a VECTOR column goes through castIfNeeded; without the metadata
      // re-attach it would fail schema compat with MISSING_UNION_BRANCH.
      spark.sql(
        s"""
           |update $tableName
           |set embedding = array(cast(0.9 as float), cast(0.8 as float), cast(0.7 as float))
           |where id = 1
           """.stripMargin)

      checkAnswer(s"select id, embedding from $tableName")(
        Seq(1L, Seq(0.9f, 0.8f, 0.7f))
      )
    }
  }

  test("Test UPDATE on BLOB column preserves custom-type metadata") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id bigint,
           |  payload BLOB
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id'
           | )
         """.stripMargin)

      // Use OUT_OF_LINE with a concrete reference: per RFC-100 the BLOB struct's
      // inner reference fields (external_path, managed) are non-null, and the
      // source literal must conform to that contract.
      spark.sql(
        s"""
           |insert into $tableName values
           |  (1, named_struct(
           |        'type', 'OUT_OF_LINE',
           |        'data', cast(null as binary),
           |        'reference', named_struct(
           |          'external_path', 'blobs/seed',
           |          'offset', 0L,
           |          'length', 3L,
           |          'managed', false)))
           """.stripMargin)

      // Assigning a BLOB column goes through castIfNeeded; without the metadata
      // re-attach it would fail schema compat with MISSING_UNION_BRANCH.
      spark.sql(
        s"""
           |update $tableName
           |set payload = named_struct(
           |  'type', 'OUT_OF_LINE',
           |  'data', cast(null as binary),
           |  'reference', named_struct(
           |    'external_path', 'blobs/updated',
           |    'offset', 10L,
           |    'length', 100L,
           |    'managed', true))
           |where id = 1
           """.stripMargin)

      checkAnswer(s"select id, payload from $tableName")(
        Seq(1L, Row("OUT_OF_LINE", null, Row("blobs/updated", 10L, 100L, true)))
      )
    }
  }
}
