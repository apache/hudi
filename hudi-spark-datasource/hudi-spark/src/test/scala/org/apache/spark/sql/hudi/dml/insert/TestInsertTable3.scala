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

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getLastCommitMetadata
import org.junit.jupiter.api.Assertions.assertEquals

class TestInsertTable3 extends HoodieSparkSqlTestBase {
  test("Test Insert Into With Catalog Identifier") {
    Seq("hudi", "parquet").foreach { format =>
      withTempDir { tmp =>
        val tableName = s"spark_catalog.default.$generateTableName"
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using $format
             | tblproperties (primaryKey = 'id')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
        // Insert into dynamic partition
        spark.sql(
          s"""
             | insert into $tableName
             | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
        // Insert into static partition
        spark.sql(
          s"""
             | insert into $tableName partition(dt = '2021-01-05')
             | select 2 as id, 'a2' as name, 10 as price, 1000 as ts
        """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 10.0, 1000, "2021-01-05")
        )
      }
    }
  }

  test("Test enable hoodie.merge.allow.duplicate.on.inserts when write") {
    withSQLConf("hoodie.datasource.write.operation" -> "insert") {
      Seq("mor", "cow").foreach { tableType =>
        withTempDir { tmp =>
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
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  type = '$tableType'
               | )
        """.stripMargin)
          spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (1, 'a1', 10, 1000)")
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1", 10, 1000, "2021-12-25")
          )
          withSQLConf("hoodie.merge.allow.duplicate.on.inserts" -> "false") {
            spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (1, 'a2', 20, 1001)")
            checkAnswer(s"select id, name, price, ts, dt from $tableName")(
              Seq(1, "a2", 20, 1001, "2021-12-25")
            )
          }
          withSQLConf("hoodie.merge.allow.duplicate.on.inserts" -> "true") {
            spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (1, 'a3', 30, 1002)")
            checkAnswer(s"select id, name, price, ts, dt from $tableName")(
              Seq(1, "a2", 20, 1001, "2021-12-25"),
              Seq(1, "a3", 30, 1002, "2021-12-25")
            )
          }
        }
      }
    }
  }

  test("Test INSERT INTO preserves duplicates by default") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf(DataSourceWriteOptions.INSERT_DUP_POLICY.key())
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    spark.sessionState.conf.unsetConf("hoodie.sql.bulk.insert.enable")
    spark.sessionState.conf.unsetConf("hoodie.merge.allow.duplicate.on.inserts")
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id BIGINT,
           |  ts BIGINT,
           |  v STRING
           |) USING hudi
           |TBLPROPERTIES (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |location '${tmp.getCanonicalPath}/$tableName'
           |""".stripMargin)

      // Insert initial batch with same-batch duplicates
      spark.sql(
        s"""
           |INSERT INTO $tableName VALUES
           |  (1, 1000, 'a'),
           |  (1, 1001, 'a_dup'),
           |  (2, 1002, 'b')
           |""".stripMargin)

      // Verify both duplicates are present (id=1 appears twice)
      checkAnswer(s"SELECT id, ts, v FROM $tableName ORDER BY id, ts")(
        Seq(1, 1000, "a"),
        Seq(1, 1001, "a_dup"),
        Seq(2, 1002, "b")
      )

      // Insert another batch with another duplicate for id=1
      spark.sql(s"INSERT INTO $tableName VALUES (1, 1003, 'a_again'), (3, 1004, 'c')")

      // Verify all three duplicates for id=1 are present
      checkAnswer(s"SELECT id, ts, v FROM $tableName ORDER BY id, ts")(
        Seq(1, 1000, "a"),
        Seq(1, 1001, "a_dup"),
        Seq(1, 1003, "a_again"),
        Seq(2, 1002, "b"),
        Seq(3, 1004, "c")
      )
    }
  }

  test("Test INSERT INTO deduplicates when operation is set to upsert") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "upsert") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tableName (
             |  id BIGINT,
             |  ts BIGINT,
             |  v STRING
             |) USING hudi
             |TBLPROPERTIES (
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |location '${tmp.getCanonicalPath}/$tableName'
             |""".stripMargin)

        // Insert initial batch with same-batch duplicates
        spark.sql(
          s"""
             |INSERT INTO $tableName VALUES
             |  (1, 1000, 'a'),
             |  (1, 1001, 'a_dup'),
             |  (2, 1002, 'b')
             |""".stripMargin)

        // Verify only latest record for id=1 is present (ts=1001)
        checkAnswer(s"SELECT id, ts, v FROM $tableName ORDER BY id, ts")(
          Seq(1, 1001, "a_dup"),
          Seq(2, 1002, "b")
        )

        // Insert another batch with another duplicate for id=1
        spark.sql(s"INSERT INTO $tableName VALUES (1, 1003, 'a_again'), (3, 1004, 'c')")

        // Verify only latest record for id=1 is present (ts=1003)
        checkAnswer(s"SELECT id, ts, v FROM $tableName ORDER BY id, ts")(
          Seq(1, 1003, "a_again"),
          Seq(2, 1002, "b"),
          Seq(3, 1004, "c")
        )
      }
    }
  }

  test("Test Insert Into Bucket Index Table") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "upsert") {
      spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
      withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  dt string,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             | primaryKey = 'id,name',
             | preCombineField = 'ts',
             | hoodie.index.type = 'BUCKET',
             | hoodie.bucket.index.hash.field = 'id,name')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        // Note: Do not write the field alias, the partition field must be placed last.
        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1,1', 10, 1000, "2021-01-05"),
             | (2, 'a2', 20, 2000, "2021-01-06"),
             | (3, 'a3', 30, 3000, "2021-01-07")
              """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )

        withSQLConf("hoodie.merge.allow.duplicate.on.inserts" -> "false") {
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 10, 1000, "2021-01-05")
              """.stripMargin)

          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
            Seq(2, "a2", 20.0, 2000, "2021-01-06"),
            Seq(3, "a3", 30.0, 3000, "2021-01-07")
          )
        }
      }
    }
  }

  test("Test Bulk Insert Into Bucket Index Table") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "1") {
      Seq("mor", "cow").foreach { tableType =>
        Seq("true", "false").foreach { bulkInsertAsRow =>
          withTempDir { tmp =>
            val tableName = generateTableName
            // Create a partitioned table
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  dt string,
                 |  name string,
                 |  price double,
                 |  ts long
                 |) using hudi
                 | tblproperties (
                 | primaryKey = 'id,name',
                 | type = '$tableType',
                 | preCombineField = 'ts',
                 | hoodie.index.type = 'BUCKET',
                 | hoodie.bucket.index.hash.field = 'id,name',
                 | hoodie.datasource.write.row.writer.enable = '$bulkInsertAsRow')
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}'
                 """.stripMargin)

            // Note: Do not write the field alias, the partition field must be placed last.
            spark.sql(
              s"""
                 | insert into $tableName values
                 | (1, 'a1,1', 10, 1000, "2021-01-05"),
                 | (2, 'a2', 20, 2000, "2021-01-06"),
                 | (3, 'a3,3', 30, 3000, "2021-01-07")
                 """.stripMargin)

            checkAnswer(s"select id, name, price, ts, dt from $tableName")(
              Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
              Seq(2, "a2", 20.0, 2000, "2021-01-06"),
              Seq(3, "a3,3", 30.0, 3000, "2021-01-07")
            )

            // for COW with disabled Spark native row writer, multiple bulk inserts are restricted
            if (tableType != "cow" && bulkInsertAsRow != "false") {
              spark.sql(
                s"""
                   | insert into $tableName values
                   | (1, 'a1', 10, 1000, "2021-01-05"),
                   | (3, "a3", 30, 3000, "2021-01-07")
                 """.stripMargin)

              checkAnswer(s"select id, name, price, ts, dt from $tableName")(
                Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
                Seq(1, "a1", 10.0, 1000, "2021-01-05"),
                Seq(2, "a2", 20.0, 2000, "2021-01-06"),
                Seq(3, "a3,3", 30.0, 3000, "2021-01-07"),
                Seq(3, "a3", 30.0, 3000, "2021-01-07")
              )

              // there are two files in partition(dt = '2021-01-05')
              checkAnswer(s"select count(distinct _hoodie_file_name) from $tableName where dt = '2021-01-05'")(
                Seq(2)
              )

              // would generate 6 other files in partition(dt = '2021-01-05')
              spark.sql(
                s"""
                   | insert into $tableName values
                   | (4, 'a1,1', 10, 1000, "2021-01-05"),
                   | (5, 'a1,1', 10, 1000, "2021-01-05"),
                   | (6, 'a1,1', 10, 1000, "2021-01-05"),
                   | (7, 'a1,1', 10, 1000, "2021-01-05"),
                   | (8, 'a1,1', 10, 1000, "2021-01-05"),
                   | (10, 'a3,3', 30, 3000, "2021-01-05")
                 """.stripMargin)

              checkAnswer(s"select count(distinct _hoodie_file_name) from $tableName where dt = '2021-01-05'")(
                Seq(8)
              )
            }
          }
        }
      }
    }
  }

  test("Test Bulk Insert Into Bucket Index Table With Remote Partitioner") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  dt string,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             | primaryKey = 'id,name',
             | type = 'cow',
             | preCombineField = 'ts',
             | hoodie.index.type = 'BUCKET',
             | hoodie.bucket.index.hash.field = 'id,name',
             | hoodie.bucket.index.remote.partitioner.enable = 'true')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        // Note: Do not write the field alias, the partition field must be placed last.
        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1,1', 10, 1000, "2021-01-05"),
             | (2, 'a2', 20, 2000, "2021-01-06"),
             | (3, 'a3,3', 30, 3000, "2021-01-07")
       """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3,3", 30.0, 3000, "2021-01-07")
        )
      }
    }
  }

  test("Test Insert Overwrite Bucket Index Table") {
    withSQLConf(
      "hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  dt string,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | tblproperties (
             | primaryKey = 'id,name',
             | type = 'cow',
             | preCombineField = 'ts',
             | hoodie.index.type = 'BUCKET',
             | hoodie.bucket.index.hash.field = 'id,name',
             | hoodie.datasource.write.row.writer.enable = 'true')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        // Note: Do not write the field alias, the partition field must be placed last.
        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1,1', 10, 1000, "2021-01-05"),
             | (2, 'a2', 20, 2000, "2021-01-06"),
             | (3, 'a3,3', 30, 3000, "2021-01-07")
       """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3,3", 30.0, 3000, "2021-01-07")
        )

        spark.sql(
          s"""
             | insert overwrite $tableName values
             | (1, 'a1,1', 100, 1000, "2021-01-05")
       """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1,1", 100.0, 1000, "2021-01-05")
        )
      }
    }
  }

  test("Test not supported multiple BULK INSERTs into COW with SIMPLE BUCKET and disabled Spark native row writer") {
    withSQLConf("hoodie.datasource.write.operation" -> "bulk_insert",
      "hoodie.bulkinsert.shuffle.parallelism" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id long,
             |  name string,
             |  ts int,
             |  par string
             |) using hudi
             | tblproperties (
             | primaryKey = 'id,name',
             | type = 'cow',
             | preCombineField = 'ts',
             | hoodie.index.type = 'BUCKET',
             | hoodie.index.bucket.engine = 'SIMPLE',
             | hoodie.bucket.index.num.buckets = '4',
             | hoodie.bucket.index.hash.field = 'id,name',
             | hoodie.datasource.write.row.writer.enable = 'false')
             | partitioned by (par)
             | location '${tmp.getCanonicalPath}'
             """.stripMargin)

        // Used rows with corresponding `bucketId`s if there are 4 buckets
        //   `id,name`    `bucketId`
        //    5,'a1,1' ->    1
        //    6,'a6,6' ->    2
        //    9,'a3,3' ->    1
        // 13,'a13,13' ->    2
        //     24,'cd' ->    0

        // buckets 1 & 2 into partition 'main', bucket 1 into partition 'side'
        spark.sql(s"insert into $tableName values (5, 'a1,1', 1, 'main'), (6, 'a6,6', 1, 'main'), (9, 'a3,3', 1, 'side')")
        // bucket 1 into 'main', bucket 2 into 'side', the whole insert will fail due to existed bucket 1 in 'main'
        val causeRegex = "Multiple bulk insert.*COW.*Spark native row writer.*not supported.*"
        checkExceptionMatch(s"insert into $tableName values (9, 'a3,3', 2, 'main'), (13, 'a13,13', 1, 'side')")(causeRegex)
        checkAnswer(spark.sql(s"select id from $tableName order by id").collect())(Seq(5), Seq(6), Seq(9))

        // bucket 0 into 'main', no bucket into 'side', will also fail,
        // bulk insert into separate not presented bucket, if there is some other buckets already written, also restricted
        checkExceptionMatch(s"insert into $tableName values (24, 'cd', 1, 'main')")(causeRegex)
        checkAnswer(spark.sql(s"select id from $tableName where par = 'main' order by id").collect())(Seq(5), Seq(6))

        // for overwrite mode it's allowed to do multiple bulk inserts
        spark.sql(s"insert overwrite $tableName values (9, 'a3,3', 3, 'main'), (13, 'a13,13', 2, 'side')")
        // only data from the latest insert overwrite is available,
        // because insert overwrite drops the whole table due to [HUDI-4704]
        checkAnswer(spark.sql(s"select id from $tableName order by id").collect())(Seq(9), Seq(13))
      }
    }
  }

  /**
   * This test is to make sure that bulk insert doesn't create a bunch of tiny files if
   * hoodie.bulkinsert.user.defined.partitioner.sort.columns doesn't start with the partition columns
   *
   * NOTE: Additionally, this test serves as a smoke test making sure that all of the bulk-insert
   * modes work
   */
  test(s"Test Bulk Insert with all sort-modes") {
    withTempDir { basePath =>
      BulkInsertSortMode.values().foreach { sortMode =>
        val tableName = generateTableName
        // Default parallelism is 200 which means in global sort, each record will end up in a different spark partition so
        // 9 files would be created. Setting parallelism to 3 so that each spark partition will contain a hudi partition.
        val parallelism = if (sortMode.name.equals(BulkInsertSortMode.GLOBAL_SORT.name())) {
          "hoodie.bulkinsert.shuffle.parallelism = 3,"
        } else {
          ""
        }
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             |) using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  preCombineField = 'name',
             |  type = 'cow',
             |  $parallelism
             |  hoodie.bulkinsert.sort.mode = '${sortMode.name}'
             | )
             | partitioned by (dt)
             | location '${basePath.getCanonicalPath}/$tableName'
                """.stripMargin)

        withSQLConf("hoodie.sql.bulk.insert.enable" -> "true",
          "hoodie.sql.insert.mode" -> "non-strict") {
          spark.sql(
            s"""insert into $tableName  values
               |(5, 'a', 35, '2021-05-21'),
               |(1, 'a', 31, '2021-01-21'),
               |(3, 'a', 33, '2021-03-21'),
               |(4, 'b', 16, '2021-05-21'),
               |(2, 'b', 18, '2021-01-21'),
               |(6, 'b', 17, '2021-03-21'),
               |(8, 'a', 21, '2021-05-21'),
               |(9, 'a', 22, '2021-01-21'),
               |(7, 'a', 23, '2021-03-21')
               |""".stripMargin)

          // TODO re-enable
          //assertResult(3)(spark.sql(s"select distinct _hoodie_file_name from $tableName").count())
        }
      }
    }
  }

  test("Test Insert Overwrite Into Bucket Index Table") {
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
      Seq("mor", "cow").foreach { tableType =>
        withTempDir { tmp =>
          val tableName = generateTableName
          // Create a partitioned table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  dt string
               |) using hudi
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  hoodie.index.type = 'BUCKET',
               |  hoodie.bucket.index.num.buckets = '4'
               |)
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

          spark.sql(
            s"""insert into $tableName  values
               |(5, 'a', 35, 1000, '2021-01-05'),
               |(1, 'a', 31, 1000, '2021-01-05'),
               |(3, 'a', 33, 1000, '2021-01-05'),
               |(4, 'b', 16, 1000, '2021-01-05'),
               |(2, 'b', 18, 1000, '2021-01-05'),
               |(6, 'b', 17, 1000, '2021-01-05'),
               |(8, 'a', 21, 1000, '2021-01-05'),
               |(9, 'a', 22, 1000, '2021-01-05'),
               |(7, 'a', 23, 1000, '2021-01-05')
               |""".stripMargin)

          // Insert overwrite static partition
          spark.sql(
            s"""
               | insert overwrite table $tableName partition(dt = '2021-01-05')
               | select * from (select 13 , 'a2', 12, 1000) limit 10
        """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
            Seq(13, "a2", 12.0, 1000, "2021-01-05")
          )
        }
      }
    }
  }

  test("Test Insert Overwrite Into Consistent Bucket Index Table") {
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
      withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             |tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.index.type = 'BUCKET',
             |  hoodie.index.bucket.engine = "CONSISTENT_HASHING",
             |  hoodie.bucket.index.num.buckets = '4'
             |)
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

        spark.sql(
          s"""insert into $tableName  values
             |(5, 'a', 35, 1000, '2021-01-05'),
             |(1, 'a', 31, 1000, '2021-01-05'),
             |(3, 'a', 33, 1000, '2021-01-05'),
             |(4, 'b', 16, 1000, '2021-01-05'),
             |(2, 'b', 18, 1000, '2021-01-05'),
             |(6, 'b', 17, 1000, '2021-01-05'),
             |(8, 'a', 21, 1000, '2021-01-05'),
             |(9, 'a', 22, 1000, '2021-01-05'),
             |(7, 'a', 23, 1000, '2021-01-05')
             |""".stripMargin)

        // Insert overwrite static partition
        spark.sql(
          s"""
             | insert overwrite table $tableName partition(dt = '2021-01-05')
             | select * from (select 13 , 'a2', 12, 1000) limit 10
        """.stripMargin)

        // Double insert overwrite static partition
        spark.sql(
          s"""
             | insert overwrite table $tableName partition(dt = '2021-01-05')
             | select * from (select 13 , 'a3', 12, 1000) limit 10
        """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
          Seq(13, "a3", 12.0, 1000, "2021-01-05")
        )
      }
    }
  }

  test("Test Hudi should not record empty orderingFields in hoodie.properties") {
    withSQLConf("hoodie.datasource.write.operation" -> "insert") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double
             |) using hudi
             |tblproperties(primaryKey = 'id')
             |location '${tmp.getCanonicalPath}/$tableName'
        """.stripMargin)

        spark.sql(s"insert into $tableName select 1, 'name1', 11")
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(1, "name1", 11.0)
        )

        spark.sql(s"insert overwrite table $tableName select 2, 'name2', 12")
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(2, "name2", 12.0)
        )

        spark.sql(s"insert into $tableName select 3, 'name3', 13")
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(2, "name2", 12.0),
          Seq(3, "name3", 13.0)
        )
      }
    }
  }

  test("Test Insert Into with auto generate record keys") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  dt string,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)

      // Note: Do not write the field alias, the partition field must be placed last.
      spark.sql(
        s"""
           | insert into $tableName values
           | (1, 'a1', 10, 1000, "2021-01-05"),
           | (2, 'a2', 20, 2000, "2021-01-06"),
           | (3, 'a3', 30, 3000, "2021-01-07")
              """.stripMargin)

      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05"),
        Seq(2, "a2", 20.0, 2000, "2021-01-06"),
        Seq(3, "a3", 30.0, 3000, "2021-01-07")
      )
      val df = spark.read.format("hudi").load(tmp.getCanonicalPath)
      assertEquals(3, df.select(HoodieRecord.RECORD_KEY_METADATA_FIELD).count())
      assertResult(WriteOperationType.INSERT) {
        getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}").getOperationType
      }
    }
  }

  test("Test Insert Into with auto generate record keys with precombine ") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  dt string,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  type = '$tableType',
             |  preCombineField = 'price'
             | )
       """.stripMargin)

        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1', 10, 1000, "2021-01-05"),
             | (2, 'a2', 20, 2000, "2021-01-06"),
             | (3, 'a3', 30, 3000, "2021-01-07")
              """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )
      }
    }
  }
}
