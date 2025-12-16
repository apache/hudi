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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, ScalaAssertionSupport}
import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField}
import org.junit.jupiter.api.Assertions.assertTrue
import org.slf4j.LoggerFactory

class TestMergeIntoTable extends HoodieSparkSqlTestBase with ScalaAssertionSupport {
  private val log = LoggerFactory.getLogger(getClass)

  test("Test MergeInto Basic") {
    Seq(true, false).foreach { sparkSqlOptimizedWrites =>
      withRecordType()(withTempDir { tmp =>
        withSparkSqlSessionConfig("hoodie.payload.combined.schema.validate" -> "false",
          // Column stats and partition stats need to be disabled to validate
          // that there is no full target table scan during MERGE INTO
          "hoodie.metadata.index.column.stats.enable" -> "false",
          "hoodie.metadata.index.partition.stats.enable" -> "false") {
          val tableName = generateTableName
          // Create table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts int,
               |  partition string
               |) using hudi
               | partitioned by (partition)
               | location '${tmp.getCanonicalPath}'
               | tblproperties (
               |  primaryKey ='id',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)

          // test with optimized sql merge enabled / disabled.
          withSQLConf(SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {
            val structFields = List(
              StructField("id", IntegerType, nullable = true),
              StructField("name", StringType, nullable = true),
              StructField("price", DoubleType, nullable = true),
              StructField("ts", IntegerType, nullable = true),
              StructField("partition", StringType, nullable = true))
            // First merge with a extra input field 'flag' (insert a new record)
            spark.sql(
              s"""
                 | merge into $tableName
                 | using (
                 |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, 'p1' as partition, '1' as flag
                 |  union
                 |  select 2 as id, 'a2' as name, 20 as price, 1000 as ts, 'p2' as partition, '1' as flag
                 |  union
                 |  select 3 as id, 'a3' as name, 30 as price, 1000 as ts, 'p3' as partition, '1' as flag
                 | ) s0
                 | on s0.id = $tableName.id
                 | when matched and flag = '1' then update set
                 | id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
                 | when not matched and flag = '1' then insert *
       """.stripMargin)
            validateTableSchema(tableName, structFields)
            checkAnswer(s"select id, name, price, ts, partition from $tableName")(
              Seq(1, "a1", 10.0, 1000, "p1"),
              Seq(2, "a2", 20.0, 1000, "p2"),
              Seq(3, "a3", 30.0, 1000, "p3")
            )

            // Second merge (update the record) with different field names in the source
            spark.sql(
              s"""
                 | merge into $tableName
                 | using (
                 |  select 1 as _id, 'a1' as name, 10 as _price, 1001 as _ts, 'p1' as partition
                 | ) s0
                 | on s0._id = $tableName.id
                 | when matched then update set
                 | id = s0._id, name = s0.name, price = s0._price + $tableName.price, ts = s0._ts
                 | """.stripMargin)
            validateTableSchema(tableName, structFields)
            checkAnswer(s"select id, name, price, ts, partition from $tableName")(
              Seq(1, "a1", 20.0, 1001, "p1"),
              Seq(2, "a2", 20.0, 1000, "p2"),
              Seq(3, "a3", 30.0, 1000, "p3")
            )

            // the third time merge (update & insert the record)
            spark.sql(
              s"""
                 | merge into $tableName
                 | using (
                 |  select * from (
                 |  select 1 as id, 'a1' as name, 10 as price, 1002 as ts, 'p1' as partition
                 |  union all
                 |  select 4 as id, 'a4' as name, 40 as price, 1001 as ts, 'p4' as partition
                 |  )
                 | ) s0
                 | on s0.id = $tableName.id
                 | when matched then update set
                 | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
                 | when not matched and s0.id % 2 = 0 then insert *
       """.stripMargin)
            validateTableSchema(tableName, structFields)
            checkAnswer(s"select id, name, price, ts, partition from $tableName")(
              Seq(1, "a1", 30.0, 1002, "p1"),
              Seq(2, "a2", 20.0, 1000, "p2"),
              Seq(3, "a3", 30.0, 1000, "p3"),
              Seq(4, "a4", 40.0, 1001, "p4")
            )

            // Validate that MERGE INTO only scan affected partitions in the target table
            // Corrupt the files in other partitions not receiving updates
            val (metaClient, fsv) = HoodieSparkSqlTestBase.getMetaClientAndFileSystemView(tmp.getCanonicalPath)
            Seq("p2", "p3", "p4").map(e => "partition=" + e).foreach(partition => {
              assertTrue(fsv.getLatestFileSlices(partition).count() > 0)
              fsv.getLatestFileSlices(partition).forEach(fileSlice => {
                if (fileSlice.getBaseFile.isPresent) {
                  HoodieSparkSqlTestBase.replaceWithEmptyFile(
                    metaClient.getStorage, fileSlice.getBaseFile.get.getStoragePath)
                }
                fileSlice.getLogFiles.forEach(logFile =>
                  HoodieSparkSqlTestBase.replaceWithEmptyFile(metaClient.getStorage, logFile.getPath)
                )
              })
            })
            // the fourth merge (delete the record)
            spark.sql(
              s"""
                 | merge into $tableName
                 | using (
                 |  select 1 as id, 'a1' as name, 12 as price, 1003 as ts, 'p1' as partition
                 | ) s0
                 | on s0.id = $tableName.id
                 | when matched and s0.id != 1 then update set
                 |    id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
                 | when matched and s0.id = 1 then delete
                 | when not matched then insert *
       """.stripMargin)
            val cnt = spark.sql(s"select * from $tableName where partition = 'p1'").count()
            assertResult(0)(cnt)
          }
        }
      })
    }
  }

  /**
   * In Spark 3.0.x, UPDATE and DELETE can appear at most once in MATCHED clauses in a MERGE INTO statement.
   * Refer to: `org.apache.spark.sql.catalyst.parser.AstBuilder#visitMergeIntoTable`
   *
   * The test also provides test coverage for "Test merge into allowed patterns of assignment clauses".
   */
  test("Test MergeInto with more than once update actions") {

    withTempDir { tmp =>
      val targetTable = generateTableName
      spark.sql(
        s"""
           |create table $targetTable (
           |  id int,
           |  name string,
           |  data int,
           |  country string,
           |  ts int
           |) using hudi
           |tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           |partitioned by (country)
           |location '${tmp.getCanonicalPath}/$targetTable'
           |""".stripMargin)
      spark.sql(
        s"""
           |merge into $targetTable as target
           |using (
           |select 1 as id, 'lb' as name, 6 as data, 'shu' as country, 43193 as ts
           |) source
           |on source.id = target.id
           |when matched then
           |update set *
           |when not matched then
           |insert *
           |""".stripMargin)
      spark.sql(
        s"""
           |merge into $targetTable as target
           |using (
           |select 1 as id, 'lb' as name, 5 as data, 'shu' as country, 43196 as ts
           |) source
           |on source.id = target.id
           |when matched and source.data > target.data then
           |update set target.data = source.data, target.ts = source.ts
           |when matched and source.data = 5 then
           |update set target.data = source.data, target.ts = source.ts
           |when not matched then
           |insert *
           |""".stripMargin)

      checkAnswer(s"select id, name, data, country, ts from $targetTable")(
        Seq(1, "lb", 5, "shu", 43196)
      )
    }
  }


  /**
   * Test MIT with global index.
   * HUDI-7131
   */
  test("Test Merge Into with Global Index") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.index.type" -> "GLOBAL_BLOOM") {
        val targetTable = generateTableName
        spark.sql(
          s"""
             |create table ${targetTable} (
             |  id int,
             |  version int,
             |  name string,
             |  inc_day string
             |) using hudi
             |tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id'
             | )
             |partitioned by (inc_day)
             |location '${tmp.getCanonicalPath}/$targetTable'
             |""".stripMargin)
        spark.sql(
          s"""
             |merge into ${targetTable} as target
             |using (
             |select 1 as id, 1 as version, 'str_1' as name, '2023-10-01' as inc_day
             |) source
             |on source.id = target.id
             |when matched then
             |update set *
             |when not matched then
             |insert *
             |""".stripMargin)
        spark.sql(
          s"""
             |merge into ${targetTable} as target
             |using (
             |select 1 as id, 2 as version, 'str_2' as name, '2023-10-01' as inc_day
             |) source
             |on source.id = target.id
             |when matched then
             |update set *
             |when not matched then
             |insert *
             |""".stripMargin)

        checkAnswer(s"select id, version, name, inc_day from $targetTable")(
          Seq(1, 2, "str_2", "2023-10-01")
        )
        // migrate the record to a new partition.

        spark.sql(
          s"""
             |merge into ${targetTable} as target
             |using (
             |select 1 as id, 2 as version, 'str_2' as name, '2023-10-02' as inc_day
             |) source
             |on source.id = target.id
             |when matched then
             |update set *
             |when not matched then
             |insert *
             |""".stripMargin)

        checkAnswer(s"select id, version, name, inc_day from $targetTable")(
          Seq(1, 2, "str_2", "2023-10-02")
        )
      }
    })
    spark.sessionState.conf.unsetConf("hoodie.index.type")
  }

  test("Test MergeInto with ignored record") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
        val sourceTable = generateTableName
        val targetTable = generateTableName
        // Create source table
        spark.sql(
          s"""
             | create table $sourceTable (
             |  id int,
             |  name string,
             |  price double,
             |  ts int
             | ) using parquet
             | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
        // Create target table
        spark.sql(
          s"""
             |create table $targetTable (
             |  id int,
             |  name string,
             |  price double,
             |  ts int
             |) using hudi
             | location '${tmp.getCanonicalPath}/$targetTable'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)
        // Insert data to source table
        spark.sql(s"insert into $sourceTable values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $sourceTable values(2, 'a2', 11, 1000)")

        spark.sql(
          s"""
             | merge into $targetTable as t0
             | using (select * from $sourceTable) as s0
             | on t0.id = s0.id
             | when matched then update set *
             | when not matched and s0.name = 'a1' then insert *
         """.stripMargin)
        // The record of "name = 'a2'" will be filter
        checkAnswer(s"select id, name, price, ts from $targetTable")(
          Seq(1, "a1", 10.0, 1000)
        )

        spark.sql(s"insert into $targetTable select 3, 'a3', 12, 1000")
        checkAnswer(s"select id, name, price, ts from $targetTable")(
          Seq(1, "a1", 10.0, 1000),
          Seq(3, "a3", 12, 1000)
        )

        spark.sql(
          s"""
             | merge into $targetTable as t0
             | using (
             |  select * from (
             |    select 1 as s_id, 'a1' as name, 20 as price, 1001 as ts
             |    union all
             |    select 3 as s_id, 'a3' as name, 20 as price, 1003 as ts
             |    union all
             |    select 4 as s_id, 'a4' as name, 10 as price, 1004 as ts
             |  )
             | ) s0
             | on s0.s_id = t0.id
             | when matched and s0.ts = 1001 then update set id = s0.s_id, name = t0.name, price =
             | s0.price, ts = s0.ts
         """.stripMargin
        )
        // Ignore the update for id = 3
        checkAnswer(s"select id, name, price, ts from $targetTable")(
          Seq(1, "a1", 20.0, 1001),
          Seq(3, "a3", 12.0, 1000)
        )
      }
    })
  }

  test("Test MergeInto with changing partition and global index") {
    Seq(true, false).foreach { updatePartitionPathEnabled =>
      withTempDir { tmp =>
        withSQLConf("hoodie.index.type" -> "GLOBAL_SIMPLE",
          "hoodie.simple.index.update.partition.path" -> updatePartitionPathEnabled.toString) {
          Seq("cow", "mor").foreach { tableType =>
            val targetTable = generateTableName
            spark.sql(
              s"""
                 | create table $targetTable (
                 |  id int,
                 |  version int,
                 |  mergeCond string,
                 |  partition string
                 | ) using hudi
                 | partitioned by (partition)
                 | tblproperties (
                 |    'primaryKey' = 'id',
                 |    'type' = '$tableType',
                 |    'payloadClass' = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
                 |    'payloadType' = 'CUSTOM',
                 |    preCombineField = 'version'
                 | )
                 | location '${tmp.getCanonicalPath}/$targetTable'
             """.stripMargin)

            spark.sql(s"insert into $targetTable values(1, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(2, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(3, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(4, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(5, 1, 'insert', '2023-10-01')")
            spark.sql(s"insert into $targetTable values(6, 1, 'insert', '2023-10-01')")

            val sourceTable = generateTableName
            spark.sql(
              s"""
                 | create table $sourceTable (
                 | id int,
                 | version int,
                 | mergeCond string,
                 | partition string
                 | ) using parquet
                 | partitioned by (partition)
                 | location '${tmp.getCanonicalPath}/$sourceTable'
            """.stripMargin)

            //merge cond matches and partition is changed
            spark.sql(s"insert into $sourceTable values(1, 2, 'yes', '2023-10-02')")
            //merge cond does not match and partition is changed
            spark.sql(s"insert into $sourceTable values(2, 2, 'no', '2023-10-02')")
            //merge cond matches and partition is not changed
            spark.sql(s"insert into $sourceTable values(3, 2, 'yes', '2023-10-01')")
            //merge cond does not match and partition is not changed
            spark.sql(s"insert into $sourceTable values(4, 2, 'no', '2023-10-01')")
            //merge cond is delete and partition is changed
            spark.sql(s"insert into $sourceTable values(5, 2, 'delete', '2023-10-02')")
            //merge cond is delete and partition is not changed
            spark.sql(s"insert into $sourceTable values(6, 2, 'delete', '2023-10-01')")
            //id does not match
            spark.sql(s"insert into $sourceTable values(7, 1, 'insert', '2023-10-01')")

            spark.sql(
              s"""
                 | merge into $targetTable t using
                 | (select * from $sourceTable) as s
                 | on t.id=s.id
                 | when matched and s.mergeCond = 'yes' then update set *
                 | when matched and s.mergeCond = 'delete' then delete
                 | when not matched then insert *
             """.stripMargin)
            val updatedPartitionPath = if (updatePartitionPathEnabled) "partition=2023-10-02" else "partition=2023-10-01"
            checkAnswer(s"select id,version,_hoodie_partition_path from $targetTable order by id")(
              Seq(1, 2, updatedPartitionPath),
              Seq(2, 1, "partition=2023-10-01"),
              Seq(3, 2, "partition=2023-10-01"),
              Seq(4, 1, "partition=2023-10-01"),
              Seq(7, 1, "partition=2023-10-01"))
          }
        }
      }
    }
  }

  test("Test MergeInto for MOR table") {
    withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true", MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
        val tableName = generateTableName
        // Create a mor partitioned table.
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts int,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}'
         """.stripMargin)
        val structFields = List(
          StructField("id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true),
          StructField("ts", IntegerType, nullable = true),
          StructField("dt", StringType, nullable = true))

        // Insert data
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
        )
        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id,name,price,dt from $tableName")(
          Seq(1, "a1", 10, "2021-03-21")
        )
        // Update data when matched-condition not matched.
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 1 as _id, 'a1' as name, 12 as _price, 1001 as _ts, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0._id
             | when matched and s0._id % 2 = 0 then update set
             |  id = s0._id, name = s0.name, price = s0._price, ts = s0._ts, dt = s0.dt
         """.stripMargin
        )
        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id,name,price,dt from $tableName")(
          Seq(1, "a1", 10, "2021-03-21")
        )
        // Update data when matched-condition matched.
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 1 as _id, 'a1' as name, 12 as _price, 1001 as _ts, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0._id
             | when matched and s0._id % 2 = 1 then update set
             |  id = s0._id, name = s0.name, price = s0._price, ts = s0._ts, dt = s0.dt
         """.stripMargin
        )
        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id,name,price,dt from $tableName")(
          Seq(1, "a1", 12, "2021-03-21")
        )
        // Insert a new data.
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
             | ) as s0
             | on t0.id = s0.id
             | when not matched and s0.id % 2 = 0 then insert *
         """.stripMargin
        )
        checkAnswer(s"select id,name,price,dt from $tableName order by id")(
          Seq(1, "a1", 12, "2021-03-21"),
          Seq(2, "a2", 10, "2021-03-21")
        )
        // Update with different source column names.
        spark.sql(
          s"""
             | merge into $tableName t0
             | using (
             |  select 2 as s_id, 'a2' as s_name, 15 as s_price, 1001 as s_ts, '2021-03-21' as dt
             | ) s0
             | on t0.id = s0.s_id
             | when matched and s_ts = 1001
             | then update set id = s_id, name = s_name, price = s_price, ts = s_ts, t0.dt = s0.dt
         """.stripMargin
        )
        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id,name,price,dt from $tableName order by id")(
          Seq(1, "a1", 12, "2021-03-21"),
          Seq(2, "a2", 15, "2021-03-21")
        )

        // Delete with condition expression.
        val errorMessage = "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key and partition path column. Found `t0.id = (s0.s_id + 1)`"

        checkException(
          s"""
             | merge into $tableName t0
             | using (
             |  select 1 as s_id, 'a2' as s_name, 15 as s_price, 1001 as s_ts, '2021-03-21' as dt
             | ) s0
             | on t0.id = s0.s_id + 1
             | when matched and s_ts = 1001 then delete
         """.stripMargin
        )(errorMessage)

        spark.sql(
          s"""
             | merge into $tableName t0
             | using (
             |  select 2 as s_id, 'a2' as s_name, 15 as s_price, 1001 as ts, '2021-03-21' as dt
             | ) s0
             | on t0.id = s0.s_id
             | when matched and s0.ts = 1001 then delete
         """.stripMargin
        )
        checkAnswer(s"select id,name,price,dt from $tableName order by id")(
          Seq(1, "a1", 12, "2021-03-21")
        )
      }
    }
  }

  test("Test MergeInto with insert only") {
    withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true", MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
        // Create a partitioned mor table
        val tableName = generateTableName
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}'
         """.stripMargin)

        spark.sql(s"insert into $tableName select 1, 'a1', 10, '2021-03-21'")
        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-20' as dt
             | ) s0
             | on s0.id = t0.id
             | when not matched and s0.id % 2 = 0 then insert (id,name,price,dt)
             | values(s0.id,s0.name,s0.price,s0.dt)
         """.stripMargin)
        checkAnswer(s"select id,name,price,dt from $tableName order by id")(
          Seq(1, "a1", 10, "2021-03-21"),
          Seq(2, "a2", 10, "2021-03-20")
        )

        spark.sql(
          s"""
             | merge into $tableName as t0
             | using (
             |  select 3 as id, 'a3' as name, 10 as price, 1000 as ts, '2021-03-20' as dt
             | ) s0
             | on s0.id = t0.id
             | when not matched and s0.id % 2 = 0 then insert (id,name,price,dt)
             | values(s0.id,s0.name,s0.price,s0.dt)
         """.stripMargin)
        // id = 3 should not write to the table as it has filtered by id % 2 = 0
        checkAnswer(s"select id,name,price,dt from $tableName order by id")(
          Seq(1, "a1", 10, "2021-03-21"),
          Seq(2, "a2", 10, "2021-03-20")
        )
      }
    }
  }

  test("Test MergeInto For PreCombineField") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true", MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
        Seq("cow", "mor").foreach { tableType =>
          val tableName1 = generateTableName
          // Create a mor partitioned table.
          spark.sql(
            s"""
               | create table $tableName1 (
               |  id int,
               |  name string,
               |  price double,
               |  v int,
               |  dt string
               | ) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'v',
               |  hoodie.compaction.payload.class = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload'
               | )
               | partitioned by(dt)
               | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
          // Insert data
          spark.sql(
            s"""
               | merge into $tableName1 as t0
               | using (
               |  select 1 as id, 'a1' as name, 10 as price, 1001 as v, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
          )
          checkAnswer(s"select id,name,price,dt,v from $tableName1")(
            Seq(1, "a1", 10, "2021-03-21", 1001)
          )

          // Update data with a smaller version value
          spark.sql(
            s"""
               | merge into $tableName1 as t0
               | using (
               |  select 1 as id, 'a1' as name, 11 as price, 1000 as v, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
          )
          // Update failed as v = 1000 < 1001
          checkAnswer(s"select id,name,price,dt,v from $tableName1")(
            Seq(1, "a1", 10, "2021-03-21", 1001)
          )

          // Update data with a bigger version value
          spark.sql(
            s"""
               | merge into $tableName1 as t0
               | using (
               |  select 1 as id, 'a1' as name, 12 as price, 1002 as v, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when matched and s0.id % 2 = 1 then update set *
         """.stripMargin
          )
          // Update success
          checkAnswer(s"select id,name,price,dt,v from $tableName1")(
            Seq(1, "a1", 12, "2021-03-21", 1002)
          )
        }
      }
    })
  }

  test("Test MergeInto with preCombine field expression") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withSQLConf("hoodie.payload.combined.schema.validate" -> "true", MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
          val tableName1 = generateTableName
          spark.sql(
            s"""
               | create table $tableName1 (
               |  id int,
               |  name string,
               |  price double,
               |  v int,
               |  dt string
               | ) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  orderingFields = 'v'
               | )
               | partitioned by(dt)
               | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
          // Insert data
          spark.sql(s"""insert into $tableName1 values(1, 'a1', 10, 1000, '2021-03-21')""")

          //
          // Update data with a value expression on preCombine field
          // 1) set source column name to be same as target column
          //
          spark.sql(
            s"""
               | merge into $tableName1 as t0
               | using (
               |  select 1 as id, 'a1' as name, 11 as price, 999 as v, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.id
               | when matched then update set id=s0.id, name=s0.name, price=s0.price*2, v=s0.v+2, dt=s0.dt
         """.stripMargin
          )
          // Update success as new value 1001 is bigger than original value 1000
          checkAnswer(s"select id,name,price,dt,v from $tableName1")(
            Seq(1, "a1", 22, "2021-03-21", 1001)
          )

          //
          // 2) set source column name to be different with target column
          //
          checkException(
            s"""
               | merge into $tableName1 as t0
               | using (
               |  select 1 as s_id, 'a1' as s_name, 12 as s_price, 1000 as s_v, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.s_id
               | when matched then update set id=s0.s_id, name=s0.s_name, price=s0.s_price*2, v=s0.s_v+2, dt=s0.dt
         """.stripMargin
          )(
            "MERGE INTO field resolution error: " +
              "Failed to resolve ordering fields `v` w/in the source-table output")

          spark.sql(
            s"""
               | merge into $tableName1 as t0
               | using (
               |  select 1 as s_id, 'a1' as s_name, 12 as s_price, 1000 as v, '2021-03-21' as dt
               | ) as s0
               | on t0.id = s0.s_id
               | when matched then update set id=s0.s_id, name=s0.s_name, price=s0.s_price*2, v=s0.v+2, dt=s0.dt
         """.stripMargin
          )
          // Update success as new value 1002 is bigger than original value 1001
          checkAnswer(s"select id,name,price,dt,v from $tableName1")(
            Seq(1, "a1", 24, "2021-03-21", 1002)
          )
        }
      }
    }
  }

  test("Test MergeInto with primaryKey expression") {
    withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
        val tableName1 = generateTableName
        spark.sql(
          s"""
             | create table $tableName1 (
             |  id int,
             |  name string,
             |  price double,
             |  v int,
             |  dt string
             | ) using hudi
             | tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  preCombineField = 'v'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}/$tableName1'
         """.stripMargin)
        // Insert data
        spark.sql(s"""insert into $tableName1 values(3, 'a3', 30, 3000, '2021-03-21')""")
        spark.sql(s"""insert into $tableName1 values(2, 'a2', 20, 2000, '2021-03-21')""")
        spark.sql(s"""insert into $tableName1 values(1, 'a1', 10, 1000, '2021-03-21')""")

        //
        // Delete data with a condition expression on primaryKey field
        // 1) set source column name to be same as target column
        //
        checkException(
          s"""merge into $tableName1 t0
             | using (
             |  select 1 as id, 'a1' as name, 15 as price, 1001 as v, '2021-03-21' as dt
             | ) s0
             | on t0.id = s0.id + 1
             | when matched then delete
       """.stripMargin)(
          "Only simple conditions of the form `t.id = s.id` are allowed on the primary-key "
            + "and partition path column. Found `t0.id = (s0.id + 1)`")

        spark.sql(
          s"""
             | merge into $tableName1 t0
             | using (
             |  select 2 as id, 'a2' as name, 20 as price, 2000 as v, '2021-03-21' as dt
             | ) s0
             | on t0.id = s0.id
             | when matched then delete
         """.stripMargin
        )
        checkAnswer(s"select id,name,price,v,dt from $tableName1 order by id")(
          Seq(1, "a1", 10, 1000, "2021-03-21"),
          Seq(3, "a3", 30, 3000, "2021-03-21")
        )

        //
        // 2.a) set source column name to be different with target column (should fail unable to match precombine field)
        //
        checkException(
          s"""merge into $tableName1 t0
             | using (
             |  select 3 as s_id, 'a3' as s_name, 30 as s_price, 3000 as s_v, '2021-03-21' as dt
             | ) s0
             | on t0.id = s0.s_id
             | when matched then delete
             |""".stripMargin)(
          "MERGE INTO field resolution error: "
            + "Failed to resolve ordering fields `v` w/in the source-table output")

        //
        // 2.b) set source column name to be different with target column
        //
        spark.sql(
          s"""
             | merge into $tableName1 t0
             | using (
             |  select 3 as s_id, 'a3' as s_name, 30 as s_price, 3000 as v, '2021-03-21' as dt
             | ) s0
             | on t0.id = s0.s_id
             | when matched then delete
         """.stripMargin
        )

        checkAnswer(s"select id,name,price,v,dt from $tableName1 order by id")(
          Seq(1, "a1", 10, 1000, "2021-03-21")
        )
      }
    }
  }

  test("Test MergeInto with combination of delete update insert") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
        val sourceTable = generateTableName
        val targetTable = generateTableName
        // Create source table
        spark.sql(
          s"""
             | create table $sourceTable (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             | ) using parquet
             | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
        spark.sql(s"insert into $sourceTable values(8, 's8', 80, 2008, '2021-03-21')")
        spark.sql(s"insert into $sourceTable values(9, 's9', 90, 2009, '2021-03-21')")
        spark.sql(s"insert into $sourceTable values(10, 's10', 100, 2010, '2021-03-21')")
        spark.sql(s"insert into $sourceTable values(11, 's11', 110, 2011, '2021-03-21')")
        spark.sql(s"insert into $sourceTable values(12, 's12', 120, 2012, '2021-03-21')")
        // Create target table
        spark.sql(
          s"""
             |create table $targetTable (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
             | partitioned by(dt)
             | location '${tmp.getCanonicalPath}/$targetTable'
       """.stripMargin)
        spark.sql(s"insert into $targetTable values(7, 'a7', 70, 1007, '2021-03-21')")
        spark.sql(s"insert into $targetTable values(8, 'a8', 80, 1008, '2021-03-21')")
        spark.sql(s"insert into $targetTable values(9, 'a9', 90, 1009, '2021-03-21')")
        spark.sql(s"insert into $targetTable values(10, 'a10', 100, 1010, '2021-03-21')")

        spark.sql(
          s"""
             | merge into $targetTable as t0
             | using $sourceTable as s0
             | on t0.id = s0.id
             | when matched and s0.id = 10 then delete
             | when matched and s0.id < 10 then update set id=s0.id, name='sxx', price=s0.price*2, ts=s0.ts+10000, dt=s0.dt
             | when not matched and s0.id > 10 then insert *
         """.stripMargin)
        checkAnswer(s"select id,name,price,ts,dt from $targetTable order by id")(
          Seq(7, "a7", 70, 1007, "2021-03-21"),
          Seq(8, "sxx", 160, 12008, "2021-03-21"),
          Seq(9, "sxx", 180, 12009, "2021-03-21"),
          Seq(11, "s11", 110, 2011, "2021-03-21"),
          Seq(12, "s12", 120, 2012, "2021-03-21")
        )
      }
    })
  }

  test("Merge Hudi to Hudi") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true",
        MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        Seq("cow", "mor").foreach { tableType =>
          val sourceTable = generateTableName
          spark.sql(
            s"""
               |create table $sourceTable (
               | id int,
               | name string,
               | price double,
               | _ts long
               |) using hudi
               |tblproperties(
               | type ='$tableType',
               | primaryKey = 'id',
               | preCombineField = '_ts'
               |)
               |location '${tmp.getCanonicalPath}/$sourceTable'
          """.stripMargin)

          val targetTable = generateTableName
          val targetBasePath = s"${tmp.getCanonicalPath}/$targetTable"
          spark.sql(
            s"""
               |create table $targetTable (
               | id int,
               | name string,
               | price double,
               | _ts long
               |) using hudi
               |tblproperties(
               | type ='$tableType',
               | primaryKey = 'id',
               | preCombineField = '_ts'
               |)
               |location '$targetBasePath'
          """.stripMargin)

          // First merge
          spark.sql(s"insert into $sourceTable values(1, 'a1', 10, 1000)")
          spark.sql(
            s"""
               |merge into $targetTable t0
               |using $sourceTable s0
               |on t0.id = s0.id
               |when not matched then insert *
          """.stripMargin)

          checkAnswer(s"select id, name, price, _ts from $targetTable")(
            Seq(1, "a1", 10, 1000)
          )
          val storage = HoodieTestUtils.getStorage(new StoragePath(targetBasePath))
          val firstCompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, targetBasePath)

          // Second merge
          spark.sql(s"update $sourceTable set price = 12, _ts = 1001 where id = 1")
          spark.sql(
            s"""
               |merge into $targetTable t0
               |using $sourceTable s0
               |on t0.id = s0.id
               |when matched and cast(s0._ts as string) > '1000' then update set *
           """.stripMargin)
          checkAnswer(s"select id, name, price, _ts from $targetTable")(
            Seq(1, "a1", 12, 1001)
          )
          val secondCompletionTime = DataSourceTestUtils.latestCommitCompletionTime(storage, targetBasePath)
          // Test incremental query
          val hudiIncDF1 = spark.read.format("org.apache.hudi")
            .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
            .option(DataSourceReadOptions.START_COMMIT.key, "000")
            .option(DataSourceReadOptions.END_COMMIT.key, firstCompletionTime)
            .load(targetBasePath)
          hudiIncDF1.createOrReplaceTempView("inc1")
          checkAnswer(s"select id, name, price, _ts from inc1")(
            Seq(1, "a1", 10, 1000)
          )
          // Third merge
          spark.sql(s"insert into $sourceTable values(2, 'a2', 10, 1001)")
          spark.sql(
            s"""
               |merge into $targetTable t0
               |using $sourceTable s0
               |on t0.id = s0.id
               |when matched then update set *
               |when not matched and s0.name = 'a2' then insert *
           """.stripMargin)
          checkAnswer(s"select id, name, price, _ts from $targetTable order by id")(
            Seq(1, "a1", 12, 1001),
            Seq(2, "a2", 10, 1001)
          )
          // Test incremental query
          val hudiIncDF2 = spark.read.format("org.apache.hudi")
            .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
            .option(DataSourceReadOptions.START_COMMIT.key, secondCompletionTime)
            .load(targetBasePath)
          hudiIncDF2.createOrReplaceTempView("inc2")
          checkAnswer(s"select id, name, price, _ts from inc2 order by id")(
            Seq(1, "a1", 12, 1001),
            Seq(2, "a2", 10, 1001)
          )
        }
      }
    })
  }

  test("Test Different Type of PreCombineField") {
    withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
        val typeAndValue = Seq(
          ("string", "'1000'"),
          ("int", 1000),
          ("bigint", 10000),
          ("timestamp", "'2021-05-20 00:00:00'"),
          ("date", "'2021-05-20'")
        )
        typeAndValue.foreach { case (dataType, dataValue) =>
          val tableName = generateTableName
          // Create table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  c $dataType
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  primaryKey ='id',
               |  preCombineField = 'c'
               | )
       """.stripMargin)

          // First merge with a extra input field 'flag' (insert a new record)
          spark.sql(
            s"""
               | merge into $tableName
               | using (
               |  select 1 as id, 'a1' as name, 10 as price, cast($dataValue as $dataType) as c, '1' as flag
               | ) s0
               | on s0.id = $tableName.id
               | when matched and flag = '1' then update set *
               | when not matched and flag = '1' then insert *
       """.stripMargin)
          checkAnswer(s"select id, name, price from $tableName")(
            Seq(1, "a1", 10.0)
          )
          spark.sql(
            s"""
               | merge into $tableName
               | using (
               |  select 1 as id, 'a1' as name, 10 as price, cast($dataValue as $dataType) as c
               | ) s0
               | on s0.id = $tableName.id
               | when matched then update set
               | id = s0.id, name = s0.name, price = s0.price + $tableName.price, c = s0.c
               | when not matched then insert *
       """.stripMargin)
          checkAnswer(s"select id, name, price from $tableName")(
            Seq(1, "a1", 20.0)
          )
        }
      }
    }
  }

  test("Test MergeInto For MOR With Compaction On") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true", MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts int
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey ='id',
             |  type = 'mor',
             |  preCombineField = 'ts',
             |  hoodie.compact.inline = 'true'
             | )
       """.stripMargin)
        val structFields = List(
          StructField("id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true),
          StructField("ts", IntegerType, nullable = true))

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1000),
          Seq(3, "a3", 10.0, 1000),
          Seq(4, "a4", 10.0, 1000)
        )

        spark.sql(
          s"""
             |merge into $tableName h0
             |using (
             | select 4 as _id, 'a4' as name, 11 as _price, 1000 as ts
             | ) s0
             | on h0.id = s0._id
             | when matched then update set
             |   id = s0._id, name = s0.name, price = s0._price, ts = s0.ts
             |""".stripMargin)

        validateTableSchema(tableName, structFields)
        // 5 commits will trigger compaction.
        checkAnswer(s"select id, name, price, ts from $tableName order by id")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 10.0, 1000),
          Seq(3, "a3", 10.0, 1000),
          Seq(4, "a4", 11.0, 1000)
        )
      }
    })
  }

  test("Test MereInto With Null Fields") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
        val types = Seq(
          "string",
          "int",
          "bigint",
          "double",
          "float",
          "timestamp",
          "date",
          "decimal"
        )
        types.foreach { dataType =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  value $dataType,
               |  ts int
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  primaryKey ='id',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)

          spark.sql(
            s"""
               |merge into $tableName h0
               |using (
               | select 1 as id, 'a1' as name, cast(null as $dataType) as value, 1000 as ts
               | ) s0
               | on h0.id = s0.id
               | when not matched then insert *
               |""".stripMargin)
          checkAnswer(s"select id, name, value, ts from $tableName")(
            Seq(1, "a1", null, 1000)
          )
        }
      }
    })
  }

  test("Test MergeInto With All Kinds Of DataType") {
    withRecordType()(withTempDir { tmp =>
      withSQLConf("hoodie.payload.combined.schema.validate" -> "true") {
        val dataAndTypes = Seq(
          ("string", "'a1'"),
          ("int", "10"),
          ("bigint", "10"),
          ("double", "10.0"),
          ("float", "10.0"),
          ("decimal(5,2)", "10.11"),
          ("decimal(5,0)", "10"),
          ("timestamp", "'2021-05-20 00:00:00'"),
          ("date", "'2021-05-20'")
        )
        dataAndTypes.foreach { case (dataType, dataValue) =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  value $dataType,
               |  ts int
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  primaryKey ='id',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)

          spark.sql(
            s"""
               |merge into $tableName h0
               |using (
               | select 1 as id, 'a1' as name, cast($dataValue as $dataType) as value, 1000 as ts
               | ) s0
               | on h0.id = s0.id
               | when not matched then insert *
               |""".stripMargin)
          checkAnswer(s"select id, name, cast(value as string), ts from $tableName")(
            Seq(1, "a1", extractRawValue(dataValue), 1000)
          )
        }
      }
    })
  }

  test("Test MergeInto with no-full fields source") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  value int,
           |  ts int
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           | select 1 as id, 1001 as ts
           | ) s0
           | on h0.id = s0.id
           | when matched then update set h0.ts = s0.ts
           |""".stripMargin)
      checkAnswer(s"select id, name, value, ts from $tableName")(
        Seq(1, "a1", 10, 1001)
      )
    })
  }

  test("Test partial insert with inline clustering") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      val tableType = "mor"
      val logDataBlockFormat = "parquet"
      withSparkSqlSessionConfig(
        HoodieWriteConfig.MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key -> "0",
        DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key -> "true",
        HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> logDataBlockFormat,
        HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "true",
        HoodieClusteringConfig.INLINE_CLUSTERING.key -> "true",
        HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key -> "2",
        HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key -> "id,price") {
        spark.sql(
          s"""
             | create table $tableName (
             |   id int,
             |   name string,
             |   price long,
             |   ts long,
             |   description string
             | ) using hudi
             | tblproperties(
             |   type ='$tableType',
             |   primaryKey = 'id',
             |   preCombineField = 'ts'
             | )
             | location '$basePath'
            """.stripMargin)
        spark.sql(s"insert into $tableName values " +
          "(1, 'a1', 10, 1000, 'a1: desc1')," +
          "(2, 'a2', 20, 1200, 'a2: desc2'), " +
          "(3, 'a3', 30.0, 1250, 'a3: desc3')")

        // Partial updates using MERGE INTO statement with changed fields: "price" and "ts"
        spark.sql(
          s"""
             | merge into $tableName t0
             | using (
             |   select 1 as id, 'a1' as name1, 12 as price, 1001 as _ts
             | union
             |   select 3 as id, 'a3' as name1, 25 as price, 1260 as _ts
             |   ) s0
             | on t0.id = s0.id
             | when matched then update set price = s0.price, ts = s0._ts
          """.stripMargin)
        checkAnswer(s"select id, name, price, ts, description from $tableName order by id")(
          Seq(1, "a1", 12, 1001, "a1: desc1"),
          Seq(2, "a2", 20, 1200, "a2: desc2"),
          Seq(3, "a3", 25, 1260, "a3: desc3")
        )
        assert(createMetaClient(spark, basePath).getActiveTimeline.lastInstant().get().getAction.equals(
          HoodieTimeline.REPLACE_COMMIT_ACTION))
      }
    })
  }

  test("Test Merge Into with target matched columns cast-ed") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  value int,
           |  ts int
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey ='id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      // Can remove redundant symmetrical casting on both sides (should succeed)
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (
           |  select cast(1 as int) as id, 1004 as ts
           | ) s0
           | on cast(h0.id as string) = cast(s0.id as string)
           | when matched then update set h0.ts = s0.ts
           |""".stripMargin)

      checkAnswer(s"select id, name, value, ts from $tableName")(
        Seq(1, "a1", 10, 1004)
      )
    })
  }

  test("Test MergeInto with partial insert") {
    Seq(true, false).foreach { sparkSqlOptimizedWrites =>
      withSparkSqlSessionConfig(
        s"${MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key}" -> "0",
        s"${SPARK_SQL_OPTIMIZED_WRITES.key()}" -> s"$sparkSqlOptimizedWrites"
      ) {
        withRecordType()(withTempDir { tmp =>
          // Create a partitioned mor table
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (
               |  id long,
               |  name string,
               |  price double,
               |  ts long,
               |  dt string
               | ) using hudi
               | tblproperties (
               |  type = 'mor',
               |  primaryKey = 'id',
               |  precombineField = 'ts'
               | )
               | partitioned by(dt)
               | location '${tmp.getCanonicalPath}'
           """.stripMargin)
          spark.sql(s"insert into $tableName select 1L, 'a1', 10, 1L, '2021-03-21'")

          spark.sql(
            s"""
               |MERGE INTO $tableName AS t0
               |USING (
               |  SELECT 2L as id, 'a2' as name, 10 as price, 1L as ts, '2021-03-20' as dt
               |  UNION
               |  SELECT 1L as id, 'a3' as name, 11 as price, 1L as ts, '2021-03-21' as dt
               |) s0
               |ON s0.id = t0.id
               |WHEN MATCHED THEN
               |  UPDATE SET price = s0.price, name = s0.name, ts = s0.ts
               |WHEN NOT MATCHED AND s0.id % 2 = 0 THEN
               |  INSERT (id, name, dt, ts)
               |  VALUES (s0.id, s0.name, s0.dt, s0.ts)
               |""".stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
            Seq(1L, "a3", 11, 1L, "2021-03-21"),
            Seq(2L, "a2", null, 1L, "2021-03-20")
          )
        })
      }
    }
  }

  test("Test MergeInto with partial update and insert") {
    withSQLConf(MERGE_SMALL_FILE_GROUP_CANDIDATES_LIMIT.key() -> "0") {


      // Test combinations: (tableType, sparkSqlOptimizedWrites)
      val testConfigs = Seq(
        // Uncomment once HUDI-8835 is fixed.
        // ("mor", true),
        // ("mor", false),
        ("cow", true),
        ("cow", false)
      )

      testConfigs.foreach { case (tableType, sparkSqlOptimizedWrites) =>
        log.info(s"=== Testing MergeInto with partial insert: tableType=$tableType, sparkSqlOptimizedWrites=$sparkSqlOptimizedWrites ===")
        withRecordType()(withTempDir { tmp =>
          withSparkSqlSessionConfig("hoodie.payload.combined.schema.validate" -> "true",
            SPARK_SQL_OPTIMIZED_WRITES.key() -> sparkSqlOptimizedWrites.toString) {
            // Create a partitioned table
            val tableName = generateTableName
            spark.sql(
              s"""
                 | create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  ts bigint,
                 |  dt string
                 | ) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by(dt)
                 | location '${tmp.getCanonicalPath}'
           """.stripMargin)

            spark.sql(s"insert into $tableName select 1, 'a1', 10, 1L, '2021-03-21'")

            spark.sql(
              s"""
                 | merge into $tableName as t0
                 | using (
                 |  select 2 as id, 'a2' as name, 10 as price, 2L as ts, '2021-03-20' as dt
                 |  union
                 |  select 1 as id, 'a1_updated' as name, 11 as price, 3L as ts, '2021-03-21' as dt
                 | ) s0
                 | on s0.id = t0.id
                 | when matched then update set t0.id = s0.id, t0.name = s0.name
                 | when not matched and s0.id % 2 = 0 then insert (id, name, dt)
                 | values(s0.id, s0.name, s0.dt)
           """.stripMargin)
            checkAnswer(s"select id, name, price, dt from $tableName order by id")(
              Seq(1, "a1_updated", 10, "2021-03-21"),
              Seq(2, "a2", null, "2021-03-20")
            )
          }
        })
      }
    }
  }

  test("Test unresolved columns in Merge Into statement") {
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
             | tblproperties (primaryKey = 'id', type = '$tableType')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             | """.stripMargin)

        spark.sql(
          s"""
             | insert into $tableName partition(dt = '2024-01-14')
             | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
             | union
             | select 2 as id, 'a2' as name, 20 as price, 1002 as ts
             | """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2024-01-14"),
          Seq(2, "a2", 20.0, 1002, "2024-01-14")
        )

        val targetTableFields = spark.sql(s"select * from $tableName").schema.fields
          .map(e => (e.name, tableName, s"spark_catalog.default.$tableName.${e.name}"))
        val sourceTableFields = Seq("s0._id", "s0._price", "s0._ts", "s0.dt", "s0.name")
          .map(e => {
            val splits = e.split('.')
            (splits(1), splits(0), e)
          })
        // Target table column cannot be found
        val sqlStatement1 =
          s"""
             | merge into $tableName
             | using (
             |  select 2 as _id, '2024-01-14' as dt, 'a2_new' as name, 25 as _price, 1005 as _ts
             |  union
             |  select 3 as _id, '2024-01-14' as dt, 'a3' as name, 30 as _price, 1003 as _ts
             | ) s0
             | on s0._id = $tableName.id
             | when matched then update set
             | id = s0._id, dt = s0.dt, new_col = s0.name, price = s0._price + $tableName.price,
             | ts = s0._ts
             | """.stripMargin
        // Source table column cannot be found
        val sqlStatement2 =
          s"""
             | merge into $tableName
             | using (
             |  select 2 as _id, '2024-01-14' as dt, 'a2_new' as name, 25 as _price, 1005 as _ts
             |  union
             |  select 3 as _id, '2024-01-14' as dt, 'a3' as name, 30 as _price, 1003 as _ts
             | ) s0
             | on s0._id = $tableName.id
             | when matched then update set
             | id = s0._id, dt = s0.dt, name = s0.new_col, price = s0._price + $tableName.price,
             | ts = s0._ts
             | """.stripMargin

        checkExceptionContain(sqlStatement1)(
          getExpectedUnresolvedColumnExceptionMessage("new_col", targetTableFields))
        checkExceptionContain(sqlStatement2)(
          getExpectedUnresolvedColumnExceptionMessage("s0.new_col", sourceTableFields ++ targetTableFields))
      }
    }
  }

  test("Test no schema evolution in MERGE INTO") {
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
             | tblproperties (primaryKey = 'id', type = '$tableType')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             | """.stripMargin)
        val structFields = List(
          StructField("id", IntegerType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true),
          StructField("ts", LongType, nullable = true),
          StructField("dt", StringType, nullable = true))

        spark.sql(
          s"""
             | insert into $tableName partition(dt = '2024-01-14')
             | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
             | union
             | select 2 as id, 'a2' as name, 20 as price, 1002 as ts
             | """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2024-01-14"),
          Seq(2, "a2", 20.0, 1002, "2024-01-14")
        )

        // MERGE INTO with an additional column that does not exist in the table schema
        // throws an error if it tries to set the new column in the "UPDATE SET" clause
        val sqlStatement1 =
          s"""
             | merge into $tableName
             | using (
             |  select 2 as _id, '2024-01-14' as dt, 'a2_new' as name, 25 as _price, 1005 as _ts, 'x' as new_col
             |  union
             |  select 3 as _id, '2024-01-14' as dt, 'a3' as name, 30 as _price, 1003 as _ts, 'y' as new_col
             | ) s0
             | on s0._id = $tableName.id
             | when matched then update set
             | id = s0._id, dt = s0.dt, name = s0.name, price = s0._price + $tableName.price,
             | ts = s0._ts, new_col = s0.new_col
             | """.stripMargin
        checkExceptionContain(sqlStatement1)(
          getExpectedUnresolvedColumnExceptionMessage("new_col", tableName))
        validateTableSchema(tableName, structFields)

        // MERGE INTO with an additional column that does not exist in the table schema
        // works only if it tries to do inserts (the additional column is dropped before inserts)
        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 3 as id, '2024-01-14' as dt, 'a3' as name, 30 as price, 1003 as ts, 'y' as new_col
             | ) s0
             | on s0.id = $tableName.id
             | when not matched then insert *
             | """.stripMargin)
        validateTableSchema(tableName, structFields)
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2024-01-14"),
          Seq(2, "a2", 20.0, 1002, "2024-01-14"),
          Seq(3, "a3", 30.0, 1003, "2024-01-14"))
      }
    }
  }

  test("Test partition not at end") {
    withSQLConf("hoodie.memory.merge.max.size" -> "1",
      "hoodie.memory.compaction.max.size" -> "1",
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      "hoodie.bulkinsert.shuffle.parallelism" -> "1") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  partition string,
             |  name string,
             |  price double,
             |  ts int
             |) using hudi
             | partitioned by (partition)
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey ='id',
             |  preCombineField = 'ts'
             | )
       """.stripMargin)

        spark.sql(
          s"""
             |insert into $tableName values
             |(1, 'a1', 1, 100, 'p1'),
             |(11, 'a1', 1, 100, 'p1'),
             |(12, 'a1', 1, 100, 'p1'),
             |(13, 'a1', 1, 100, 'p1'),
             |(2, 'a2', 2, 200, 'p2'),
             |(21, 'a2', 2, 200, 'p2'),
             |(22, 'a2', 2, 200, 'p2'),
             |(23, 'a2', 2, 200, 'p2')
             |""".stripMargin
        )

        spark.sql(
          s"""
             | merge into $tableName
             | using (
             |  select 1 as id, 'p1' as partition, 'a1' as name, 10 as price, 1000 as ts
             |  union
             |  select 11 as id, 'p1' as partition, 'a1' as name, 10 as price, 1000 as ts
             |  union
             |  select 12 as id, 'p1' as partition, 'a1' as name, 10 as price, 1000 as ts
             |  union
             |  select 13 as id, 'p1' as partition, 'a1' as name, 10 as price, 1000 as ts
             |  union
             |  select 2 as id, 'p2' as partition, 'a2' as name, 20 as price, 1000 as ts
             |  union
             |  select 21 as id, 'p2' as partition, 'a2' as name, 20 as price, 1000 as ts
             |  union
             |  select 22 as id, 'p2' as partition, 'a2' as name, 20 as price, 1000 as ts
             |  union
             |  select 23 as id, 'p2' as partition, 'a2' as name, 20 as price, 1000 as ts
             |  union
             |  select 3 as id, 'p3' as partition, 'a3' as name, 30 as price, 1000 as ts
             |  union
             |  select 4 as id, 'p1' as partition, 'a4' as name, 40 as price, 1000 as ts
             | ) s0
             | on s0.id = $tableName.id and s0.partition = $tableName.partition
             | when matched then update set *
             | when not matched then insert *
       """.stripMargin)
      }
    }
  }

  test("Test MergeInto with non existent target table") {
    withTempDir { tmp =>
      val sourceTable = generateTableName
      spark.sql(
        s"""
           | create table $sourceTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts int
           | ) using parquet
           | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
      spark.sql(s"insert into $sourceTable values(1, 'a1', 10, 1000)")
      val nonExistentTable = "hudi_test_table"
      val exception = intercept[org.apache.spark.sql.AnalysisException] {
        spark.sql(
          s"""
             | MERGE INTO $nonExistentTable AS target
             | USING $sourceTable AS source
             | ON target.id = source.id
             | WHEN MATCHED THEN UPDATE SET
             |   name = source.name,
             |   ts = source.ts
             | WHEN NOT MATCHED THEN INSERT *
         """.stripMargin)
      }
      assert(exception.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") ||
        exception.getMessage.contains("Table or view not found"),
        s"Expected TABLE_OR_VIEW_NOT_FOUND error but got: ${exception.getMessage}")
    }
  }
}
