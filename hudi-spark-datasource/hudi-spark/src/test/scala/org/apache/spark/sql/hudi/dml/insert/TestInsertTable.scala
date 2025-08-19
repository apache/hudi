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

import org.apache.hudi.{DataSourceWriteOptions, HoodieCLIUtils, HoodieSparkUtils}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.WriteClientTestUtils
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieDuplicateKeyException, HoodieException}
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.{disableComplexKeygenValidation, enableComplexKeygenValidation, getLastCommitMetadata}
import org.junit.jupiter.api.Assertions.assertEquals

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

class TestInsertTable extends HoodieSparkSqlTestBase {

  test("Test Insert Into with subset of columns") {
    // This is only supported by Spark 3.5
    if (HoodieSparkUtils.gteqSpark3_5) {
      Seq("cow", "mor").foreach(tableType =>
        Seq(true, false).foreach(isPartitioned => withTempDir { tmp =>
          testInsertIntoWithSubsetOfColumns(
            "hudi", tableType, s"${tmp.getCanonicalPath}/hudi_table", isPartitioned)
        }))
    }
  }

  test("Test Insert Into with subset of columns on Parquet table") {
    // This is only supported by Spark 3.5
    if (HoodieSparkUtils.gteqSpark3_5) {
      // Make sure parquet tables are not affected by the custom rules for
      // INSERT INTO statements on Hudi tables
      Seq(true, false).foreach(isPartitioned => withTempDir { tmp =>
        testInsertIntoWithSubsetOfColumns(
          "parquet", "", s"${tmp.getCanonicalPath}/parquet_table", isPartitioned)
      })
    }
  }

  private def testInsertIntoWithSubsetOfColumns(format: String,
                                                tableType: String,
                                                tablePath: String,
                                                isPartitioned: Boolean): Unit = {
    val tableName = generateTableName
    val createTablePartitionClause = if (isPartitioned) "partitioned by (dt)" else ""
    // Create a partitioned table
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  dt string,
         |  name string,
         |  price double,
         |  ts long
         |) using $format
         | tblproperties (
         | type = '$tableType',
         | primaryKey = 'id'
         | )
         | $createTablePartitionClause
         | location '$tablePath'
       """.stripMargin)

    // INSERT INTO with all columns
    // Same ordering of columns as the schema
    spark.sql(
      s"""
         | insert into $tableName (id, name, price, ts, dt)
         | values (1, 'a1', 10, 1000, '2025-01-01'),
         | (2, 'a2', 20, 2000, '2025-01-02')
        """.stripMargin)
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(1, "a1", 10.0, 1000, "2025-01-01"),
      Seq(2, "a2", 20.0, 2000, "2025-01-02")
    )

    // Different ordering of columns compared to the schema
    spark.sql(
      s"""
         | insert into $tableName (dt, name, id, price, ts)
         | values ('2025-01-03', 'a3', 3, 30, 3000)
        """.stripMargin)
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(1, "a1", 10.0, 1000, "2025-01-01"),
      Seq(2, "a2", 20.0, 2000, "2025-01-02"),
      Seq(3, "a3", 30.0, 3000, "2025-01-03")
    )

    // INSERT INTO with a subset of columns
    // Using different ordering of subset of columns in user-specified columns,
    // and VALUES without column names
    spark.sql(
      s"""
         | insert into $tableName (dt, ts, name, id)
         | values ('2025-01-04', 4000, 'a4', 4)
        """.stripMargin)
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(1, "a1", 10.0, 1000, "2025-01-01"),
      Seq(2, "a2", 20.0, 2000, "2025-01-02"),
      Seq(3, "a3", 30.0, 3000, "2025-01-03"),
      Seq(4, "a4", null, 4000, "2025-01-04")
    )

    spark.sql(
      s"""
         | insert into $tableName (id, price, ts, dt)
         | values (5, 50.0, 5000, '2025-01-05')
        """.stripMargin)
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(1, "a1", 10.0, 1000, "2025-01-01"),
      Seq(2, "a2", 20.0, 2000, "2025-01-02"),
      Seq(3, "a3", 30.0, 3000, "2025-01-03"),
      Seq(4, "a4", null, 4000, "2025-01-04"),
      Seq(5, null, 50.0, 5000, "2025-01-05")
    )

    // Using a subset of columns in user-specified columns, and VALUES with column names
    spark.sql(
      s"""
         | insert into $tableName (dt, ts, id, name)
         | values ('2025-01-06' as dt, 6000 as ts, 6 as id, 'a6' as name)
        """.stripMargin)
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(1, "a1", 10.0, 1000, "2025-01-01"),
      Seq(2, "a2", 20.0, 2000, "2025-01-02"),
      Seq(3, "a3", 30.0, 3000, "2025-01-03"),
      Seq(4, "a4", null, 4000, "2025-01-04"),
      Seq(5, null, 50.0, 5000, "2025-01-05"),
      Seq(6, "a6", null, 6000, "2025-01-06")
    )

    if (isPartitioned) {
      spark.sql(
        s"""
           | insert into $tableName partition(dt='2025-01-07') (ts, id, name)
           | values (7000, 7, 'a7')
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2025-01-01"),
        Seq(2, "a2", 20.0, 2000, "2025-01-02"),
        Seq(3, "a3", 30.0, 3000, "2025-01-03"),
        Seq(4, "a4", null, 4000, "2025-01-04"),
        Seq(5, null, 50.0, 5000, "2025-01-05"),
        Seq(6, "a6", null, 6000, "2025-01-06"),
        Seq(7, "a7", null, 7000, "2025-01-07")
      )
    }
  }

  test("Test table type name incase-sensitive test") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      spark.sql(
        s"""
           |create table ${targetTable} (
           |  `id` string,
           |  `name` string,
           |  `dt` bigint,
           |  `day` STRING,
           |  `hour` INT
           |) using hudi
           |tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'MOR',
           |  'preCombineField'='dt',
           |  'hoodie.index.type' = 'BUCKET',
           |  'hoodie.bucket.index.hash.field' = 'id',
           |  'hoodie.bucket.index.num.buckets'=512
           | )
             partitioned by (`day`,`hour`)
             location '${tablePath}'
             """.stripMargin)

      spark.sql("set spark.sql.shuffle.partitions = 11")

      disableComplexKeygenValidation(spark, targetTable)
      spark.sql(
        s"""
           |insert into ${targetTable}
           |select '1' as id, 'aa' as name, 123 as dt, '2024-02-19' as `day`, 10 as `hour`
           |""".stripMargin)

      spark.sql(
        s"""
           |merge into ${targetTable} as target
           |using (
           |select '2' as id, 'bb' as name, 456L as dt, '2024-02-19' as `day`, 10 as `hour`
           |) as source
           |on target.id = source.id
           |when matched then update set *
           |when not matched then insert *
           |""".stripMargin
      )
      enableComplexKeygenValidation(spark, targetTable)
      // check result after insert and merge data into target table
      checkAnswer(s"select id, name, dt, day, hour from $targetTable limit 10")(
        Seq("1", "aa", 123, "2024-02-19", 10),
        Seq("2", "bb", 456, "2024-02-19", 10)
      )
    }
  }

  test("Test Insert Into with values") {
    withRecordType()(withTempDir { tmp =>
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
           | tblproperties (primaryKey = 'id')
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

      checkAnswer(s"select id, name, price, ts, dt from $tableName where year(dt) > '2020' and lower(name) > 'a0'")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05"),
        Seq(2, "a2", 20.0, 2000, "2021-01-06"),
        Seq(3, "a3", 30.0, 3000, "2021-01-07")
      )
    })
  }

  test("Test Insert Into with static partition") {
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
       """.stripMargin)
        // Insert into static partition
        spark.sql(
          s"""
             | insert into $tableName partition(dt = '2021-01-05')
             | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
              """.stripMargin)

        spark.sql(
          s"""
             | insert into $tableName partition(dt = '2021-01-06')
             | select 20 as price, 2000 as ts, 2 as id, 'a2' as name
              """.stripMargin)
        // should not mess with the original order after write the out-of-order data.
        val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
        val schema = HoodieSqlCommonUtils.getTableSqlSchema(metaClient).get
        assert(schema.getFieldIndex("id").contains(0))
        assert(schema.getFieldIndex("price").contains(2))

        // Note: Do not write the field alias, the partition field must be placed last.
        spark.sql(
          s"""
             | insert into $tableName
             | select 3, 'a3', 30, 3000, '2021-01-07'
        """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )
      }
    }
  }

  test("Test Insert Into with dynamic partition") {
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
       """.stripMargin)

        // Insert into dynamic partition
        spark.sql(
          s"""
             | insert into $tableName partition(dt)
             | select 1 as id, '2021-01-05' as dt, 'a1' as name, 10 as price, 1000 as ts
        """.stripMargin)
        // should not mess with the original order after write the out-of-order data.
        val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
        val schema = HoodieSqlCommonUtils.getTableSqlSchema(metaClient).get
        assert(schema.getFieldIndex("id").contains(0))
        assert(schema.getFieldIndex("price").contains(2))

        spark.sql(
          s"""
             | insert into $tableName
             | select 2 as id, 'a2' as name, 20 as price, 2000 as ts, '2021-01-06' as dt
        """.stripMargin)

        // Note: Do not write the field alias, the partition field must be placed last.
        spark.sql(
          s"""
             | insert into $tableName
             | select 3, 'a3', 30, 3000, '2021-01-07'
        """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05"),
          Seq(2, "a2", 20.0, 2000, "2021-01-06"),
          Seq(3, "a3", 30.0, 3000, "2021-01-07")
        )
      }
    }
  }

  test("Test Insert Into with multi partition") {
    Seq("cow", "mor").foreach { tableType =>
      withRecordType()(withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  dt string,
             |  name string,
             |  price double,
             |  ht string,
             |  ts long
             |) using hudi
             | tblproperties (primaryKey = 'id', type = '$tableType')
             | partitioned by (dt, ht)
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
        disableComplexKeygenValidation(spark, tableName)
        spark.sql(
          s"""
             | insert into $tableName partition(dt, ht)
             | select 1 as id, 'a1' as name, 10 as price,'20210101' as dt, 1000 as ts, '01' as ht
              """.stripMargin)

        // Insert into static partition and dynamic partition
        spark.sql(
          s"""
             | insert into $tableName partition(dt = '20210102', ht)
             | select 2 as id, 'a2' as name, 20 as price, 2000 as ts, '02' as ht
              """.stripMargin)

        spark.sql(
          s"""
             | insert into $tableName partition(dt, ht = '03')
             | select 3 as id, 'a3' as name, 30 as price, 3000 as ts, '20210103' as dt
              """.stripMargin)

        // Note: Do not write the field alias, the partition field must be placed last.
        spark.sql(
          s"""
             | insert into $tableName
             | select 4, 'a4', 40, 4000, '20210104', '04'
        """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt, ht from $tableName")(
          Seq(1, "a1", 10.0, 1000, "20210101", "01"),
          Seq(2, "a2", 20.0, 2000, "20210102", "02"),
          Seq(3, "a3", 30.0, 3000, "20210103", "03"),
          Seq(4, "a4", 40.0, 4000, "20210104", "04")
        )
      })
    }
  }

  test("Test Insert Into Non Partitioned Table") {
    withRecordType(Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK))(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(s"set hoodie.datasource.insert.dup.policy=fail")
      // Create none partitioned cow table
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
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
          """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10.0, 1000)
      )
      spark.sql(s"insert into $tableName select 2, 'a2', 12, 1000")
      checkAnswer(s"select id, name, price, ts from $tableName")(
        Seq(1, "a1", 10.0, 1000),
        Seq(2, "a2", 12.0, 1000)
      )

      assertThrows[HoodieDuplicateKeyException] {
        try {
          spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        } catch {
          case e: Exception =>
            var root: Throwable = e
            while (root.getCause != null) {
              root = root.getCause
            }
            throw root
        }
      }

      // Create table with dropDup is true
      val tableName2 = generateTableName
      spark.sql("set hoodie.datasource.insert.dup.policy=drop")
      spark.sql(
        s"""
           |create table $tableName2 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName2'
           | tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
          """.stripMargin)
      spark.sql(s"insert into $tableName2 select 1, 'a1', 10, 1000")
      // This record will be drop when dropDup is true
      spark.sql(s"insert into $tableName2 select 1, 'a1', 12, 1000")
      checkAnswer(s"select id, name, price, ts from $tableName2")(
        Seq(1, "a1", 10.0, 1000)
      )
      // disable this config to avoid affect other test in this class.
      spark.sql(s"set hoodie.sql.insert.mode=upsert")
    })
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
  }

  test("Test Insert Into None Partitioned Table strict mode with no preCombineField") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(s"set hoodie.datasource.insert.dup.policy=fail")
      // Create none partitioned cow table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  type = 'cow',
           |  primaryKey = 'id'
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10)")
      checkAnswer(s"select id, name, price from $tableName")(
        Seq(1, "a1", 10.0)
      )
      spark.sql(s"insert into $tableName select 2, 'a2', 12")
      checkAnswer(s"select id, name, price from $tableName")(
        Seq(1, "a1", 10.0),
        Seq(2, "a2", 12.0)
      )
      spark.sql("set hoodie.merge.allow.duplicate.on.inserts = false")
      assertThrows[HoodieDuplicateKeyException] {
        try {
          spark.sql(s"insert into $tableName select 1, 'a1', 10")
        } catch {
          case e: Exception =>
            var root: Throwable = e
            while (root.getCause != null) {
              root = root.getCause
            }
            throw root
        }
      }

      // disable this config to avoid affect other test in this class.
      spark.sql(s"set hoodie.sql.insert.mode=upsert")
    }
  }

  test("Test Insert Overwrite") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { tableName =>
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
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id'
               | )
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}/$tableName'
          """.stripMargin)

          //  Insert into table
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,'a1',10,1000,'2021-01-05'),
               | (2,'a2',10,1000,'2021-01-06')
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05"),
            Seq(2, "a2", 10.0, 1000, "2021-01-06")
          )


          // First respect hoodie.datasource.write.operation, if not set then respect hoodie.datasource.overwrite.mode,
          // If the previous two config both not set, then respect spark.sql.sources.partitionOverwriteMode
          spark.sql(
            s"""
               | insert overwrite table $tableName values
               | (3,'a3',10,1000,'2021-01-06'),
               | (4,'a4',10,1000,'2021-01-07')
          """.stripMargin)
          // As hoodie.datasource.write.operation and hoodie.datasource.overwrite.mode both not set, respect
          // spark.sql.sources.partitionOverwriteMode and it's default behavior is static,so insert overwrite whole table
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
            Seq(3, "a3", 10.0, 1000, "2021-01-06"),
            Seq(4, "a4", 10.0, 1000, "2021-01-07")
          )

          spark.sql(s"set spark.sql.sources.partitionOverwriteMode=dynamic")
          spark.sql(
            s"""
               | insert overwrite table $tableName values
               | (5,'a5',10,1000,'2021-01-07')
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
            Seq(3, "a3", 10.0, 1000, "2021-01-06"),
            Seq(5, "a5", 10.0, 1000, "2021-01-07")
          )

          // Insert overwrite partitioned table with the PARTITION clause will always insert overwrite the specific
          // partition regardless of static or dynamic mode
          spark.sql(
            s"""
               | insert overwrite table $tableName partition(dt = '2021-01-06')
               | select * from (select 6 , 'a6', 10, 1000) limit 10
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
            Seq(6, "a6", 10.0, 1000, "2021-01-06"),
            Seq(5, "a5", 10.0, 1000, "2021-01-07")
          )

          spark.sql(s"set spark.sql.sources.partitionOverwriteMode=static")
          spark.sql(s"set hoodie.datasource.overwrite.mode=dynamic")
          spark.sql(
            s"""
               | insert overwrite table $tableName values
               | (7,'a7',10,1000,'2021-01-07')
          """.stripMargin)
          // Config hoodie.datasource.overwrite.mode takes precedence over spark.sql.sources.partitionOverwriteMode
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
            Seq(6, "a6", 10.0, 1000, "2021-01-06"),
            Seq(7, "a7", 10.0, 1000, "2021-01-07")
          )

          spark.sql(s"set hoodie.datasource.overwrite.mode=static")
          spark.sql(
            s"""
               | insert overwrite table $tableName values
               | (8,'a8',10,1000,'2021-01-07'),
               | (9,'a9',10,1000,'2021-01-08')
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
            Seq(8, "a8", 10.0, 1000, "2021-01-07"),
            Seq(9, "a9", 10.0, 1000, "2021-01-08")
          )

          // Config hoodie.datasource.write.operation always takes precedence over other configs
          spark.sql("set hoodie.datasource.write.operation = insert_overwrite")
          spark.sql(
            s"""
               | insert overwrite table $tableName values
               | (10,'a10',10,1000,'2021-01-08')
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
            Seq(8, "a8", 10.0, 1000, "2021-01-07"),
            Seq(10, "a10", 10.0, 1000, "2021-01-08")
          )

          spark.sql("set hoodie.datasource.write.operation = insert_overwrite_table")
          spark.sql(
            s"""
               | insert overwrite table $tableName values
               | (11,'a11',10,1000,'2021-01-08'),
               | (12,'a12',10,1000,'2021-01-09')
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
            Seq(11, "a11", 10.0, 1000, "2021-01-08"),
            Seq(12, "a12", 10.0, 1000, "2021-01-09")
          )

          spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
          spark.sessionState.conf.unsetConf("hoodie.datasource.overwrite.mode")
          spark.sessionState.conf.unsetConf("spark.sql.sources.partitionOverwriteMode")

          // Test insert overwrite non-partitioned table (non-partitioned table always insert overwrite the whole table)
          val tblNonPartition = generateTableName
          spark.sql(
            s"""
               | create table $tblNonPartition (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               | ) using hudi
               | tblproperties (primaryKey = 'id')
               | location '${tmp.getCanonicalPath}/$tblNonPartition'
          """.stripMargin)
          spark.sql(s"insert into $tblNonPartition select 1, 'a1', 10, 1000")
          spark.sql(s"insert overwrite table $tblNonPartition select 2, 'a2', 10, 1000")
          checkAnswer(s"select id, name, price, ts from $tblNonPartition")(
            Seq(2, "a2", 10.0, 1000)
          )
        }
      }
    }
  }

  test("Test insert overwrite for multi partitioned table") {
    withRecordType()(Seq("cow", "mor").foreach { tableType =>
      Seq("dynamic", "static").foreach { overwriteMode =>
        withTable(generateTableName) { tableName =>
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  dt string,
               |  hh string
               |) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id'
               | )
               | partitioned by (dt, hh)
          """.stripMargin
          )
          disableComplexKeygenValidation(spark, tableName)
          spark.sql(
            s"""
               | insert into table $tableName values
               | (0, 'a0', 10, 1000, '2023-12-05', '00'),
               | (1, 'a1', 10, 1000, '2023-12-06', '00'),
               | (2, 'a2', 10, 1000, '2023-12-06', '01')
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt, hh from $tableName")(
            Seq(0, "a0", 10.0, 1000, "2023-12-05", "00"),
            Seq(1, "a1", 10.0, 1000, "2023-12-06", "00"),
            Seq(2, "a2", 10.0, 1000, "2023-12-06", "01")
          )

          withSQLConf("hoodie.datasource.overwrite.mode" -> overwriteMode) {
            // test insert overwrite partitions with partial partition values
            spark.sql(
              s"""
                 | insert overwrite table $tableName partition (dt='2023-12-06', hh) values
                 | (3, 'a3', 10, 1000, '00'),
                 | (4, 'a4', 10, 1000, '02')
            """.stripMargin)
            val expected = if (overwriteMode.equalsIgnoreCase("dynamic")) {
              Seq(
                Seq(0, "a0", 10.0, 1000, "2023-12-05", "00"),
                Seq(3, "a3", 10.0, 1000, "2023-12-06", "00"),
                Seq(2, "a2", 10.0, 1000, "2023-12-06", "01"),
                Seq(4, "a4", 10.0, 1000, "2023-12-06", "02")
              )
            } else {
              Seq(
                Seq(0, "a0", 10.0, 1000, "2023-12-05", "00"),
                Seq(3, "a3", 10.0, 1000, "2023-12-06", "00"),
                Seq(4, "a4", 10.0, 1000, "2023-12-06", "02")
              )
            }
            checkAnswer(s"select id, name, price, ts, dt, hh from $tableName")(expected: _*)

            // test insert overwrite without partition values
            spark.sql(
              s"""
                 | insert overwrite table $tableName values
                 | (5, 'a5', 10, 1000, '2023-12-06', '02')
            """.stripMargin)
            val expected2 = if (overwriteMode.equalsIgnoreCase("dynamic")) {
              // dynamic mode only overwrite the matching partitions
              Seq(
                Seq(0, "a0", 10.0, 1000, "2023-12-05", "00"),
                Seq(3, "a3", 10.0, 1000, "2023-12-06", "00"),
                Seq(2, "a2", 10.0, 1000, "2023-12-06", "01"),
                Seq(5, "a5", 10.0, 1000, "2023-12-06", "02")
              )
            } else {
              // static mode will overwrite the table
              Seq(
                Seq(5, "a5", 10.0, 1000, "2023-12-06", "02")
              )
            }
            checkAnswer(s"select id, name, price, ts, dt, hh from $tableName")(expected2: _*)

            // test insert overwrite table
            withSQLConf("hoodie.datasource.write.operation" -> "insert_overwrite_table") {
              spark.sql(
                s"""
                   | insert overwrite table $tableName partition (dt='2023-12-06', hh) values
                   | (6, 'a6', 10, 1000, '00')
              """.stripMargin)
              checkAnswer(s"select id, name, price, ts, dt, hh from $tableName")(
                Seq(6, "a6", 10.0, 1000, "2023-12-06", "00")
              )
            }
          }
        }
      }
    })
  }

  test("Test Different Type of Partition Column") {
    withTempDir { tmp =>
      val typeAndValue = Seq(
        ("string", "'1000'"),
        ("int", 1000),
        ("bigint", 10000),
        ("timestamp", "TIMESTAMP'2021-05-20 00:00:00'"),
        ("date", "DATE'2021-05-20'")
      )
      typeAndValue.foreach { case (partitionType, partitionValue) =>
        val tableName = generateTableName
        validateDifferentTypesOfPartitionColumn(tmp, partitionType, partitionValue, tableName)
      }
    }
  }

  test("Test TimestampType Partition Column With Consistent Logical Timestamp Enabled") {
    withRecordType()(withTempDir { tmp =>
      val typeAndValue = Seq(
        ("timestamp", "TIMESTAMP'2021-05-20 00:00:00'"),
        ("date", "DATE'2021-05-20'")
      )
      typeAndValue.foreach { case (partitionType, partitionValue) =>
        val tableName = generateTableName
        spark.sql(s"set hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled=true")
        validateDifferentTypesOfPartitionColumn(tmp, partitionType, partitionValue, tableName)
      }
    })
  }

  test("Test insert for uppercase table name") {
    withTempDir { tmp =>
      val tableName = s"H_$generateTableName"
      if (HoodieSparkUtils.gteqSpark3_5) {
        // [SPARK-44284] Spark 3.5+ requires conf below to be case sensitive
        spark.sql(s"set spark.sql.caseSensitive=true")
      }

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double
           |) using hudi
           | tblproperties (primaryKey = 'id')
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10)")
      checkAnswer(s"select id, name, price from $tableName")(
        Seq(1, "a1", 10.0)
      )
      val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
      assertResult(tableName)(metaClient.getTableConfig.getTableName)
    }
  }

  test("Test Insert Exception") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (primaryKey = 'id')
         | partitioned by (dt)
     """.stripMargin)
    val tooManyDataColumnsErrorMsg = if (HoodieSparkUtils.gteqSpark3_5) {
      s"""
         |[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to `spark_catalog`.`default`.`$tableName`, the reason is too many data columns:
         |Table columns: `id`, `name`, `price`.
         |Data columns: `1`, `a1`, `10`, `2021-06-20`.
         |""".stripMargin
    } else if (HoodieSparkUtils.gteqSpark3_4) {
      """
        |too many data columns:
        |Table columns: 'id', 'name', 'price'.
        |Data columns: '1', 'a1', '10', '2021-06-20'.
        |""".stripMargin
    } else {
      """
        |too many data columns:
        |Table columns: 'id', 'name', 'price'
        |Data columns: '1', 'a1', '10', '2021-06-20'
        |""".stripMargin
    }
    checkExceptionContain(s"insert into $tableName partition(dt = '2021-06-20') select 1, 'a1', 10, '2021-06-20'")(
      tooManyDataColumnsErrorMsg)

    val notEnoughDataColumnsErrorMsg = if (HoodieSparkUtils.gteqSpark3_5) {
      s"""
         |[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot write to `spark_catalog`.`default`.`$tableName`, the reason is not enough data columns:
         |Table columns: `id`, `name`, `price`, `dt`.
         |Data columns: `1`, `a1`, `10`.
         |""".stripMargin
    } else if (HoodieSparkUtils.gteqSpark3_4) {
      """
        |not enough data columns:
        |Table columns: 'id', 'name', 'price', 'dt'.
        |Data columns: '1', 'a1', '10'.
        |""".stripMargin
    } else {
      """
        |not enough data columns:
        |Table columns: 'id', 'name', 'price', 'dt'
        |Data columns: '1', 'a1', '10'
        |""".stripMargin
    }
    checkExceptionContain(s"insert into $tableName select 1, 'a1', 10")(notEnoughDataColumnsErrorMsg)
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "true", "hoodie.sql.insert.mode" -> "strict") {
      val tableName2 = generateTableName
      spark.sql(
        s"""
           |create table $tableName2 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |   primaryKey = 'id',
           |   preCombineField = 'ts'
           | )
        """.stripMargin)
      checkException(s"insert into $tableName2 values(1, 'a1', 10, 1000)")(
        "Table with primaryKey can not use bulk insert in strict mode."
      )
    }
  }


  test("Test Insert timestamp when 'spark.sql.datetime.java8API.enabled' enabled") {
    withRecordType() {
      withSQLConf("spark.sql.datetime.java8API.enabled" -> "true") {
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt timestamp
             |)
             |using hudi
             |partitioned by(dt)
             |options(type = 'cow', primaryKey = 'id')
             |""".stripMargin
        )

        spark.sql(s"insert into $tableName values (1, 'a1', 10, cast('2021-05-07 00:00:00' as timestamp))")
        checkAnswer(s"select id, name, price, cast(dt as string) from $tableName")(
          Seq(1, "a1", 10, "2021-05-07 00:00:00")
        )
      }
    }
  }

  test("Test bulk insert with insert into for single partitioned table") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { tableName =>
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
            spark.sql("set hoodie.datasource.write.insert.drop.duplicates = false")

            // Enable the bulk insert
            spark.sql("set hoodie.sql.bulk.insert.enable = true")
            spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

            assertResult(WriteOperationType.BULK_INSERT) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
            }
            checkAnswer(s"select id, name, price, dt from $tableName")(
              Seq(1, "a1", 10.0, "2021-07-18")
            )

            // Disable the bulk insert
            spark.sql("set hoodie.sql.bulk.insert.enable = false")
            spark.sql(s"insert into $tableName values(2, 'a2', 10, '2021-07-18')")

            assertResult(WriteOperationType.INSERT) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
            }
            checkAnswer(s"select id, name, price, dt from $tableName order by id")(
              Seq(1, "a1", 10.0, "2021-07-18"),
              Seq(2, "a2", 10.0, "2021-07-18")
            )
          }
        }
      }
    }
  }

  test("Test bulk insert with insert into for multi partitioned table") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict") {
      withRecordType()(withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { tableMultiPartition =>
            spark.sql(
              s"""
                 |create table $tableMultiPartition (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string,
                 |  hh string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt, hh)
                 | location '${tmp.getCanonicalPath}/$tableMultiPartition'
         """.stripMargin)
            disableComplexKeygenValidation(spark, tableMultiPartition)
            // Enable the bulk insert
            spark.sql("set hoodie.sql.bulk.insert.enable = true")
            spark.sql(s"insert into $tableMultiPartition values(1, 'a1', 10, '2021-07-18', '12')")

            checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition")(
              Seq(1, "a1", 10.0, "2021-07-18", "12")
            )
            assertResult(WriteOperationType.BULK_INSERT) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableMultiPartition").getOperationType
            }
            // Disable the bulk insert
            spark.sql("set hoodie.sql.bulk.insert.enable = false")
            spark.sql(s"insert into $tableMultiPartition " +
              s"values(2, 'a2', 10, '2021-07-18','12')")

            checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition order by id")(
              Seq(1, "a1", 10.0, "2021-07-18", "12"),
              Seq(2, "a2", 10.0, "2021-07-18", "12")
            )
            assertResult(WriteOperationType.INSERT) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableMultiPartition").getOperationType
            }
          }
        }
      })
    }
  }

  test("Test bulk insert with insert into for non partitioned table") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict",
      "hoodie.sql.bulk.insert.enable" -> "true") {
      withRecordType()(withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { nonPartitionedTable =>
            spark.sql(
              s"""
                 |create table $nonPartitionedTable (
                 |  id int,
                 |  name string,
                 |  price double
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | location '${tmp.getCanonicalPath}/$nonPartitionedTable'
         """.stripMargin)
            spark.sql(s"insert into $nonPartitionedTable values(1, 'a1', 10)")
            checkAnswer(s"select id, name, price from $nonPartitionedTable")(
              Seq(1, "a1", 10.0)
            )
            assertResult(WriteOperationType.BULK_INSERT) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$nonPartitionedTable").getOperationType
            }
          }
        }
      })
    }
  }

  test("Test bulk insert with CTAS") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict",
      "hoodie.sql.bulk.insert.enable" -> "true") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { inputTable =>
            spark.sql(
              s"""
                 |create table $inputTable (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$inputTable'
         """.stripMargin)
            spark.sql(s"insert into $inputTable values(1, 'a1', 10, '2021-07-18')")

            withTable(generateTableName) { target =>
              spark.sql(
                s"""
                   |create table $target
                   |using hudi
                   |tblproperties(
                   | type = '$tableType',
                   | primaryKey = 'id'
                   |)
                   | location '${tmp.getCanonicalPath}/$target'
                   | as
                   | select * from $inputTable
                   |""".stripMargin)
              checkAnswer(s"select id, name, price, dt from $target order by id")(
                Seq(1, "a1", 10.0, "2021-07-18")
              )
              assertResult(WriteOperationType.BULK_INSERT) {
                getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$target").getOperationType
              }
            }
          }
        }
      }
    }
  }

  test("Test combine before bulk insert") {
    withTempDir { tmp => {
      Seq(true, false).foreach { combineConfig => {
        Seq(true, false).foreach { populateMetaConfig => {
          import spark.implicits._

          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"

          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  value double,
               |  ts long
               |) using hudi
               | location '${tablePath}'
               | tblproperties (
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
               |""".stripMargin)

          val rowsWithSimilarKey = Seq((1, "a1", 10.0, 1000L), (1, "a1", 11.0, 1001L))
          rowsWithSimilarKey.toDF("id", "name", "value", "ts")
            .write.format("hudi")
            // common settings
            .option(HoodieWriteConfig.TBL_NAME.key, tableName)
            .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
            .option(RECORDKEY_FIELD.key, "id")
            .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
            .option(HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key, "1")
            // test specific settings
            .option(OPERATION.key, WriteOperationType.BULK_INSERT.value)
            .option(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key, combineConfig.toString)
            .option(HoodieTableConfig.POPULATE_META_FIELDS.key, populateMetaConfig.toString)
            .mode(SaveMode.Append)
            .save(tablePath)

          val countRows = spark.sql(s"select id, name, value, ts from $tableName").count()
          if (combineConfig) {
            assertResult(1)(countRows)
          } else {
            assertResult(rowsWithSimilarKey.length)(countRows)
          }
        }}
      }}
    }}
  }

  test("Test bulk insert with empty dataset") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { inputTable =>
            spark.sql(
              s"""
                 |create table $inputTable (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$inputTable'
         """.stripMargin)

            // insert empty dataset into target table
            withTable(generateTableName) { target =>
              spark.sql(
                s"""
                   |create table $target
                   |using hudi
                   |tblproperties(
                   | type = '$tableType',
                   | primaryKey = 'id'
                   |)
                   | location '${tmp.getCanonicalPath}/$target'
                   | as
                   | select * from $inputTable where id = 2
                   |""".stripMargin)
              // check the target table is empty
              checkAnswer(s"select id, name, price, dt from $target order by id")(Seq.empty: _*)
            }
          }
        }
      }
    }
  }

  test("Test bulk insert overwrite with rollback") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
      withTempDir { tmp =>
        withTable(generateTableName) { tableName =>
          val tableLocation = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  dt int
               |) using hudi
               | tblproperties (
               |  type = 'cow',
               |  primaryKey = 'id'
               | ) partitioned by (dt)
               | location '$tableLocation'
               | """.stripMargin)

          spark.sql(s"insert overwrite table $tableName partition (dt) values(1, 'a1', 10)")
          checkAnswer(s"select id, name, dt from $tableName order by id")(
            Seq(1, "a1", 10)
          )

          // Simulate a insert overwrite failure
          val metaClient = createMetaClient(spark, tableLocation)
          val instant = WriteClientTestUtils.createNewInstantTime()
          val timeline = HoodieTestUtils.TIMELINE_FACTORY.createActiveTimeline(metaClient)
          timeline.createNewInstant(metaClient.createNewInstant(
            HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instant))
          timeline.createNewInstant(metaClient.createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, instant))

          spark.sql(s"insert overwrite table $tableName partition (dt) values(1, 'a1', 10)")
          checkAnswer(s"select id, name, dt from $tableName order by id")(
            Seq(1, "a1", 10)
          )
        }
      }
    }
  }

  test("Test insert overwrite partitions with empty dataset") {
    Seq(true, false).foreach { enableBulkInsert =>
      val bulkInsertConf: Array[(String, String)] = if (enableBulkInsert) {
        Array(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value(),
          HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false")
      } else {
        Array(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key -> "false")
      }
      withSQLConf(bulkInsertConf: _*) {
        withTempDir { tmp =>
          Seq("cow", "mor").foreach { tableType =>
            withTable(generateTableName) { inputTable =>
              spark.sql(
                s"""
                   |create table $inputTable (
                   |  id int,
                   |  name string,
                   |  price double,
                   |  dt string
                   |) using hudi
                   | tblproperties (
                   |  type = '$tableType',
                   |  primaryKey = 'id'
                   | )
                   | partitioned by (dt)
                   | location '${tmp.getCanonicalPath}/$inputTable'
              """.stripMargin)

              withTable(generateTableName) { target =>
                spark.sql(
                  s"""
                     |create table $target (
                     |  id int,
                     |  name string,
                     |  price double,
                     |  dt string
                     |) using hudi
                     | tblproperties (
                     |  type = '$tableType',
                     |  primaryKey = 'id'
                     | )
                     | partitioned by (dt)
                     | location '${tmp.getCanonicalPath}/$target'
              """.stripMargin)
                spark.sql(s"insert into $target values(3, 'c1', 13, '2021-07-17')")
                spark.sql(s"insert into $target values(1, 'a1', 10, '2021-07-18')")

                // Insert overwrite a partition with empty record
                spark.sql(s"insert overwrite table $target partition(dt='2021-07-17') select id, name, price from $inputTable")
                checkAnswer(s"select id, name, price, dt from $target where dt='2021-07-17'")(Seq.empty: _*)
              }
            }
          }
        }
      }
    }
  }

  test("Test bulk insert with insert overwrite table") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { nonPartitionedTable =>
            spark.sql(
              s"""
                 |create table $nonPartitionedTable (
                 |  id int,
                 |  name string,
                 |  price double
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | location '${tmp.getCanonicalPath}/$nonPartitionedTable'
         """.stripMargin)
            spark.sql(s"insert into $nonPartitionedTable values(1, 'a1', 10)")

            spark.sql(s"insert overwrite table $nonPartitionedTable values(2, 'b1', 11)")
            checkAnswer(s"select id, name, price from $nonPartitionedTable order by id")(
              Seq(2, "b1", 11.0)
            )
            assertResult(WriteOperationType.INSERT_OVERWRITE_TABLE) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$nonPartitionedTable").getOperationType
            }
          }
        }
      }
    }
  }

  test("Test bulk insert with insert overwrite partition") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { partitionedTable =>
            spark.sql(
              s"""
                 |create table $partitionedTable (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$partitionedTable'
         """.stripMargin)
            spark.sql(s"insert into $partitionedTable values(3, 'c1', 13, '2021-07-17')")
            spark.sql(s"insert into $partitionedTable values(1, 'a1', 10, '2021-07-18')")

            // Insert overwrite a partition
            spark.sql(s"insert overwrite table $partitionedTable partition(dt='2021-07-17') values(2, 'b1', 11)")
            checkAnswer(s"select id, name, price, dt from $partitionedTable order by id")(
              Seq(1, "a1", 10.0, "2021-07-18"),
              Seq(2, "b1", 11.0, "2021-07-17")
            )
            assertResult(WriteOperationType.INSERT_OVERWRITE) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$partitionedTable").getOperationType
            }

            // Insert overwrite whole table
            spark.sql(s"insert overwrite table $partitionedTable values(4, 'd1', 14, '2021-07-19')")
            checkAnswer(s"select id, name, price, dt from $partitionedTable order by id")(
              Seq(4, "d1", 14.0, "2021-07-19")
            )
            assertResult(WriteOperationType.INSERT_OVERWRITE_TABLE) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$partitionedTable").getOperationType
            }
          }
        }
      }
    }
  }

  test("Test combine before insert") {
    Seq("cow", "mor").foreach { tableType =>
      withSQLConf("hoodie.sql.bulk.insert.enable" -> "false", "hoodie.merge.allow.duplicate.on.inserts" -> "false") {
        withRecordType()(withTempDir { tmp =>
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
               |  primaryKey = 'id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)
          spark.sql(
            s"""
               |insert overwrite table $tableName
               |select * from (
               | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
               | union all
               | select 1 as id, 'a1' as name, 11 as price, 1001 as ts
               | )
               |""".stripMargin
          )
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 11.0, 1001)
          )
        })
      }
    }
  }

  test("Test insert pk-table") {
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    Seq("cow", "mor").foreach { tableType =>
      withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
        withRecordType()(withTempDir { tmp =>
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
               |  primaryKey = 'id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(1, 'a1', 11, 1000)")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 11.0, 1000)
          )
        })
      }
    }
  }

  test("Test For read operation's field") {
    withRecordType()(withTempDir { tmp => {
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      import spark.implicits._
      val day = "2021-08-02"
      val df = Seq((1, "a1", 10, 1000, day, 12)).toDF("id", "name", "value", "ts", "day", "hh")
      // Write a table by spark dataframe.
      df.write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, tableName)
        .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
        .option(RECORDKEY_FIELD.key, "id")
        .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
        .option(PARTITIONPATH_FIELD.key, "day,hh")
        .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
        .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
        .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
        .option(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key, "true")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val metaClient = createMetaClient(spark, tablePath)

      assertResult(true)(new TableSchemaResolver(metaClient).hasOperationField)

      spark.sql(
        s"""
           |create table $tableName using hudi
           |location '${tablePath}'
           |""".stripMargin)

      // Note: spark sql batch write currently does not write actual content to the operation field
      checkAnswer(s"select id, _hoodie_operation from $tableName")(
        Seq(1, null)
      )
    }
    })
  }

  test("Test enable hoodie.datasource.write.drop.partition.columns when write") {
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
      Seq("mor", "cow").foreach { tableType =>
        withRecordType()(withTempDir { tmp =>
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
               |  type = '$tableType',
               |  hoodie.datasource.write.drop.partition.columns = 'true'
               | )
       """.stripMargin)
          spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (2, 'a2', 20, 1000)")
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1", 10, 1000, "2021-12-25"),
            Seq(2, "a2", 20, 1000, "2021-12-25")
          )
        })
      }
    }
  }

  test("Test nested field as primaryKey and preCombineField") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  name string,
             |  price double,
             |  ts long,
             |  nestedcol struct<a1:string, a2:struct<b1:string, b2:struct<c1:string, c2:int>>>
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | options (
             |  type = '$tableType',
             |  primaryKey = 'nestedcol.a1',
             |  preCombineField = 'nestedcol.a2.b2.c2'
             | )
       """.stripMargin)
        // insert data to table
        spark.sql(
          s"""insert into $tableName values
             |('name_1', 10, 1000, struct('a', struct('b', struct('c', 999)))),
             |('name_2', 20, 2000, struct('a', struct('b', struct('c', 333))))
             |""".stripMargin)
        checkAnswer(s"select name, price, ts, nestedcol.a1, nestedcol.a2.b2.c2 from $tableName")(
          Seq("name_1", 10.0, 1000, "a", 999)
        )
      }
    })
  }

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

  private def validateDifferentTypesOfPartitionColumn(tmp: File, partitionType: String, partitionValue: Any, tableName: String) = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt $partitionType
         |) using hudi
         | tblproperties (primaryKey = 'id')
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
    // NOTE: We have to drop type-literal prefix since Spark doesn't parse type literals appropriately
    spark.sql(s"insert into $tableName partition(dt = ${dropTypeLiteralPrefix(partitionValue)}) select 1, 'a1', 10")
    // try again to trigger hoodieFileIndex
    spark.sql(s"insert overwrite $tableName partition(dt = ${dropTypeLiteralPrefix(partitionValue)}) select 1, 'a1', 10")
    spark.sql(s"insert into $tableName select 2, 'a2', 10, $partitionValue")
    checkAnswer(s"select id, name, price, cast(dt as string) from $tableName order by id")(
      Seq(1, "a1", 10, extractRawValue(partitionValue).toString),
      Seq(2, "a2", 10, extractRawValue(partitionValue).toString)
    )
  }

  test("Test enable hoodie.merge.allow.duplicate.on.inserts when write") {
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sql("set hoodie.datasource.write.operation = insert")
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
        spark.sql("set hoodie.merge.allow.duplicate.on.inserts = false")
        spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (1, 'a2', 20, 1001)")
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a2", 20, 1001, "2021-12-25")
        )
        spark.sql("set hoodie.merge.allow.duplicate.on.inserts = true")
        spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (1, 'a3', 30, 1002)")
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a2", 20, 1001, "2021-12-25"),
          Seq(1, "a3", 30, 1002, "2021-12-25")
        )
      }
    }
    spark.sql("set hoodie.merge.allow.duplicate.on.inserts = false")
    spark.sql("set hoodie.datasource.write.operation = upsert")
  }

  test("Test Insert Into Bucket Index Table") {
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

      spark.sql("set hoodie.merge.allow.duplicate.on.inserts = false")
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
   *       modes work
   */
  test(s"Test Bulk Insert with all sort-modes") {
    withTempDir { basePath =>
      BulkInsertSortMode.values().foreach { sortMode =>
        val tableName = generateTableName
        // Remove these with [HUDI-5419]
        spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
        spark.sessionState.conf.unsetConf("hoodie.datasource.write.insert.drop.duplicates")
        spark.sessionState.conf.unsetConf("hoodie.merge.allow.duplicate.on.inserts")
        spark.sessionState.conf.unsetConf("hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled")
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

        spark.sql("set hoodie.sql.bulk.insert.enable = true")
        spark.sql("set hoodie.sql.insert.mode = non-strict")

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
      assertResult(WriteOperationType.BULK_INSERT) {
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

  test("Test Bulk Insert Into Consistent Hashing Bucket Index Table") {
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    withSQLConf("hoodie.datasource.write.operation" -> "bulk_insert") {
      Seq("false", "true").foreach { bulkInsertAsRow =>
        withTempDir { tmp =>
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
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
               | tblproperties (
               | primaryKey = 'id,name',
               | type = 'mor',
               | preCombineField = 'ts',
               | hoodie.index.type = 'BUCKET',
               | hoodie.index.bucket.engine = 'CONSISTENT_HASHING',
               | hoodie.bucket.index.hash.field = 'id,name',
               | hoodie.datasource.write.row.writer.enable = '$bulkInsertAsRow'
               | )
               | partitioned by (dt)
               | location '${basePath}'
           """.stripMargin)

          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 10, 1000, "2021-01-05"),
               | (2, 'a1,2', 10, 1000, "2021-01-05"),
               | (1, 'a2,1', 20, 2000, "2021-01-06"),
               | (2, 'a2,2', 20, 2000, "2021-01-06"),
               | (1, 'a3,1', 30, 3000, "2021-01-07"),
               | (2, 'a3,2', 30, 3000, "2021-01-07")
            """.stripMargin)

          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
            Seq(2, "a1,2", 10.0, 1000, "2021-01-05"),
            Seq(1, "a2,1", 20.0, 2000, "2021-01-06"),
            Seq(2, "a2,2", 20.0, 2000, "2021-01-06"),
            Seq(1, "a3,1", 30.0, 3000, "2021-01-07"),
            Seq(2, "a3,2", 30.0, 3000, "2021-01-07")
          )

          // there are six files in the table, each partition contains two files
          checkAnswer(s"select count(distinct _hoodie_file_name) from $tableName")(
            Seq(6)
          )

          val insertStatement =
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 11, 1000, "2021-01-05"),
               | (2, 'a1,2', 11, 1000, "2021-01-05"),
               | (3, 'a2,1', 21, 2000, "2021-01-05"),
               | (4, 'a2,2', 21, 2000, "2021-01-05"),
               | (5, 'a3,1', 31, 3000, "2021-01-05"),
               | (6, 'a3,2', 31, 3000, "2021-01-05")
            """.stripMargin

          // We can only upsert to existing consistent hashing bucket index table
          checkExceptionContain(insertStatement)("Consistent Hashing bulk_insert only support write to new file group")

          spark.sql("set hoodie.datasource.write.operation = upsert")
          spark.sql(insertStatement)

          checkAnswer(s"select id, name, price, ts, dt from $tableName where dt = '2021-01-05'")(
            Seq(1, "a1,1", 11.0, 1000, "2021-01-05"),
            Seq(2, "a1,2", 11.0, 1000, "2021-01-05"),
            Seq(3, "a2,1", 21.0, 2000, "2021-01-05"),
            Seq(4, "a2,2", 21.0, 2000, "2021-01-05"),
            Seq(5, "a3,1", 31.0, 3000, "2021-01-05"),
            Seq(6, "a3,2", 31.0, 3000, "2021-01-05")
          )

          val clusteringOptions: Map[String, String] = Map(
            s"${RECORDKEY_FIELD.key()}" -> "id,name",
            s"${ENABLE_ROW_WRITER.key()}" -> s"${bulkInsertAsRow}",
            s"${HoodieIndexConfig.INDEX_TYPE.key()}" -> s"${IndexType.BUCKET.name()}",
            s"${HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()}" -> "id,name",
            s"${HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key()}" -> "CONSISTENT_HASHING",
            s"${HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME.key()}" -> "org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy",
            s"${HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME.key()}" -> "org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy"
          )

          val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, clusteringOptions, Option(tableName))

          // Test bucket merge by clustering
          val instant = client.scheduleClustering(HOption.empty()).get()

          checkAnswer(s"call show_clustering(table => '$tableName')")(
            Seq(instant, 10, HoodieInstant.State.REQUESTED.name(), "*")
          )

          client.cluster(instant)

          checkAnswer(s"call show_clustering(table => '$tableName')")(
            Seq(instant, 10, HoodieInstant.State.COMPLETED.name(), "*")
          )

          spark.sql("set hoodie.datasource.write.operation = bulk_insert")
        }
      }
    }
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  /**
   * When neither of strict mode nor sql.write.operation is set, sql write operation is deduced as UPSERT
   * due to presence of preCombineField.
   */
  test("Test sql write operation with INSERT_INTO No explicit configs") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { tableName =>
          ingestAndValidateData(tableType, tableName, tmp, WriteOperationType.UPSERT)
        }
      }
    }
  }

  test("Test sql write operation with INSERT_INTO override both strict mode and sql write operation") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq(WriteOperationType.INSERT, WriteOperationType.BULK_INSERT, WriteOperationType.UPSERT).foreach { operation =>
          withTable(generateTableName) { tableName =>
            ingestAndValidateData(tableType, tableName, tmp, operation,
              List("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + " = " + operation.value(), "set hoodie.sql.insert.mode = upsert"))
          }
        }
      }
    }
  }

  test("Test sql write operation with INSERT_INTO override only sql write operation") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq(WriteOperationType.INSERT, WriteOperationType.BULK_INSERT, WriteOperationType.UPSERT).foreach { operation =>
          withTable(generateTableName) { tableName =>
            ingestAndValidateData(tableType, tableName, tmp, operation,
              List("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + " = " + operation.value()))
          }
        }
      }
    }
  }

  test("Test sql write operation with INSERT_INTO override only strict mode") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf(DataSourceWriteOptions.INSERT_DUP_POLICY.key())
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    spark.sessionState.conf.unsetConf("hoodie.sql.bulk.insert.enable")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { tableName =>
          ingestAndValidateData(tableType, tableName, tmp, WriteOperationType.UPSERT,
            List("set hoodie.sql.insert.mode = upsert"))
        }
      }
    }
  }

  def ingestAndValidateData(tableType: String, tableName: String, tmp: File,
                            expectedOperationtype: WriteOperationType,
                            setOptions: List[String] = List.empty) : Unit = {
    setOptions.foreach(entry => {
      spark.sql(entry)
    })

    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  preCombine = 'name'
         | )
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

    spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    checkAnswer(s"select id, name, price, dt from $tableName")(
      Seq(1, "a1", 10.0, "2021-07-18")
    )

    // insert record again but w/ diff values but same primary key.
    spark.sql(
      s"""
         | insert into $tableName values
         | (1, 'a1_1', 10, "2021-07-18"),
         | (2, 'a2', 20, "2021-07-18"),
         | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    if (expectedOperationtype == WriteOperationType.UPSERT) {
      // dedup should happen within same batch being ingested and existing records on storage should get updated
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    } else {
      // no dedup across batches
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "2021-07-18"),
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        // Seq(2, "a2", 20.0, "2021-07-18"), // preCombine within same batch kicks in if preCombine is set
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  test("Test sql write operation with INSERT_INTO No explicit configs No Precombine") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { tableName =>
          ingestAndValidateDataNoPrecombine(tableType, tableName, tmp, WriteOperationType.INSERT)
        }
      }
    }
  }

  var listenerCallCount: Int = 0
  var countDownLatch: CountDownLatch = _

  // add a listener for stages for parallelism checking with stage name
  class StageParallelismListener(var stageName: String) extends SparkListener {
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      if (stageSubmitted.stageInfo.name.contains(stageName)) {
        assertResult(1)(stageSubmitted.stageInfo.numTasks)
        listenerCallCount = listenerCallCount + 1
        countDownLatch.countDown
      }
    }
  }

  test("Test multiple partition fields pruning") {

    withTempDir { tmp =>
      val targetTable = generateTableName
      countDownLatch = new CountDownLatch(1)
      listenerCallCount = 0
      spark.sql(
        s"""
           |create table ${targetTable} (
           |  `id` string,
           |  `name` string,
           |  `dt` bigint,
           |  `day` STRING,
           |  `hour` INT
           |) using hudi
           |tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'mor',
           |  'preCombineField'='dt',
           |  'hoodie.index.type' = 'BUCKET',
           |  'hoodie.bucket.index.hash.field' = 'id',
           |  'hoodie.bucket.index.num.buckets'=512,
           |  'hoodie.metadata.enable'='false'
           | )
           |partitioned by (`day`,`hour`)
           |location '${tmp.getCanonicalPath}/$targetTable'
           |""".stripMargin)
      disableComplexKeygenValidation(spark, targetTable)
      spark.sql(
        s"""
           |insert into ${targetTable}
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 10 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 11 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 12 as `hour`
           |""".stripMargin)
      val stageClassName = classOf[HoodieSparkEngineContext].getSimpleName
      spark.sparkContext.addSparkListener(new StageParallelismListener(stageName = stageClassName))
      val df = spark.sql(
        s"""
           |select * from ${targetTable} where day='2023-10-12' and hour=11
           |""".stripMargin)
      var rddHead = df.rdd
      while (rddHead.dependencies.size > 0) {
        assertResult(1)(rddHead.partitions.size)
        rddHead = rddHead.firstParent
      }
      assertResult(1)(rddHead.partitions.size)
      countDownLatch.await(1, TimeUnit.MINUTES)
      assert(listenerCallCount >= 1)
    }
  }

  test("Test single partiton field pruning") {

    withTempDir { tmp =>
      countDownLatch = new CountDownLatch(1)
      listenerCallCount = 0
      val targetTable = generateTableName
      spark.sql(
        s"""
           |create table ${targetTable} (
           |  `id` string,
           |  `name` string,
           |  `dt` bigint,
           |  `day` STRING,
           |  `hour` INT
           |) using hudi
           |tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'mor',
           |  'preCombineField'='dt',
           |  'hoodie.index.type' = 'BUCKET',
           |  'hoodie.bucket.index.hash.field' = 'id',
           |  'hoodie.bucket.index.num.buckets'=512,
           |  'hoodie.metadata.enable'='false'
           | )
           |partitioned by (`day`)
           |location '${tmp.getCanonicalPath}/$targetTable'
           |""".stripMargin)
      spark.sql(
        s"""
           |insert into ${targetTable}
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 10 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 11 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 12 as `hour`
           |""".stripMargin)
      val stageClassName = classOf[HoodieSparkEngineContext].getSimpleName
      spark.sparkContext.addSparkListener(new StageParallelismListener(stageName = stageClassName))
      val df = spark.sql(
        s"""
           |select * from ${targetTable} where day='2023-10-12' and hour=11
           |""".stripMargin)
      var rddHead = df.rdd
      while (rddHead.dependencies.size > 0) {
        assertResult(1)(rddHead.partitions.size)
        rddHead = rddHead.firstParent
      }
      assertResult(1)(rddHead.partitions.size)
      countDownLatch.await(1, TimeUnit.MINUTES)
      assert(listenerCallCount >= 1)
    }
  }

  test("Test inaccurate index type") {
    withTempDir { tmp =>
      val targetTable = generateTableName

      assertThrows[IllegalArgumentException] {
        try {
          spark.sql(
            s"""
               |create table ${targetTable} (
               |  `id` string,
               |  `name` string,
               |  `dt` bigint,
               |  `day` STRING,
               |  `hour` INT
               |) using hudi
               |OPTIONS ('hoodie.datasource.write.hive_style_partitioning' 'false', 'hoodie.datasource.meta.sync.enable' 'false', 'hoodie.datasource.hive_sync.enable' 'false')
               |tblproperties (
               |  'primaryKey' = 'id',
               |  'type' = 'mor',
               |  'preCombineField'='dt',
               |  'hoodie.index.type' = 'BUCKET_aa',
               |  'hoodie.bucket.index.hash.field' = 'id',
               |  'hoodie.bucket.index.num.buckets'=512
               | )
               |partitioned by (`day`,`hour`)
               |location '${tmp.getCanonicalPath}'
               |""".stripMargin)
        }
      }
    }
  }

  test("Test vectorized read nested columns for LegacyHoodieParquetFileFormat") {
    withSQLConf(
      "hoodie.datasource.read.use.new.parquet.file.format" -> "false",
      "hoodie.file.group.reader.enabled" -> "false",
      "spark.sql.parquet.enableNestedColumnVectorizedReader" -> "true",
      "spark.sql.parquet.enableVectorizedReader" -> "true") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  attributes map<string, string>,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (primaryKey = 'id')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
                    """.stripMargin)
        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1', map('color', 'red', 'size', 'M'), 10, 1000, '2021-01-05'),
             | (2, 'a2', map('color', 'blue', 'size', 'L'), 20, 2000, '2021-01-06'),
             | (3, 'a3', map('color', 'green', 'size', 'S'), 30, 3000, '2021-01-07')
                    """.stripMargin)
        // Check the inserted records with map type attributes
        checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
      }
    }
  }

  def ingestAndValidateDataNoPrecombine(tableType: String, tableName: String, tmp: File,
                                        expectedOperationtype: WriteOperationType,
                                        setOptions: List[String] = List.empty) : Unit = {
    setOptions.foreach(entry => {
      spark.sql(entry)
    })

    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id'
         | )
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

    spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    checkAnswer(s"select id, name, price, dt from $tableName")(
      Seq(1, "a1", 10.0, "2021-07-18")
    )

    // insert record again but w/ diff values but same primary key.
    spark.sql(
      s"""
         | insert into $tableName values
         | (1, 'a1_1', 10, "2021-07-18"),
         | (2, 'a2', 20, "2021-07-18"),
         | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    if (expectedOperationtype == WriteOperationType.UPSERT) {
      // dedup should happen within same batch being ingested and existing records on storage should get updated
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    } else {
      // no dedup across batches
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "2021-07-18"),
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2", 20.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  test("Test insert dup policy with INSERT_INTO explicit new configs INSERT operation ") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val operation = WriteOperationType.INSERT
        Seq(NONE_INSERT_DUP_POLICY, DROP_INSERT_DUP_POLICY).foreach { dupPolicy =>
          withTable(generateTableName) { tableName =>
            ingestAndValidateDataDupPolicy(tableType, tableName, tmp, operation,
              List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${operation.value}",
                s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"),
              dupPolicy)
          }
        }
      }
    }
  }

  test("Test insert dup policy with INSERT_INTO explicit new configs BULK_INSERT operation ") {
    withTempDir { tmp =>
      Seq("cow").foreach { tableType =>
        val operation = WriteOperationType.BULK_INSERT
        val dupPolicy = NONE_INSERT_DUP_POLICY
        withTable(generateTableName) { tableName =>
          ingestAndValidateDataDupPolicy(tableType, tableName, tmp, operation,
            List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${operation.value}",
              s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"),
            dupPolicy)
        }
      }
    }
  }

  test("Test DROP insert dup policy with INSERT_INTO explicit new configs BULK INSERT operation") {
    withRecordType(Seq(HoodieRecordType.AVRO))(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val dupPolicy = DROP_INSERT_DUP_POLICY
        withTable(generateTableName) { tableName =>
          ingestAndValidateDropDupPolicyBulkInsert(tableType, tableName, tmp,
            List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${WriteOperationType.BULK_INSERT.value}",
              s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"))
        }
      }
    })
  }

  test("Test FAIL insert dup policy with INSERT_INTO explicit new configs") {
    withRecordType(Seq(HoodieRecordType.AVRO))(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val operation = WriteOperationType.INSERT
        val dupPolicy = FAIL_INSERT_DUP_POLICY
        withTable(generateTableName) { tableName =>
          ingestAndValidateDataDupPolicy(tableType, tableName, tmp, operation,
            List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${operation.value}",
              s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"),
            dupPolicy, true)
        }
      }
    })
  }

  test("Test various data types as partition fields") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT,
           |  boolean_field BOOLEAN,
           |  float_field FLOAT,
           |  byte_field BYTE,
           |  short_field SHORT,
           |  decimal_field DECIMAL(10, 5),
           |  date_field DATE,
           |  string_field STRING,
           |  timestamp_field TIMESTAMP
           |) USING hudi
           | TBLPROPERTIES (primaryKey = 'id')
           | PARTITIONED BY (boolean_field, float_field, byte_field, short_field, decimal_field, date_field, string_field, timestamp_field)
           |LOCATION '${tmp.getCanonicalPath}'
     """.stripMargin)
      // Avoid operation type modification.
      spark.sql(s"set ${INSERT_DROP_DUPS.key}=false")
      spark.sql(s"set ${INSERT_DUP_POLICY.key}=$NONE_INSERT_DUP_POLICY")
      disableComplexKeygenValidation(spark, tableName)
      // Insert data into partitioned table
      spark.sql(
        s"""
           |INSERT INTO $tableName VALUES
           |(1, TRUE, CAST(1.0 as FLOAT), 1, 1, 1234.56789, DATE '2021-01-05', 'partition1', TIMESTAMP '2021-01-05 10:00:00'),
           |(2, FALSE,CAST(2.0 as FLOAT), 2, 2, 6789.12345, DATE '2021-01-06', 'partition2', TIMESTAMP '2021-01-06 11:00:00')
     """.stripMargin)

      checkAnswer(s"SELECT id, boolean_field FROM $tableName ORDER BY id")(
        Seq(1, true),
        Seq(2, false)
      )
    })
  }

  test(s"Test INSERT INTO with upsert operation type") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  ts long,
             |  price int
             |) using hudi
             |partitioned by (ts)
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |location '${tmp.getCanonicalPath}/$tableName'
             |""".stripMargin
        )

        // Test insert into with upsert operation type
        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 1000, 10), (2, 'a2', 2000, 20), (3, 'a3', 3000, 30), (4, 'a4', 2000, 10), (5, 'a5', 3000, 20), (6, 'a6', 4000, 30)
             | """.stripMargin
        )
        checkAnswer(s"select id, name, price, ts from $tableName where price > 3000")(
          Seq(6, "a6", 4000, 30)
        )

        // Test update
        spark.sql(s"update $tableName set price = price + 1 where id = 6")
        checkAnswer(s"select id, name, price, ts from $tableName where price > 3000")(
          Seq(6, "a6", 4001, 30)
        )
      }
    }
  }

  test("Test Insert Into with extraMetadata") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  dt string,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (primaryKey = 'id')
           | partitioned by (dt)
           | location '$tablePath'
       """.stripMargin)

      spark.sql("set hoodie.datasource.write.commitmeta.key.prefix=commit_extra_meta_")

      spark.sql("set commit_extra_meta_a=valA")
      spark.sql("set commit_extra_meta_b=valB")
      spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, '2024-06-14')")

      assertResult("valA") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_a")
      }
      assertResult("valB") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_b")
      }
      checkAnswer(s"select id, name, price, dt from $tableName")(
        Seq(1, "a1", 10.0, "2024-06-14")
      )

      spark.sql("set commit_extra_meta_a=new_valA")
      spark.sql("set commit_extra_meta_b=new_valB")
      spark.sql(s"insert into $tableName values (2, 'a2', 20, 2000, '2024-06-14')")

      assertResult("new_valA") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_a")
      }
      assertResult("new_valB") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_b")
      }
      checkAnswer(s"select id, name, price, dt from $tableName")(
        Seq(1, "a1", 10.0, "2024-06-14"),
        Seq(2, "a2", 20.0, "2024-06-14")
      )
    }
  }

  def ingestAndValidateDataDupPolicy(tableType: String, tableName: String, tmp: File,
                                     expectedOperationtype: WriteOperationType = WriteOperationType.INSERT,
                                     setOptions: List[String] = List.empty,
                                     insertDupPolicy : String = NONE_INSERT_DUP_POLICY,
                                     expectExceptionOnSecondBatch: Boolean = false) : Unit = {

    // set additional options
    setOptions.foreach(entry => {
      spark.sql(entry)
    })

    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  preCombine = 'name'
         | )
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

    spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    checkAnswer(s"select id, name, price, dt from $tableName")(
      Seq(1, "a1", 10.0, "2021-07-18")
    )

    if (expectExceptionOnSecondBatch) {
      assertThrows[HoodieDuplicateKeyException] {
        try {
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1_1', 10, "2021-07-18"),
               | (2, 'a2', 20, "2021-07-18"),
               | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)
        } catch {
          case e: Exception =>
            var root: Throwable = e
            while (root.getCause != null) {
              root = root.getCause
            }
            throw root
        }
      }
    } else {

      // insert record again w/ diff values but same primary key. Since "insert" is chosen as operation type,
      // dups should be seen w/ snapshot query
      spark.sql(
        s"""
           | insert into $tableName values
           | (1, 'a1_1', 10, "2021-07-18"),
           | (2, 'a2', 20, "2021-07-18"),
           | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)

      assertResult(expectedOperationtype) {
        getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
      }

      if (expectedOperationtype == WriteOperationType.UPSERT) {
        // dedup should happen within same batch being ingested and existing records on storage should get updated
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1_1", 10.0, "2021-07-18"),
          Seq(2, "a2_2", 30.0, "2021-07-18")
        )
      } else {
        if (insertDupPolicy == NONE_INSERT_DUP_POLICY) {
          // no dedup across batches
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 10.0, "2021-07-18"),
            Seq(1, "a1_1", 10.0, "2021-07-18"),
            // Seq(2, "a2", 20.0, "2021-07-18"), // preCombine within same batch kicks in if preCombine is set
            Seq(2, "a2_2", 30.0, "2021-07-18")
          )
        } else if (insertDupPolicy == DROP_INSERT_DUP_POLICY) {
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 10.0, "2021-07-18"),
            // Seq(2, "a2", 20.0, "2021-07-18"), // preCombine within same batch kicks in if preCombine is set
            Seq(2, "a2_2", 30.0, "2021-07-18")
          )
        }
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  def ingestAndValidateDropDupPolicyBulkInsert(tableType: String,
                                               tableName: String,
                                               tmp: File,
                                               setOptions: List[String] = List.empty) : Unit = {

    // set additional options
    setOptions.foreach(entry => {
      spark.sql(entry)
    })
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id'
         | )
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

    // drop dups is not supported in bulk_insert row writer path.
    assertThrows[HoodieException] {
      try {
        spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")
      } catch {
        case e: Exception =>
          var root: Throwable = e
          while (root.getCause != null) {
            root = root.getCause
          }
          throw root
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  test("Test insert into with special cols") {
    withTempDir { tmp =>
      val targetTableA = generateTableName
      val tablePathA = s"${tmp.getCanonicalPath}/$targetTableA"
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      spark.sql(
        s"""
           |create table if not exists $targetTableA (
           | id bigint,
           | name string,
           | price double
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) location '$tablePathA'
           |""".stripMargin)

      spark.sql(s"insert into $targetTableA (id, price, name) values (1, 12.1, 'aaa')")

      checkAnswer(s"select id, price, name from $targetTableA")(
        Seq(1, 12.1, "aaa")
      )

      val targetTableB = generateTableName
      val tablePathB = s"${tmp.getCanonicalPath}/$targetTableB"

      spark.sql(
        s"""
           |create table if not exists $targetTableB (
           | id bigint,
           | name string,
           | price double,
           | day string,
           | hour string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) partitioned by (day, hour)
           |location '$tablePathB'
           |""".stripMargin)

      disableComplexKeygenValidation(spark, targetTableB)

      spark.sql(s"insert into $targetTableB (id, day, price, name, hour) " +
        s"values (2, '01', 12.2, 'bbb', '02')")

      spark.sql(s"insert into $targetTableB (id, day, price, name, hour) " +
        s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")

      spark.sql(s"insert into $targetTableB partition(day='02', hour) (id, hour, price, name) " +
        s"values (3, '01', 12.3, 'ccc')")

      spark.sql(s"insert into $targetTableB partition(day='02', hour='02') (id, price, name) " +
        s"values (4, 12.4, 'ddd')")

      checkAnswer(s"select id, price, name, day, hour from $targetTableB")(
        Seq(2, 12.2, "bbb", "01", "02"),
        Seq(1, 12.1, "aaa", "01", "03"),
        Seq(3, 12.3, "ccc", "02", "01"),
        Seq(4, 12.4, "ddd", "02", "02")
      )

      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = true")
        checkExceptionContain(s"insert into $targetTableB (id, day, price, name, hour) " +
          s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")(
          "hudi not support specified cols when enable default columns")
      }
    }
  }

  test("Test insert overwrite with special cols") {
    withTempDir { tmp =>
      val targetTableA = generateTableName
      val tablePathA = s"${tmp.getCanonicalPath}/$targetTableA"
      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = false")
      }

      spark.sql(
        s"""
           |create table if not exists $targetTableA (
           | id bigint,
           | name string,
           | price double
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) location '$tablePathA'
           |""".stripMargin)

      spark.sql(s"insert overwrite $targetTableA (id, price, name) values (1, 12.1, 'aaa')")

      checkAnswer(s"select id, price, name from $targetTableA")(
        Seq(1, 12.1, "aaa")
      )

      val targetTableB = generateTableName
      val tablePathB = s"${tmp.getCanonicalPath}/$targetTableB"

      spark.sql(
        s"""
           |create table if not exists $targetTableB (
           | id bigint,
           | name string,
           | price double,
           | day string,
           | hour string
           |) using hudi
           |tblproperties (
           | primaryKey = 'id',
           | type = 'mor',
           | preCombineField = 'name'
           |) partitioned by (day, hour)
           |location '$tablePathB'
           |""".stripMargin)

      disableComplexKeygenValidation(spark, targetTableB)

      spark.sql(s"insert overwrite $targetTableB (id, day, price, name, hour) " +
        s"values (2, '01', 12.2, 'bbb', '02')")

      checkAnswer(s"select id, price, name, day, hour from $targetTableB")(
        Seq(2, 12.2, "bbb", "01", "02")
      )

      spark.sql(s"insert overwrite $targetTableB (id, day, price, name, hour) " +
        s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")

      spark.sql(s"insert overwrite $targetTableB partition(day='02', hour) (id, hour, price, name) " +
        s"values (3, '01', 12.3, 'ccc')")

      spark.sql(s"insert overwrite $targetTableB partition(day='02', hour='02') (id, price, name) " +
        s"values (4, 12.4, 'ddd')")

      checkAnswer(s"select id, price, name, day, hour from $targetTableB")(
        Seq(1, 12.1, "aaa", "01", "03"),
        Seq(3, 12.3, "ccc", "02", "01"),
        Seq(4, 12.4, "ddd", "02", "02")
      )

      if (HoodieSparkUtils.isSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled = true")
        checkExceptionContain(s"insert overwrite $targetTableB (id, day, price, name, hour) " +
          s"select id, '01' as dt, price, name, '03' as hour from $targetTableA")(
          "hudi not support specified cols when enable default columns")
      }
    }
  }

  test("Test SparkKeyGenerator When Bulk Insert") {
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "true", "hoodie.sql.insert.mode" -> "non-strict") {
      withRecordType()(withTempDir { tmp =>
        val tableName = generateTableName
        // Create a multi-level partitioned table
        // Specify wrong keygenarator by setting hoodie.datasource.write.keygenerator.class = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator'
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string,
             |  pt string
             |) using hudi
             |tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.table.keygenerator.class = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator',
             |  hoodie.datasource.write.keygenerator.class = 'org.apache.hudi.keygen.ComplexAvroKeyGenerator'
             |)
             | partitioned by (dt, pt)
             | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)
        disableComplexKeygenValidation(spark, tableName)
        //Insert data and check the same
        spark.sql(
          s"""insert into $tableName  values
             |(1, 'a', 31, 1000, '2021-01-05', 'A'),
             |(2, 'b', 18, 1000, '2021-01-05', 'A')
             |""".stripMargin)
        checkAnswer(s"select id, name, price, ts, dt, pt from $tableName order by dt")(
          Seq(1, "a", 31, 1000, "2021-01-05", "A"),
          Seq(2, "b", 18, 1000, "2021-01-05", "A")
        )
      })
    }
  }

  test("Test table with insert dup policy") {
    withTempDir { tmp =>
      Seq(
        NONE_INSERT_DUP_POLICY,
        FAIL_INSERT_DUP_POLICY,
        DROP_INSERT_DUP_POLICY).foreach(policy => {
        val targetTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

        spark.sql(s"set hoodie.datasource.insert.dup.policy=$policy")
        spark.sql(
          s"""
             |create table ${targetTable} (
             |  `id` string,
             |  `name` string,
             |  `dt` bigint,
             |  `day` STRING,
             |  `hour` INT
             |) using hudi
             |tblproperties (
             |  'primaryKey' = 'id',
             |  'type' = 'MOR',
             |  'preCombineField'= 'dt',
             |  'hoodie.bucket.index.hash.field' = 'id',
             |  'hoodie.bucket.index.num.buckets'= 512
             | )
           partitioned by (`day`,`hour`)
           location '${tablePath}'
           """.stripMargin)
        disableComplexKeygenValidation(spark, targetTable)
        spark.sql(
          s"""
             |ALTER TABLE $targetTable
             |SET TBLPROPERTIES (hoodie.write.complex.keygen.encode.single.record.key.field.name = 'true')
             |""".stripMargin)
        spark.sql("set spark.sql.shuffle.partitions = 11")
        spark.sql(
          s"""
             |insert into ${targetTable}
             |select '1' as id, 'aa' as name, 123 as dt, '2024-02-19' as `day`, 10 as `hour`
             |""".stripMargin)
        if (policy.equals(FAIL_INSERT_DUP_POLICY)) {
          checkExceptionContain(
            () => spark.sql(
            s"""
               |insert into ${targetTable}
               |select '1' as id, 'aa' as name, 1234 as dt, '2024-02-19' as `day`, 10 as `hour`
               |""".stripMargin))(s"Duplicate key found for insert statement, key is: id:1")
        } else {
          spark.sql(s"set hoodie.datasource.write.operation=insert")
          spark.sql(
            s"""
               |insert into ${targetTable}
               |select '1' as id, 'aa' as name, 1234 as dt, '2024-02-19' as `day`, 10 as `hour`
               |""".stripMargin)
          if (policy.equals(NONE_INSERT_DUP_POLICY)) {
            checkAnswer(s"select id, name, dt, day, hour from $targetTable limit 10")(
              Seq("1", "aa", 123, "2024-02-19", 10),
              Seq("1", "aa", 1234, "2024-02-19", 10)
            )
          } else {
            checkAnswer(s"select id, name, dt, day, hour from $targetTable limit 10")(
              Seq("1", "aa", 123, "2024-02-19", 10)
            )
          }
        }
      })
    }
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
  }

  test("Test throwing error on schema evolution in INSERT INTO") {
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

        // INSERT INTO with same set of columns as that in the table schema works
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

        // INSERT INTO with an additional column that does not exist in the table schema
        // throws an error, as INSERT INTO does not allow schema evolution
        val sqlStatement =
          s"""
             | insert into $tableName partition(dt = '2024-01-14')
             | select 3 as id, 'a3' as name, 30 as price, 1003 as ts, 'x' as new_col
             | union
             | select 2 as id, 'a2_updated' as name, 25 as price, 1005 as ts, 'y' as new_col
             | """.stripMargin
        val expectedExceptionMessage = if (HoodieSparkUtils.gteqSpark3_5) {
          "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] " +
            s"Cannot write to `spark_catalog`.`default`.`$tableName`, " +
            "the reason is too many data columns:\n" +
            "Table columns: `id`, `name`, `price`, `ts`.\n" +
            "Data columns: `id`, `name`, `price`, `ts`, `new_col`."
        } else {
          val endingStr = if (HoodieSparkUtils.gteqSpark3_4) "." else ""
          val tableId = if (HoodieSparkUtils.gteqSpark3_4) {
            s"spark_catalog.default.$tableName"
          } else {
            s"default.$tableName"
          }
          s"Cannot write to '$tableId', too many data columns:\n" +
            s"Table columns: 'id', 'name', 'price', 'ts'$endingStr\n" +
            s"Data columns: 'id', 'name', 'price', 'ts', 'new_col'$endingStr"
        }
        checkExceptionContain(sqlStatement)(expectedExceptionMessage)
      }
    }
  }
}

