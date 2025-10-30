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

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.internal.SQLConf

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
      spark.sql("set hoodie.merge.allow.duplicate.on.inserts=true")
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

  test("Test Query With PK Filter") {
    withTable(generateTableName) { tableName =>
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
           |  type = 'mor',
           |  primaryKey = 'id,name',
           |  preCombineField = 'ts',
           |  'hoodie.index.type' = 'BUCKET',
           |  'hoodie.bucket.index.num.buckets' = '1',
           |  '${HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key()}' = 'parquet'
           | )
           | partitioned by (dt)
          """.stripMargin
      )
      spark.conf.unset("hoodie.datasource.insert.dup.policy")

      withSQLConf("hoodie.datasource.overwrite.mode" -> "dynamic") {
        spark.sql(
          s"""
             | insert overwrite table $tableName partition(dt) values
             | (0, 'a0', 10, 1000, '2023-12-06'),
             | (1, 'a1', 10, 1000, '2023-12-06'),
             | (2, 'a2', 10, 1000, '2023-12-06'),
             | (3, 'a3', 10, 1000, '2023-12-06')
          """.stripMargin)
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(0, "a0", 10.0, 1000, "2023-12-06"),
          Seq(1, "a1", 10.0, 1000, "2023-12-06"),
          Seq(2, "a2", 10.0, 1000, "2023-12-06"),
          Seq(3, "a3", 10.0, 1000, "2023-12-06")
        )
      }
      withSQLConf("hoodie.datasource.write.operation" -> "upsert") {
        spark.sql(
          s"""
             | insert into table $tableName partition (dt='2023-12-06') values
             | (1, 'a1', 11, 2000),
             | (4, 'a4', 10, 1000)
            """.stripMargin)
      }

      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(0, "a0", 10.0, 1000, "2023-12-06"),
        Seq(1, "a1", 11.0, 2000, "2023-12-06"),
        Seq(2, "a2", 10.0, 1000, "2023-12-06"),
        Seq(3, "a3", 10.0, 1000, "2023-12-06"),
        Seq(4, "a4", 10.0, 1000, "2023-12-06")
      )

      withSQLConf(s"${SQLConf.PARQUET_RECORD_FILTER_ENABLED.key}" -> "true") {
        checkAnswer(s"select price, ts, dt from $tableName where (id = 1 or name = 'a3') and price <> 10")(
          Seq(11.0, 2000, "2023-12-06")
        )
        // Filter(id = 1) and Filter(name = 'a3') can be push down, Filter(price <> 10) can't be push down since it's not primary key
        val df = spark.sql(s"select price, ts, dt from $tableName where (id = 1 or name = 'a3') and price <> 10")
        // only execute file scan physical plan
        // expected in file scan only (id: 1), (id: 3) and (id: 4) matched, (id: 3) and (id: 4)  matched but will be filtered later
        assertResult(3)(df.queryExecution.sparkPlan.children(0).children(0).executeCollect().length)
      }

      withSQLConf(s"${SQLConf.PARQUET_RECORD_FILTER_ENABLED.key}" -> "false") {
        spark.sql(s"set ${SQLConf.PARQUET_RECORD_FILTER_ENABLED.key}=false")
        checkAnswer(s"select price, ts, dt from $tableName where (id = 1 or name = 'a3') and price <> 10")(
          Seq(11.0, 2000, "2023-12-06")
        )
        val df = spark.sql(s"select price, ts, dt from $tableName where (id = 1 or name = 'a3') and price <> 10")
        // only execute file scan physical plan
        // expected all ids in the table are scanned, and filtered later
        assertResult(5)(df.queryExecution.sparkPlan.children(0).children(0).executeCollect().length)
      }

    }

  }
}

