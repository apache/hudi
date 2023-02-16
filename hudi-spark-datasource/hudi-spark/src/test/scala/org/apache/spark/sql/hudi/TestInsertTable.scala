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

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.WriteOperationType
import org.apache.spark.sql.hudi.command.HoodieSparkValidateDuplicateKeyRecordMerger
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase.getLastCommitMetadata
import org.scalatest.Inspectors.forAll

import java.io.File

class TestInsertTable extends HoodieSparkSqlTestBase {

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

      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05"),
        Seq(2, "a2", 20.0, 2000, "2021-01-06"),
        Seq(3, "a3", 30.0, 3000, "2021-01-07")
      )
    })
  }

  test("Test Insert Into with static partition") {
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
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tmp.getCanonicalPath)
        .setConf(spark.sessionState.newHadoopConf())
        .build()
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
    })
  }

  test("Test Insert Into with dynamic partition") {
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

      // Insert into dynamic partition
      spark.sql(
        s"""
           | insert into $tableName partition(dt)
           | select 1 as id, '2021-01-05' as dt, 'a1' as name, 10 as price, 1000 as ts
        """.stripMargin)
      // should not mess with the original order after write the out-of-order data.
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tmp.getCanonicalPath)
        .setConf(spark.sessionState.newHadoopConf())
        .build()
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
    })
  }

  test("Test Insert Into with multi partition") {
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
           | tblproperties (primaryKey = 'id')
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

  test("Test Insert Into None Partitioned Table") {
   withRecordType(Map(HoodieRecordType.SPARK ->
     // SparkMerger should use "HoodieSparkValidateDuplicateKeyRecordMerger"
     // with "hoodie.sql.insert.mode=strict"
     Map(HoodieWriteConfig.RECORD_MERGER_IMPLS.key ->
       classOf[HoodieSparkValidateDuplicateKeyRecordMerger].getName)))(withTempDir { tmp =>
     val tableName = generateTableName
     spark.sql(s"set hoodie.sql.insert.mode=strict")
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
     spark.sql("set hoodie.datasource.write.insert.drop.duplicates = true")
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
     spark.sql("set hoodie.datasource.write.insert.drop.duplicates = false")
     spark.sql(s"set hoodie.sql.insert.mode=upsert")
   })
  }

  test("Test Insert Into None Partitioned Table strict mode with no preCombineField") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(s"set hoodie.sql.insert.mode=strict")
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
    withRecordType()(withTempDir { tmp =>
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
           | tblproperties (primaryKey = 'id')
           | partitioned by (dt)
           | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

      //  Insert overwrite table
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName")(
        Seq(1, "a1", 10.0, 1000, "2021-01-05")
      )

      //  Insert overwrite table
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-01-06' as dt
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
        Seq(2, "a2", 10.0, 1000, "2021-01-06")
      )

      // Insert overwrite static partition
      spark.sql(
        s"""
           | insert overwrite table $tableName partition(dt = '2021-01-05')
           | select * from (select 2 , 'a2', 12, 1000) limit 10
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
        Seq(2, "a2", 12.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-06")
      )

      // Insert data from another table
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
      spark.sql(
        s"""
           | insert overwrite table $tableName partition(dt ='2021-01-04')
           | select * from $tblNonPartition limit 10
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName order by id,dt")(
        Seq(1, "a1", 10.0, 1000, "2021-01-04"),
        Seq(2, "a2", 12.0, 1000, "2021-01-05"),
        Seq(2, "a2", 10.0, 1000, "2021-01-06")
      )

      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select id + 2, name, price, ts , '2021-01-04' from $tblNonPartition limit 10
        """.stripMargin)
      checkAnswer(s"select id, name, price, ts, dt from $tableName " +
        s"where dt >='2021-01-04' and dt <= '2021-01-06' order by id,dt")(
        Seq(3, "a1", 10.0, 1000, "2021-01-04")
      )

      // Test insert overwrite non-partitioned table
      spark.sql(s"insert overwrite table $tblNonPartition select 2, 'a2', 10, 1000")
      checkAnswer(s"select id, name, price, ts from $tblNonPartition")(
        Seq(2, "a2", 10.0, 1000)
      )

      spark.sql(s"insert overwrite table $tblNonPartition select 3, 'a3', 10, 1000")
      checkAnswer(s"select id, name, price, ts from $tblNonPartition")(
        Seq(3, "a3", 10.0, 1000)
      )
    })
  }

  test("Test Different Type of Partition Column") {
   withRecordType()(withTempDir { tmp =>
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
   })
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
    withRecordType()(withTempDir{ tmp =>
      val tableName = s"H_$generateTableName"

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
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tmp.getCanonicalPath)
        .setConf(spark.sessionState.newHadoopConf())
        .build()
      assertResult(metaClient.getTableConfig.getTableName)(tableName)
    })
  }

  test("Test Insert Exception") {
    withRecordType() {
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
      checkExceptionContain(s"insert into $tableName partition(dt = '2021-06-20') select 1, 'a1', 10, '2021-06-20'") (
        """
          |too many data columns:
          |Table columns: 'id', 'name', 'price'
          |Data columns: '1', 'a1', '10', '2021-06-20'
          |""".stripMargin
      )
      checkExceptionContain(s"insert into $tableName select 1, 'a1', 10")(
        """
          |not enough data columns:
          |Table columns: 'id', 'name', 'price', 'dt'
          |Data columns: '1', 'a1', '10'
          |""".stripMargin
      )
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

        spark.sql("set hoodie.sql.insert.mode = non-strict")
        val tableName3 = generateTableName
        spark.sql(
          s"""
             |create table $tableName3 (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             |) using hudi
             | tblproperties (primaryKey = 'id')
             | partitioned by (dt)
       """.stripMargin)
        checkException(s"insert overwrite table $tableName3 partition(dt = '2021-07-18') values(1, 'a1', 10, '2021-07-18')")(
          "Insert Overwrite Partition can not use bulk insert."
        )
      }
    }
  }


  test("Test Insert timestamp when 'spark.sql.datetime.java8API.enabled' enables") {
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

  test("Test bulk insert") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict") {
      withRecordType()(withTempDir { tmp =>
        Seq("cow", "mor").foreach {tableType =>
          // Test bulk insert for single partition
          val tableName = generateTableName
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

          // Test bulk insert for multi-level partition
          val tableMultiPartition = generateTableName
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

          // Enable the bulk insert
          spark.sql("set hoodie.sql.bulk.insert.enable = true")
          spark.sql(s"insert into $tableMultiPartition values(1, 'a1', 10, '2021-07-18', '12')")

          checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition")(
            Seq(1, "a1", 10.0, "2021-07-18", "12")
          )
          // Disable the bulk insert
          spark.sql("set hoodie.sql.bulk.insert.enable = false")
          spark.sql(s"insert into $tableMultiPartition " +
            s"values(2, 'a2', 10, '2021-07-18','12')")

          checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition order by id")(
            Seq(1, "a1", 10.0, "2021-07-18", "12"),
            Seq(2, "a2", 10.0, "2021-07-18", "12")
          )
          // Test bulk insert for non-partitioned table
          val nonPartitionedTable = generateTableName
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
          spark.sql("set hoodie.sql.bulk.insert.enable = true")
          spark.sql(s"insert into $nonPartitionedTable values(1, 'a1', 10)")
          checkAnswer(s"select id, name, price from $nonPartitionedTable")(
            Seq(1, "a1", 10.0)
          )
          spark.sql(s"insert overwrite table $nonPartitionedTable values(2, 'a2', 10)")
          checkAnswer(s"select id, name, price from $nonPartitionedTable")(
            Seq(2, "a2", 10.0)
          )
          spark.sql("set hoodie.sql.bulk.insert.enable = false")

          // Test CTAS for bulk insert
          val tableName2 = generateTableName
          spark.sql(
            s"""
               |create table $tableName2
               |using hudi
               |tblproperties(
               | type = '$tableType',
               | primaryKey = 'id'
               |)
               | location '${tmp.getCanonicalPath}/$tableName2'
               | as
               | select * from $tableName
               |""".stripMargin)
          checkAnswer(s"select id, name, price, dt from $tableName2 order by id")(
            Seq(1, "a1", 10.0, "2021-07-18"),
            Seq(2, "a2", 10.0, "2021-07-18")
          )
        }
      })
    }
  }

  test("Test combine before insert") {
    withSQLConf("set hoodie.sql.bulk.insert.enable" -> "false") {
      withRecordType()(withTempDir{tmp =>
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

  test("Test insert pk-table") {
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
      withRecordType()(withTempDir{tmp =>
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
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "day,hh")
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key, "true")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(spark.sessionState.newHadoopConf())
          .build()

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

  test("Test Insert Into With Catalog Identifier for spark >= 3.2.0") {
    Seq("hudi", "parquet").foreach { format =>
      withRecordType()(withTempDir { tmp =>
        val tableName = s"spark_catalog.default.$generateTableName"
        // Create a partitioned table
        if (HoodieSparkUtils.gteqSpark3_2) {
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
      })
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
    spark.sql(s"insert into $tableName select 2, 'a2', 10, $partitionValue")
    checkAnswer(s"select id, name, price, cast(dt as string) from $tableName order by id")(
      Seq(1, "a1", 10, extractRawValue(partitionValue).toString),
      Seq(2, "a2", 10, extractRawValue(partitionValue).toString)
    )
  }

  test("Test enable hoodie.merge.allow.duplicate.on.inserts when write") {
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
}
