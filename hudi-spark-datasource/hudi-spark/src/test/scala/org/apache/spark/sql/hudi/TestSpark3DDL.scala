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

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{arrays_zip, col}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TestSpark3DDL extends HoodieSparkSqlTestBase {

  def createTestResult(tableName: String): Array[Row] = {
    spark.sql(s"select * from ${tableName} order by id")
      .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name").collect()
  }

  def createAndPreparePartitionTable(spark: SparkSession, tableName: String, tablePath: String, tableType: String): Unit = {
    // try to clean tablePath
    spark.sql(
      s"""
         |create table $tableName (
         |  id int, comb int, col0 int, col1 bigint, col2 float, col3 double, col4 decimal(10,4), col5 string, col6 date, col7 timestamp, col8 boolean, col9 binary, par date
         |) using hudi
         | location '$tablePath'
         | options (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  preCombineField = 'comb'
         | )
         | partitioned by (par)
             """.stripMargin)
    spark.sql(
      s"""
         | insert into $tableName values
         | (1,1,11,100001,101.01,1001.0001,100001.0001,'a000001',DATE'2021-12-25',TIMESTAMP'2021-12-25 12:01:01',true,X'a01',TIMESTAMP'2021-12-25'),
         | (2,2,12,100002,102.02,1002.0002,100002.0002,'a000002',DATE'2021-12-25',TIMESTAMP'2021-12-25 12:02:02',true,X'a02',TIMESTAMP'2021-12-25'),
         | (3,3,13,100003,103.03,1003.0003,100003.0003,'a000003',DATE'2021-12-25',TIMESTAMP'2021-12-25 12:03:03',false,X'a03',TIMESTAMP'2021-12-25'),
         | (4,4,14,100004,104.04,1004.0004,100004.0004,'a000004',DATE'2021-12-26',TIMESTAMP'2021-12-26 12:04:04',true,X'a04',TIMESTAMP'2021-12-26'),
         | (5,5,15,100005,105.05,1005.0005,100005.0005,'a000005',DATE'2021-12-26',TIMESTAMP'2021-12-26 12:05:05',false,X'a05',TIMESTAMP'2021-12-26')
         |""".stripMargin)
  }

  test("Test multi change data type") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSparkUtils.gteqSpark3_1) {
          spark.sql("set hoodie.schema.on.read.enable=true")
          // NOTE: This is required since as this tests use type coercions which were only permitted in Spark 2.x
          //       and are disallowed now by default in Spark 3.x
          spark.sql("set spark.sql.storeAssignmentPolicy=legacy")
          createAndPreparePartitionTable(spark, tableName, tablePath, tableType)
          // date -> string -> date
          spark.sql(s"alter table $tableName alter column col6 type String")
          checkAnswer(spark.sql(s"select col6 from $tableName where id = 1").collect())(
            Seq("2021-12-25")
          )
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,1,13.0,100001,101.01,1001.0001,100001.0001,'a000001','2021-12-26','2021-12-25 12:01:01',true,'a01','2021-12-25')
               |""".stripMargin)
          spark.sql(s"alter table $tableName alter column col6 type date")
          checkAnswer(spark.sql(s"select col6 from $tableName where id = 1 or id = 5 order by id").collect())(
            Seq(java.sql.Date.valueOf("2021-12-26")), // value from new file
            Seq(java.sql.Date.valueOf("2021-12-26"))  // value from old file
          )
          // int -> double -> decimal
          spark.sql(s"alter table $tableName alter column col0 type double")
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,1,13.0,100001,101.01,1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
               | (6,1,14.0,100001,101.01,1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25')
               |""".stripMargin)
          spark.sql(s"alter table $tableName alter column col0 type decimal(16, 4)")
          checkAnswer(spark.sql(s"select col0 from $tableName where id = 1 or id = 6 order by id").collect())(
            Seq(new java.math.BigDecimal("13.0000")),
            Seq(new java.math.BigDecimal("14.0000"))
          )
          // float -> double -> decimal
          spark.sql(s"alter table $tableName alter column col2 type double")
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,1,13.0,100001,901.01,1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
               | (6,1,14.0,100001,601.01,1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25')
               |""".stripMargin)
          spark.sql(s"alter table $tableName alter column col2 type decimal(16, 4)")
          checkAnswer(spark.sql(s"select col0, col2 from $tableName where id = 1 or id = 6 order by id").collect())(
            Seq(new java.math.BigDecimal("13.0000"), new java.math.BigDecimal("901.0100")),
            Seq(new java.math.BigDecimal("14.0000"), new java.math.BigDecimal("601.0100"))
          )
          // long -> double -> decimal
          spark.sql(s"alter table $tableName alter column col1 type double")
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,1,13.0,700001.0,901.01,1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25')
               |""".stripMargin)
          spark.sql(s"alter table $tableName alter column col1 type decimal(16, 4)")
          checkAnswer(spark.sql(s"select col0, col2, col1 from $tableName where id = 1 or id = 6 order by id").collect())(
            Seq(new java.math.BigDecimal("13.0000"), new java.math.BigDecimal("901.0100"), new java.math.BigDecimal("700001.0000")),
            Seq(new java.math.BigDecimal("14.0000"), new java.math.BigDecimal("601.0100"), new java.math.BigDecimal("100001.0000"))
          )
          spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
          spark.sessionState.catalog.refreshTable(TableIdentifier(tableName))
        }
      }
    }
  }

  test("Test multi change data type2") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSparkUtils.gteqSpark3_1) {
          spark.sql("set hoodie.schema.on.read.enable=true")
          // NOTE: This is required since as this tests use type coercions which were only permitted in Spark 2.x
          //       and are disallowed now by default in Spark 3.x
          spark.sql("set spark.sql.storeAssignmentPolicy=legacy")
          createAndPreparePartitionTable(spark, tableName, tablePath, tableType)
          // float -> double -> decimal -> String
          spark.sql(s"alter table $tableName alter column col2 type double")
          spark.sql(s"alter table $tableName alter column col2 type decimal(16, 4)")
          spark.sql(s"alter table $tableName alter column col2 type String")
          checkAnswer(spark.sql(s"select col2 from $tableName where id = 1").collect())(
            Seq("101.01")
          )
          // long -> double -> decimal -> string
          spark.sql(s"alter table $tableName alter column col1 type double")
          spark.sql(s"alter table $tableName alter column col1 type decimal(16, 4)")
          spark.sql(s"alter table $tableName alter column col1 type String")
          checkAnswer(spark.sql(s"select col1 from $tableName where id = 1").collect())(
            Seq("100001")
          )
          // int -> double -> decimal -> String
          spark.sql(s"alter table $tableName alter column col0 type double")
          spark.sql(s"alter table $tableName alter column col0 type decimal(16, 4)")
          spark.sql(s"alter table $tableName alter column col0 type String")
          checkAnswer(spark.sql(s"select col0 from $tableName where id = 1").collect())(
            Seq("11")
          )
        }
      }
    }
  }

  test("Test Partition Table alter ") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSparkUtils.gteqSpark3_1) {
          spark.sql("set hoodie.schema.on.read.enable=true")
          // NOTE: This is required since as this tests use type coercions which were only permitted in Spark 2.x
          //       and are disallowed now by default in Spark 3.x
          spark.sql("set spark.sql.storeAssignmentPolicy=legacy")
          createAndPreparePartitionTable(spark, tableName, tablePath, tableType)

          // test set properties
          spark.sql(s"alter table $tableName set tblproperties(comment='it is a hudi table', 'key1'='value1', 'key2'='value2')")
          val meta = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
          assert(meta.comment.get.equals("it is a hudi table"))
          assert(Seq("key1", "key2").filter(meta.properties.contains(_)).size == 2)
          // test unset propertes
          spark.sql(s"alter table $tableName unset tblproperties(comment, 'key1', 'key2')")
          val unsetMeta = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
          assert(Seq("key1", "key2").filter(unsetMeta.properties.contains(_)).size == 0)
          assert(unsetMeta.comment.isEmpty)
          // test forbidden operation.
          checkException(s"Alter table $tableName add columns(col_new1 int first)")("forbid adjust top-level columns position by using through first syntax")
          HoodieRecord.HOODIE_META_COLUMNS.subList(0, HoodieRecord.HOODIE_META_COLUMNS.size - 2).asScala.foreach {f =>
            checkException(s"Alter table $tableName add columns(col_new1 int after $f)")("forbid adjust the position of ordinary columns between meta columns")
          }
          Seq("id", "comb", "par").foreach { col =>
            checkException(s"alter table $tableName drop column $col")("cannot support apply changes for primaryKey/CombineKey/partitionKey")
            checkException(s"alter table $tableName rename column $col to ${col + col}")("cannot support apply changes for primaryKey/CombineKey/partitionKey")
          }
          // check duplicate add or rename
          // keep consistent with hive, column names insensitive
          checkExceptions(s"alter table $tableName rename column col0 to col9")(Seq("cannot rename column: col0 to a existing name",
            "Cannot rename column, because col9 already exists in root"))
          checkExceptions(s"alter table $tableName rename column col0 to COL9")(Seq("cannot rename column: col0 to a existing name", "Cannot rename column, because COL9 already exists in root"))
          checkExceptions(s"alter table $tableName add columns(col9 int first)")(Seq("cannot add column: col9 which already exist", "Cannot add column, because col9 already exists in root"))
          checkExceptions(s"alter table $tableName add columns(COL9 int first)")(Seq("cannot add column: COL9 which already exist", "Cannot add column, because COL9 already exists in root"))
          // test add comment for columns / alter columns comment
          spark.sql(s"alter table $tableName add columns(col1_new int comment 'add new columns col1_new after id' after id)")
          spark.sql(s"alter table $tableName alter column col9 comment 'col9 desc'")
          val schema = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).schema
          assert(schema.filter(p => p.name.equals("col1_new")).get(0).getComment().get == "add new columns col1_new after id")
          assert(schema.filter(p => p.name.equals("col9")).get(0).getComment().get == "col9 desc")
          // test change column type float to double
          spark.sql(s"alter table $tableName alter column col2 type double")
          spark.sql(s"select id, col1_new, col2 from $tableName where id = 1 or id = 2 order by id").show(false)
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,3,1,11,100001,101.01,1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
               | (6,6,5,15,100005,105.05,1005.0005,100005.0005,'a000005','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
               |""".stripMargin)

          spark.sql(s"select id, col1_new, col2 from $tableName where id = 1 or id = 6 or id = 2 order by id").show(false)
          // try schedule compact
          if (tableType == "mor") spark.sql(s"schedule compaction  on $tableName")
          // test change column type decimal(10,4) 为decimal(18,8)
          spark.sql(s"alter table $tableName alter column col4 type decimal(18, 8)")
          spark.sql(s"select id, col1_new, col2 from $tableName where id = 1 or id = 2 order by id").show(false)
          spark.sql(
            s"""
               | insert into $tableName values
               | (5,6,5,15,100005,105.05,1005.0005,100005.0005,'a000005','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
               |""".stripMargin)

          spark.sql(s"select id, col1_new, col4 from $tableName where id = 1 or id = 6 or id = 2 order by id").show(false)
          // test change column type float to double
          spark.sql(s"alter table $tableName alter column col2 type string")
          spark.sql(s"select id, col1_new, col2 from $tableName where id = 1 or id = 2 order by id").show(false)
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,3,1,11,100001,'101.01',1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
               | (6,6,5,15,100005,'105.05',1005.0005,100005.0005,'a000005','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
               |""".stripMargin)

          spark.sql(s"select id, col1_new, col2 from $tableName where id = 1 or id = 6 or id = 2 order by id").show(false)
          // try schedule compact
          if (tableType == "mor") spark.sql(s"schedule compaction  on $tableName")
          // if tableType is mor, check compaction
          if (tableType == "mor") {
            val compactionRows = spark.sql(s"show compaction on $tableName limit 10").collect()
            val timestamps = compactionRows.map(_.getString(0))
            assertResult(2)(timestamps.length)
            spark.sql(s"run compaction on $tableName at ${timestamps(1)}")
            spark.sql(s"run compaction on $tableName at ${timestamps(0)}")
          }
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,3,1,11,100001,'101.01',1001.0001,100009.0001,'a000008','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
               | (11,3,1,11,100001,'101.01',1001.0001,100011.0001,'a000008','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
               | (6,6,5,15,100005,'105.05',1005.0005,100007.0005,'a000009','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
               |""".stripMargin)

          spark.sql(s"select id, col1_new, col2 from $tableName where id = 1 or id = 6 or id = 2 or id = 11 order by id").show(false)
        }
      }
    }
  }

  test("Test Chinese table ") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSparkUtils.gteqSpark3_1) {
          spark.sql("set hoodie.schema.on.read.enable=true")
          spark.sql(
            s"""
               |create table $tableName (
               |  id int, comb int, `名字` string, col9 string, `成绩` int, `身高` float, `体重` double, `上次更新时间` date, par date
               |) using hudi
               | location '$tablePath'
               | options (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'comb'
               | )
               | partitioned by (par)
             """.stripMargin)
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,3,'李明', '读书', 100,180.0001,99.0001,'2021-12-25', '2021-12-26')
               |""".stripMargin)
          spark.sql(s"alter table $tableName rename column col9 to `爱好_Best`")

          // update current table to produce log files for mor
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,3,'李明', '读书', 100,180.0001,99.0001,'2021-12-26', '2021-12-26')
               |""".stripMargin)

          // alter date to string
          spark.sql(s"alter table $tableName alter column `上次更新时间` type string ")
          checkAnswer(spark.sql(s"select `上次更新时间` from $tableName").collect())(
            Seq("2021-12-26")
          )
          // alter string to date
          spark.sql(s"alter table $tableName alter column `上次更新时间` type date ")
          spark.sql(s"select `上次更新时间` from $tableName").collect()
          checkAnswer(spark.sql(s"select `上次更新时间` from $tableName").collect())(
            Seq(java.sql.Date.valueOf("2021-12-26"))
          )
        }
      }
    }
  }


  test("Test Alter Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSparkUtils.gteqSpark3_1) {
          spark.sql("set hoodie.schema.on.read.enable=true")
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | location '$tablePath'
               | options (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
             """.stripMargin)
          spark.sql(s"show create table ${tableName}").show(false)
          spark.sql(s"insert into ${tableName} values (1, 'jack', 0.9, 1000)")
          spark.sql(s"update ${tableName} set price = 1.9  where id =  1")

          spark.sql(s"alter table ${tableName} alter column id type long")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", 1.9, 1000)
          )
          // test add action, include position change
          spark.sql(s"alter table ${tableName} add columns(ext1 string comment 'add ext1' after name)")
          spark.sql(s"insert into ${tableName} values (2, 'jack', 'exx1', 0.9, 1000)")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", null, 1.9, 1000), Seq(2, "jack","exx1", 0.9, 1000)
          )
          // test rename
          spark.sql(s"alter table ${tableName} rename column price to newprice")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", null, 1.9, 1000), Seq(2, "jack","exx1", 0.9, 1000)
          )
          spark.sql(s"update ${tableName} set ext1 =  'haha' where id =  1 ")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", "haha", 1.9, 1000), Seq(2, "jack","exx1", 0.9, 1000)
          )
          // drop column newprice

          spark.sql(s"alter table ${tableName} drop column newprice")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", "haha", 1000), Seq(2, "jack","exx1", 1000)
          )
          // add newprice back
          spark.sql(s"alter table ${tableName} add columns(newprice string comment 'add newprice back' after ext1)")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", "haha", null, 1000), Seq(2, "jack","exx1", null, 1000)
          )
        }
      }
    }
  }

  test("Test Alter Table complex") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSparkUtils.gteqSpark3_1) {
          spark.sql("set hoodie.schema.on.read.enable=true")
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  members map<String, struct<n:string, a:int>>,
               |  user struct<name:string, age:int, score: int>,
               |  ts long
               |) using hudi
               | location '$tablePath'
               | options (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
             """.stripMargin)

          spark.sql(s"alter table $tableName alter column members.value.a first")

          spark.sql(s"insert into ${tableName} values(1, 'jack', map('k1', struct(100, 'v1'), 'k2', struct(200, 'v2')), struct('jackStruct', 29, 100), 1000)")

          // rename column
          spark.sql(s"alter table ${tableName} rename column user to userx")

          checkAnswer(spark.sql(s"select ts, userx.score, id, userx.age, name from ${tableName}").collect())(
            Seq(1000, 100, 1, 29, "jack")
          )

          // drop column
          spark.sql(s"alter table ${tableName} drop columns(name, userx.name, userx.score)")

          spark.sql(s"select * from ${tableName}").show(false)

          // add cols back, and adjust cols position
          spark.sql(s"alter table ${tableName} add columns(name string comment 'add name back' after userx," +
            s" userx.name string comment 'add userx.name back' first, userx.score int comment 'add userx.score back' after age)")

          // query new columns: name, userx.name, userx.score, those field should not be readed.
          checkAnswer(spark.sql(s"select name, userx.name, userx.score from ${tableName}").collect())(Seq(null, null, null))

          // insert again
          spark.sql(s"insert into ${tableName} values(2 , map('k1', struct(100, 'v1'), 'k2', struct(200, 'v2')), struct('jackStructNew', 291 , 101), 'jacknew', 1000)")

          // check again
          checkAnswer(spark.sql(s"select name, userx.name as uxname, userx.score as uxs from ${tableName} order by id").collect())(
            Seq(null, null, null),
            Seq("jacknew", "jackStructNew", 101))


          spark.sql(s"alter table ${tableName} alter column userx.age type long")

          spark.sql(s"select userx.age, id, name from ${tableName}")
          checkAnswer(spark.sql(s"select userx.age, id, name from ${tableName} order by id").collect())(
            Seq(29, 1, null),
            Seq(291, 2, "jacknew"))
          // test map value type change
          spark.sql(s"alter table ${tableName} add columns(mxp map<String, int>)")
          spark.sql(s"insert into ${tableName} values(2, map('k1', struct(100, 'v1'), 'k2', struct(200, 'v2')), struct('jackStructNew', 291 , 101), 'jacknew', 1000, map('t1', 9))")
          spark.sql(s"alter table ${tableName} alter column mxp.value type double")
          spark.sql(s"insert into ${tableName} values(2, map('k1', struct(100, 'v1'), 'k2', struct(200, 'v2')), struct('jackStructNew', 291 , 101), 'jacknew', 1000, map('t1', 10))")
          spark.sql(s"select * from $tableName").show(false)
          checkAnswer(spark.sql(s"select mxp from ${tableName} order by id").collect())(
            Seq(null),
            Seq(Map("t1" -> 10.0d))
          )
          spark.sql(s"alter table ${tableName} rename column members to mem")
          spark.sql(s"alter table ${tableName} rename column mem.value.n to nn")
          spark.sql(s"alter table ${tableName} rename column userx to us")
          spark.sql(s"alter table ${tableName} rename column us.age to age1")

          spark.sql(s"insert into ${tableName} values(2, map('k1', struct(100, 'v1'), 'k2', struct(200, 'v2')), struct('jackStructNew', 291 , 101), 'jacknew', 1000, map('t1', 10))")
          spark.sql(s"select mem.value.nn, us.age1 from $tableName order by id").show()
          checkAnswer(spark.sql(s"select mem.value.nn, us.age1 from $tableName order by id").collect())(
            Seq(null, 29),
            Seq(null, 291)
          )
        }
      }
    }
  }

  test("Test schema auto evolution") {
    withTempDir { tmp =>
      Seq("COPY_ON_WRITE", "MERGE_ON_READ").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        if (HoodieSparkUtils.gteqSpark3_1) {

          val dataGen = new HoodieTestDataGenerator
          val schema = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA
          val records1 = RawTripTestPayload.recordsToStrings(dataGen.generateInsertsAsPerSchema("001", 1000, schema)).toList
          val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
          // drop tip_history.element.amount, city_to_state, distance_in_meters, drivers
          val orgStringDf = inputDF1.drop("city_to_state", "distance_in_meters", "drivers")
            .withColumn("tip_history", arrays_zip(col("tip_history.currency")))
          spark.sql("set hoodie.schema.on.read.enable=true")

          val hudiOptions = Map[String,String](
            HoodieWriteConfig.TABLE_NAME -> tableName,
            DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> tableType,
            DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
            DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
            DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
            "hoodie.schema.on.read.enable" -> "true",
            "hoodie.datasource.write.reconcile.schema" -> "true",
            DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY -> "true"
          )

          orgStringDf.write
            .format("org.apache.hudi")
            .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
            .options(hudiOptions)
            .mode(SaveMode.Overwrite)
            .save(tablePath)

          val oldView = spark.read.format("hudi").load(tablePath)
          oldView.show(false)

          val records2 = RawTripTestPayload.recordsToStrings(dataGen.generateUpdatesAsPerSchema("002", 100, schema)).toList
          val inputD2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
          val updatedStringDf = inputD2.drop("fare").drop("height")
          val checkRowKey = inputD2.select("_row_key").collectAsList().map(_.getString(0)).get(0)

          updatedStringDf.write
            .format("org.apache.hudi")
            .options(hudiOptions)
            .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
            .option("hoodie.datasource.write.reconcile.schema", "true")
            .mode(SaveMode.Append)
            .save(tablePath)
          spark.read.format("hudi").load(tablePath).registerTempTable("newView")
          val checkResult = spark.sql(s"select tip_history.amount,city_to_state,distance_in_meters,fare,height from newView where _row_key='$checkRowKey' ")
            .collect().map(row => (row.isNullAt(0), row.isNullAt(1), row.isNullAt(2), row.isNullAt(3), row.isNullAt(4)))
          assertResult((false, false, false, true, true))(checkResult(0))
          checkAnswer(spark.sql(s"select fare,height from newView where _row_key='$checkRowKey'").collect())(
            Seq(null, null)
          )
        }
      }
    }
  }
}
