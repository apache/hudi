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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger, QuickstartUtils}
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.index.inmemory.HoodieInMemoryHashIndex
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{arrays_zip, col, expr, lit}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertEquals

import scala.Seq
import scala.collection.JavaConverters._

class TestSpark3DDL extends HoodieSparkSqlTestBase {

  def createTestResult(tableName: String): Array[Row] = {
    spark.sql(s"select * from $tableName order by id")
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
         |  orderingFields = 'comb'
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

  test("Test alter column types") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        spark.sql("set " + DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key + "=upsert")
        spark.sql("set hoodie.schema.on.read.enable=true")
        spark.sql("set hoodie.metadata.index.column.stats.enable=false")
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
        spark.sessionState.conf.unsetConf(DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key)
        spark.sessionState.conf.unsetConf("spark.sql.storeAssignmentPolicy")
      }
      spark.sessionState.conf.unsetConf("unset hoodie.metadata.index.column.stats.enable")
    })
  }

  test("Test alter column types 2") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
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
        spark.sessionState.conf.unsetConf("spark.sql.storeAssignmentPolicy")
      }
    }
  }

  test("Test float to double evolution") {
    withTempDir { tmp =>
      Seq(HoodieTableType.COPY_ON_WRITE, HoodieTableType.MERGE_ON_READ).foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        spark.sql("set hoodie.schema.on.read.enable=false")

        val structType = StructType(Array(
          StructField("id", StringType, true),
          StructField("ts", IntegerType, true),
          StructField("partition", StringType, true),
          StructField("col", FloatType, true)
        ))

        val data = Seq(Row("r1", 0, "p1", 1.01f), Row("r2", 0, "p1", 2.02f), Row("r3", 0, "p2", 3.03f))
        val rowRdd: RDD[Row] = spark.sparkContext.parallelize(data)
        val df = spark.createDataFrame(rowRdd, structType)
        df.write.format("hudi")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key(), "ts")
          .option("hoodie.datasource.write.partitionpath.field", "partition")
          .option("hoodie.table.name", tableName)
          .option("hoodie.datasource.write.table.type", tableType.name())
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        checkAnswer(spark.read.format("hudi").load(tablePath).select("id", "col").orderBy("id").collect())(
          Seq("r1", 1.01f),
          Seq("r2", 2.02f),
          Seq("r3", 3.03f)
        )


        val data2 = Seq(Row("r2", 1, "p1", 2.03f))
        val rowRdd2: RDD[Row] = spark.sparkContext.parallelize(data2)
        val df2 = spark.createDataFrame(rowRdd2, structType)
        df2.write.format("hudi")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key(), "ts")
          .option("hoodie.datasource.write.partitionpath.field", "partition")
          .option("hoodie.table.name", tableName)
          .option("hoodie.datasource.write.table.type", tableType.name())
          .mode(SaveMode.Append)
          .save(tablePath)


        checkAnswer(spark.read.format("hudi").load(tablePath).select("id", "col").orderBy("id").collect())(
          Seq("r1", 1.01f),
          Seq("r2", 2.03f),
          Seq("r3", 3.03f)
        )

        val structType3 = StructType(Array(
          StructField("id", StringType, true),
          StructField("ts", IntegerType, true),
          StructField("partition", StringType, true),
          StructField("col", DoubleType, true)
        ))

        val data3 = Seq(Row("r1", 2, "p1", 1.000000000001d))
        val rowRdd3: RDD[Row] = spark.sparkContext.parallelize(data3)
        val df3 = spark.createDataFrame(rowRdd3, structType3)
        df3.write.format("hudi")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key(), "ts")
          .option("hoodie.datasource.write.partitionpath.field", "partition")
          .option("hoodie.table.name", tableName)
          .option("hoodie.datasource.write.table.type", tableType.name())
          .mode(SaveMode.Append)
          .save(tablePath)

        checkAnswer(spark.read.format("hudi").load(tablePath).select("id", "col").orderBy("id").collect())(
          Seq("r1", 1.000000000001d),
          Seq("r2", 2.03d),
          Seq("r3", 3.03d)
        )
      }
    }
  }

  test("Test Enable and Disable Schema on read") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql("set hoodie.schema.on.read.enable=true")
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
       """.stripMargin)

        // Insert data to the new table.
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        checkAnswer(s"select id, name, price, ts from $tableName")(
          Seq(1, "a1", 10.0, 1000)
        )

        // add column
        spark.sql(s"alter table $tableName add columns(new_col string)")
        val catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult(Seq("id", "name", "price", "ts", "new_col")) {
          HoodieSqlCommonUtils.removeMetaFields(catalogTable.schema).fields.map(_.name)
        }
        checkAnswer(s"select id, name, price, ts, new_col from $tableName")(
          Seq(1, "a1", 10.0, 1000, null)
        )
        // disable schema on read.
        spark.sql("set hoodie.schema.on.read.enable=false")
        spark.sql(s"refresh table $tableName")
        // Insert data to the new table.
        spark.sql(s"insert into $tableName values(2, 'a2', 12, 2000, 'e0')")
        // write should succeed. and subsequent read should succeed as well.
        checkAnswer(s"select id, name, price, ts, new_col from $tableName")(
          Seq(1, "a1", 10.0, 1000, null),
          Seq(2, "a2", 12.0, 2000, "e0")
        )
      }
    }
  }

  test("Test alter table properties and add rename drop column with table services") {
    withTempDir { tmp =>
      Seq("cow,clustering", "mor,compaction", "mor,clustering").foreach { args =>
        val argArray = args.split(',')
        val tableType = argArray(0)
        val runCompaction = "compaction".equals(argArray(1))
        val runClustering = "clustering".equals(argArray(1))
        val isMor = "mor".equals(tableType)
        assert(runCompaction || runClustering)

        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        // disable automatic inline compaction
        spark.sql("set hoodie.compact.inline=false")
        spark.sql("set hoodie.compact.schedule.inline=false")

        spark.sql("set hoodie.schema.on.read.enable=true")
        spark.sql("set hoodie.metadata.index.column.stats.enable=false")
        spark.sql("set " + DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key + "=upsert")
        // NOTE: This is required since as this tests use type coercions which were only permitted in Spark 2.x
        //       and are disallowed now by default in Spark 3.x
        spark.sql("set spark.sql.storeAssignmentPolicy=legacy")
        createAndPreparePartitionTable(spark, tableName, tablePath, tableType)

        // test set properties
        spark.sql(s"alter table $tableName set tblproperties(comment='it is a hudi table', 'key1'='value1', 'key2'='value2')")
        val meta = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
        assert(meta.comment.get.equals("it is a hudi table"))
        assert(Seq("key1", "key2").filter(meta.properties.contains(_)).size == 2)
        // test unset properties
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
          checkException(s"alter table $tableName drop column $col")("cannot support apply changes for primaryKey/orderingFields/partitionKey")
          checkException(s"alter table $tableName rename column $col to ${col + col}")("cannot support apply changes for primaryKey/orderingFields/partitionKey")
        }
        // check duplicate add or rename
        // keep consistent with hive, column names insensitive
        checkExceptions(s"alter table $tableName rename column col0 to col9")(Seq("Cannot rename column 'col0' to 'col9' because a column with name 'col9' already exists in the schema"))
        checkExceptions(s"alter table $tableName rename column col0 to COL9")(Seq("Cannot rename column 'col0' to 'COL9' because a column with name 'COL9' already exists in the schema"))
        checkExceptions(s"alter table $tableName add columns(col9 int first)")(Seq("Cannot add column 'col9' because it already exists in the schema"))
        checkExceptions(s"alter table $tableName add columns(COL9 int first)")(Seq("Cannot add column 'COL9' because it already exists in the schema"))
        // test add comment for columns / alter columns comment
        spark.sql(s"alter table $tableName add columns(col1_new int comment 'add new columns col1_new after id' after id)")
        spark.sql(s"alter table $tableName alter column col9 comment 'col9 desc'")
        val schema = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).schema
        assert(schema.filter(p => p.name.equals("col1_new")).asJava.get(0).getComment().get == "add new columns col1_new after id")
        assert(schema.filter(p => p.name.equals("col9")).asJava.get(0).getComment().get == "col9 desc")
        // test change column type float to double
        spark.sql(s"alter table $tableName alter column col2 type double")
        checkAnswer(s"select id, col1_new, col2 from $tableName where id = 1 or id = 2 order by id")(
          Seq(1, null, 101.01),
          Seq(2, null, 102.02))
        spark.sql(
          s"""
             | insert into $tableName values
             | (1,3,1,11,100001,101.01,1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
             | (6,6,5,15,100005,105.05,1005.0005,100005.0005,'a000005','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
             |""".stripMargin)

        val allExpectedRows = Seq(
          Seq(1, 3, 1, 11, 100001L, 101.01, 1001.0001,
            new java.math.BigDecimal("100001.0001"), "a000001", java.sql.Date.valueOf("2021-12-25"),
            java.sql.Timestamp.valueOf("2021-12-25 12:01:01"), true,
            java.sql.Date.valueOf("2021-12-25")),
          Seq(2, null, 2, 12, 100002L, 102.02, 1002.0002,
            new java.math.BigDecimal("100002.0002"), "a000002", java.sql.Date.valueOf("2021-12-25"),
            java.sql.Timestamp.valueOf("2021-12-25 12:02:02"), true,
            java.sql.Date.valueOf("2021-12-25")),
          Seq(3, null, 3, 13, 100003L, 103.03, 1003.0003,
            new java.math.BigDecimal("100003.0003"), "a000003", java.sql.Date.valueOf("2021-12-25"),
            java.sql.Timestamp.valueOf("2021-12-25 12:03:03"), false,
            java.sql.Date.valueOf("2021-12-25")),
          Seq(4, null, 4, 14, 100004L, 104.04, 1004.0004,
            new java.math.BigDecimal("100004.0004"), "a000004", java.sql.Date.valueOf("2021-12-26"),
            java.sql.Timestamp.valueOf("2021-12-26 12:04:04"), true,
            java.sql.Date.valueOf("2021-12-26")),
          Seq(5, null, 5, 15, 100005L, 105.05, 1005.0005,
            new java.math.BigDecimal("100005.0005"), "a000005", java.sql.Date.valueOf("2021-12-26"),
            java.sql.Timestamp.valueOf("2021-12-26 12:05:05"), false,
            java.sql.Date.valueOf("2021-12-26")),
          Seq(6, 6, 5, 15, 100005L, 105.05, 1005.0005,
            new java.math.BigDecimal("100005.0005"), "a000005", java.sql.Date.valueOf("2021-12-26"),
            java.sql.Timestamp.valueOf("2021-12-26 12:05:05"), false,
            java.sql.Date.valueOf("2021-12-26")))

        checkAnswer(s"select id, col1_new, comb, col0, col1, col2, col3, col4, col5, "
          + s"col6, col7, col8, par from $tableName")(allExpectedRows: _*)
        if (runCompaction) {
          // try schedule compact
          if (tableType == "mor") spark.sql(s"schedule compaction on $tableName")
        } else if (runClustering) {
          assertEquals(0, spark.sql(s"CALL show_clustering('$tableName')").count)
          spark.sql(s"CALL run_clustering(table => '$tableName', op => 'schedule')")
          spark.sql(s"CALL run_clustering(table => '$tableName', op => 'execute')")
          val clusteringRows = spark.sql(s"CALL show_clustering('$tableName')").collect()
          assertResult(1)(clusteringRows.length)
          val states = clusteringRows.map(_.getString(2))
          assertResult(HoodieInstant.State.COMPLETED.name())(states(0))
        }
        // Data should not change after scheduling or running table services
        checkAnswer(s"select id, col1_new, comb, col0, col1, col2, col3, col4, col5, "
          + s"col6, col7, col8, par from $tableName")(allExpectedRows: _*)
        // test change column type decimal(10,4) to decimal(18,8)
        spark.sql(s"alter table $tableName alter column col4 type decimal(18, 8)")
        checkAnswer(s"select id, col1_new, comb, col0, col1, col2, col3, col4, col5, "
          + s"col6, col7, col8, par from $tableName")(allExpectedRows: _*)
        spark.sql(
          s"""
             | insert into $tableName values
             | (5,6,5,15,100005,105.05,1005.0005,100005.0005,'a000005','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
             |""".stripMargin)

        checkAnswer(s"select id, col1_new, col4 from $tableName "
          + s"where id = 1 or id = 6 or id = 2 order by id")(
          Seq(1, 3, new java.math.BigDecimal("100001.00010000")),
          Seq(2, null, new java.math.BigDecimal("100002.00020000")),
          Seq(6, 6, new java.math.BigDecimal("100005.00050000")))

        // test change column type float to double
        spark.sql(s"alter table $tableName alter column col2 type string")
        checkAnswer(s"select id, col1_new, col2 from $tableName where id = 1 or id = 2 order by id")(
          Seq(1, 3, "101.01"),
          Seq(2, null, "102.02"))
        spark.sql(
          s"""
             | insert into $tableName values
             | (1,3,1,11,100001,'101.01',1001.0001,100001.0001,'a000001','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
             | (6,6,5,15,100005,'105.05',1005.0005,100005.0005,'a000005','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
             |""".stripMargin)

        checkAnswer(s"select id, col1_new, comb, col0, col1, col2, col3, col4, col5, "
          + s"col6, col7, col8, par from $tableName")(getExpectedRowsSecondTime(): _*)
        if (runCompaction) {
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
        } else if (runClustering) {
          spark.sql(s"CALL run_clustering(table => '$tableName', op => 'schedule')")
          spark.sql(s"CALL run_clustering(table => '$tableName', op => 'execute')")
          val clusteringRows = spark.sql(s"CALL show_clustering('$tableName')").collect()
          assertResult(2)(clusteringRows.length)
          val states = clusteringRows.map(_.getString(2))
          assertResult(HoodieInstant.State.COMPLETED.name())(states(0))
          assertResult(HoodieInstant.State.COMPLETED.name())(states(1))
        }
        // Data should not change after scheduling or running table services
        checkAnswer(s"select id, col1_new, comb, col0, col1, col2, col3, col4, col5, "
          + s"col6, col7, col8, par from $tableName")(getExpectedRowsSecondTime(): _*)
        spark.sql(
          s"""
             | insert into $tableName values
             | (1,3,1,11,100001,'101.01',1001.0001,100009.0001,'a000008','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
             | (11,3,1,11,100001,'101.01',1001.0001,100011.0001,'a000008','2021-12-25','2021-12-25 12:01:01',true,'a01','2021-12-25'),
             | (6,6,5,15,100005,'105.05',1005.0005,100007.0005,'a000009','2021-12-26','2021-12-26 12:05:05',false,'a05','2021-12-26')
             |""".stripMargin)
        checkAnswer(s"select id, col1_new, col2 from $tableName "
          + s"where id = 1 or id = 6 or id = 2 or id = 11 order by id")(
          Seq(1, 3, "101.01"),
          Seq(11, 3, "101.01"),
          Seq(2, null, "102.02"),
          Seq(6, 6, "105.05"))
      }
      spark.sessionState.conf.unsetConf("spark.sql.storeAssignmentPolicy")
      spark.sessionState.conf.unsetConf(DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key)
      spark.sessionState.conf.unsetConf("unset hoodie.metadata.index.column.stats.enable")
    }
  }

  private def getExpectedRowsSecondTime(): Seq[Seq[Any]] = {
    Seq(
      Seq(1, 3, 1, 11, 100001L, "101.01", 1001.0001, new java.math.BigDecimal("100001.00010000"),
        "a000001", java.sql.Date.valueOf("2021-12-25"),
        java.sql.Timestamp.valueOf("2021-12-25 12:01:01"), true,
        java.sql.Date.valueOf("2021-12-25")),
      Seq(2, null, 2, 12, 100002L, "102.02",
        1002.0002, new java.math.BigDecimal("100002.00020000"),
        "a000002", java.sql.Date.valueOf("2021-12-25"),
        java.sql.Timestamp.valueOf("2021-12-25 12:02:02"), true,
        java.sql.Date.valueOf("2021-12-25")),
      Seq(3, null, 3, 13, 100003L, "103.03",
        1003.0003, new java.math.BigDecimal("100003.00030000"),
        "a000003", java.sql.Date.valueOf("2021-12-25"),
        java.sql.Timestamp.valueOf("2021-12-25 12:03:03"), false,
        java.sql.Date.valueOf("2021-12-25")),
      Seq(4, null, 4, 14, 100004L, "104.04", 1004.0004, new java.math.BigDecimal("100004.00040000"),
        "a000004", java.sql.Date.valueOf("2021-12-26"),
        java.sql.Timestamp.valueOf("2021-12-26 12:04:04"), true,
        java.sql.Date.valueOf("2021-12-26")),
      Seq(5, 6, 5, 15, 100005L, "105.05", 1005.0005, new java.math.BigDecimal("100005.00050000"),
        "a000005", java.sql.Date.valueOf("2021-12-26"),
        java.sql.Timestamp.valueOf("2021-12-26 12:05:05"), false,
        java.sql.Date.valueOf("2021-12-26")),
      Seq(6, 6, 5, 15, 100005L, "105.05", 1005.0005, new java.math.BigDecimal("100005.00050000"),
        "a000005", java.sql.Date.valueOf("2021-12-26"),
        java.sql.Timestamp.valueOf("2021-12-26 12:05:05"), false,
        java.sql.Date.valueOf("2021-12-26")))
  }

  test("Test Chinese table ") {
    withRecordType()(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withSparkSqlSessionConfig(
          "hoodie.schema.on.read.enable" -> "true",
          "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true",
          DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key -> "upsert") {
          val tableName = generateTableName
          val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int, comb int, `名字` string, col9 string, `成绩` int, `身高` float, `体重` double, `上次更新时间` date, par date
               |) using hudi
               | location '$tablePath'
               | options (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  orderingFields = 'comb'
               | )
               | partitioned by (par)
             """.stripMargin)
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,3,'李明', '读书', 100,180.0001,99.0001,DATE'2021-12-25', DATE'2021-12-26')
               |""".stripMargin)
          spark.sql(s"alter table $tableName rename column col9 to `爱好_Best`")

          // update current table to produce log files for mor
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,3,'李明', '读书', 100,180.0001,99.0001,DATE'2021-12-26', DATE'2021-12-26')
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
        spark.sessionState.conf.unsetConf(DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key)
      }
    })
  }


  test("Test alter column by add rename and drop") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withSparkSqlSessionConfig(
          "hoodie.schema.on.read.enable" -> "true",
          "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true") {
          val tableName = generateTableName
          val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
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
               |  orderingFields = 'ts'
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
            Seq(1, "jack", null, 1.9, 1000), Seq(2, "jack", "exx1", 0.9, 1000)
          )
          // test rename
          spark.sql(s"alter table ${tableName} rename column price to newprice")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", null, 1.9, 1000), Seq(2, "jack", "exx1", 0.9, 1000)
          )
          spark.sql(s"update ${tableName} set ext1 =  'haha' where id =  1 ")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", "haha", 1.9, 1000), Seq(2, "jack", "exx1", 0.9, 1000)
          )
          var maxColumnId = getMaxColumnId(tablePath)
          // drop column newprice
          spark.sql(s"alter table ${tableName} drop column newprice")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", "haha", 1000), Seq(2, "jack", "exx1", 1000)
          )
          validateInternalSchema(tablePath, isDropColumn = true, currentMaxColumnId = maxColumnId)
          maxColumnId = getMaxColumnId(tablePath)
          // add newprice back
          spark.sql(s"alter table ${tableName} add columns(newprice string comment 'add newprice back' after ext1)")
          checkAnswer(createTestResult(tableName))(
            Seq(1, "jack", "haha", null, 1000), Seq(2, "jack", "exx1", null, 1000)
          )
          validateInternalSchema(tablePath, isDropColumn = false, currentMaxColumnId = maxColumnId)
        }
      }
    }
  }

  def validateInternalSchema(basePath: String, isDropColumn: Boolean, currentMaxColumnId: Int): Unit = {
    val metaClient = createMetaClient(spark, basePath)
    val schema = new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata.get()
    val lastInstant = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get()
    assert(schema.schemaId() == lastInstant.requestedTime.toLong)
    if (isDropColumn) {
      assert(schema.getMaxColumnId == currentMaxColumnId)
    } else {
      assert(schema.getMaxColumnId == currentMaxColumnId + 1)
    }
  }

  def getMaxColumnId(basePath: String): Int = {
    val metaClient = createMetaClient(spark, basePath)
    new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata.get.getMaxColumnId
  }

  test("Test alter column nullability") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        spark.sql("set hoodie.schema.on.read.enable=true")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string not null,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | options (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
             """.stripMargin)
        spark.sql(s"alter table $tableName alter column name drop not null")
        spark.sql(s"insert into $tableName values(1, null, 1000)")
        checkAnswer(s"select id, name, ts from $tableName")(Seq(1, null, 1000))
      }
    }
  }

  test("Test alter column multiple times") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withSparkSqlSessionConfig(
          "hoodie.schema.on.read.enable" -> "true",
          "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true") {
          val tableName = generateTableName
          val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  col1 string,
               |  col2 string,
               |  ts long
               |) using hudi
               | location '$tablePath'
               | options (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  orderingFields = 'ts'
               | )
             """.stripMargin)
          spark.sql(s"show create table ${tableName}").show(false)
          spark.sql(s"insert into ${tableName} values (1, 'aaa', 'bbb', 1000)")

          // Rename to a previously existing column name + insert
          spark.sql(s"alter table ${tableName} drop column col1")
          spark.sql(s"alter table ${tableName} rename column col2 to col1")

          spark.sql(s"insert into ${tableName} values (2, 'aaa', 1000)")
          checkAnswer(spark.sql(s"select col1 from ${tableName} order by id").collect())(
            Seq("bbb"), Seq("aaa")
          )
        }
      }
    }
  }

  test("Test alter column with complex schema") {
    withTempDir { tmp =>
      withSQLConf(DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION.key -> "upsert",
        "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true",
        "hoodie.schema.on.read.enable" -> "true",
        "spark.sql.parquet.enableNestedColumnVectorizedReader" -> "false") {
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
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
             |  type = 'mor',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
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

        // query new columns: name, userx.name, userx.score, those field should not be read.
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

  test("Test schema auto evolution complex") {
    withRecordType()(withTempDir { tmp =>
      Seq("COPY_ON_WRITE", "MERGE_ON_READ").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        val dataGen = new HoodieTestDataGenerator
        val schema = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA
        val records1 = HoodieTestDataGenerator.recordsToStrings(dataGen.generateInsertsAsPerSchema("001", 1000, schema)).asScala.toList
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
          DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY -> "true"
        )

        val (writeOpt, readOpt) = getWriterReaderOpts(HoodieRecordType.SPARK, hudiOptions)
        orgStringDf.write
          .format("org.apache.hudi")
          .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
          .options(writeOpt)
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        val oldView = spark.read.format("hudi").options(readOpt)
            .load(tablePath)
        oldView.show(5, false)

        val records2 = HoodieTestDataGenerator.recordsToStrings(dataGen.generateInsertsAsPerSchema("002", 100, schema)).asScala.toList
        val inputD2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
        val updatedStringDf = inputD2.drop("fare").drop("height")
        val checkRowKey = inputD2.select("_row_key").collectAsList().asScala.map(_.getString(0)).head

        updatedStringDf.write
          .format("org.apache.hudi")
          .options(writeOpt)
          .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
          .option("hoodie.datasource.write.reconcile.schema", "true")
          .mode(SaveMode.Append)
          .save(tablePath)
        spark.read.format("hudi").options(readOpt)
            .load(tablePath).registerTempTable("newView")
        val checkResult = spark.sql(s"select tip_history.amount,city_to_state,distance_in_meters,fare,height from newView where _row_key='$checkRowKey' ")
          .collect().map(row => (row.isNullAt(0), row.isNullAt(1), row.isNullAt(2), row.isNullAt(3), row.isNullAt(4)))
        assertResult((false, false, false, true, true))(checkResult(0))
        checkAnswer(spark.sql(s"select fare,height from newView where _row_key='$checkRowKey'").collect())(
          Seq(null, null)
        )
      }
    })
  }

  val sparkOpts = Map(
    HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName,
    HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet"
  )

  def getWriterReaderOpts(recordType: HoodieRecordType, opt: Map[String, String]): (Map[String, String], Map[String, String]) = {
    recordType match {
      case HoodieRecordType.SPARK => (opt ++ sparkOpts, sparkOpts)
      case _ => (opt, Map.empty[String, String])
    }
  }

  test("Test schema auto evolution") {
    withTempDir { tmp =>
      Seq("COPY_ON_WRITE").foreach { tableType =>
        // for complex schema.
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        val dataGen = new QuickstartUtils.DataGenerator
        val inserts = QuickstartUtils.convertToStringList(dataGen.generateInserts(10)).asScala.toSeq
        val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
          .withColumn("ts", lit("20240404000000")) // to make test determinate for HOODIE_AVRO_DEFAULT payload
        df.write.format("hudi").
          options(QuickstartUtils.getQuickstartWriteConfigs).
          option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, tableType).
          option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
          option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uuid").
          option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
          option("hoodie.schema.on.read.enable","true").
          option(DataSourceWriteOptions.TABLE_NAME.key(), tableName).
          mode("overwrite").
          save(tablePath)

        val updates = QuickstartUtils.convertToStringList(dataGen.generateUpdates(10)).asScala.toSeq
        // type change: fare (double -> String)
        // add new column and drop a column
        val dfUpdate = spark.read.json(spark.sparkContext.parallelize(updates, 2))
          .withColumn("fare", expr("cast(fare as string)"))
          .withColumn("addColumn", lit("new"))
          .withColumn("ts", lit("20240404000005")) // to make test determinate for HOODIE_AVRO_DEFAULT payload
        dfUpdate.drop("begin_lat").write.format("hudi").
          options(QuickstartUtils.getQuickstartWriteConfigs).
          option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, tableType).
          option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
          option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uuid").
          option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
          option("hoodie.schema.on.read.enable","true").
          option("hoodie.datasource.write.reconcile.schema","true").
          option(DataSourceWriteOptions.TABLE_NAME.key(), tableName).
          mode("append").
          save(tablePath)
        spark.sql("set hoodie.schema.on.read.enable=true")

        val snapshotDF = spark.read.format("hudi").load(tablePath)

        assertResult(StringType)(snapshotDF.schema.fields.filter(_.name == "fare").head.dataType)
        assertResult("addColumn")(snapshotDF.schema.fields.last.name)
        val checkRowKey = dfUpdate.select("fare").collectAsList().asScala.map(_.getString(0)).head
        snapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
        checkAnswer(spark.sql(s"select fare, addColumn from  hudi_trips_snapshot where fare = ${checkRowKey}").collect())(
          Seq(checkRowKey, "new")
        )

        spark.sql(s"select * from  hudi_trips_snapshot").show(false)
        //  test insert_over_write  + update again
        val overwrite = QuickstartUtils.convertToStringList(dataGen.generateInserts(10)).asScala.toSeq
        val dfOverWrite = spark.
          read.json(spark.sparkContext.parallelize(overwrite, 2)).
          filter("partitionpath = 'americas/united_states/san_francisco'")
          .withColumn("ts", lit("20240404000010")) // to make test determinate for HOODIE_AVRO_DEFAULT payload
          .withColumn("fare", expr("cast(fare as string)")) // fare now in table is string type, we forbid convert string to double.
        dfOverWrite.write.format("hudi").
          options(QuickstartUtils.getQuickstartWriteConfigs).
          option("hoodie.datasource.write.operation","insert_overwrite").
          option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
          option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uuid").
          option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
          option("hoodie.schema.on.read.enable","true").
          option("hoodie.datasource.write.reconcile.schema","true").
          option(DataSourceWriteOptions.TABLE_NAME.key(), tableName).
          mode("append").
          save(tablePath)
        spark.read.format("hudi").load(tablePath).show(false)

        val updatesAgain = QuickstartUtils.convertToStringList(dataGen.generateUpdates(10)).asScala.toSeq
        val dfAgain = spark.read.json(spark.sparkContext.parallelize(updatesAgain, 2)).
          withColumn("fare", expr("cast(fare as string)")).
          withColumn("ts", lit("20240404000015")) // to make test determinate for HOODIE_AVRO_DEFAULT payload
        dfAgain.write.format("hudi").
          options(QuickstartUtils.getQuickstartWriteConfigs).
          option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts").
          option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uuid").
          option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
          option("hoodie.schema.on.read.enable","true").
          option("hoodie.datasource.write.reconcile.schema","true").
          option(DataSourceWriteOptions.TABLE_NAME.key(), tableName).
          mode("append").
          save(tablePath)
        spark.read.format("hudi").load(tablePath).createOrReplaceTempView("hudi_trips_snapshot1")
        val checkKey = dfAgain.select("fare").collectAsList().asScala.map(_.getString(0)).head
        checkAnswer(spark.sql(s"select fare, addColumn from  hudi_trips_snapshot1 where fare = ${checkKey}").collect())(
          Seq(checkKey, null)
        )
      }
    }
  }

  test("Test DATE to STRING conversions when vectorized reading is not enabled") {
    withTempDir { tmp =>
      Seq("COPY_ON_WRITE", "MERGE_ON_READ").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${new Path(tmp.getCanonicalPath, tableName).toUri.toString}"
        // adding a struct column to force reads to use non-vectorized readers
        spark.sql(
          s"""
             | create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  struct_col struct<f0: int, f1: string>,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | options (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
             | partitioned by (ts)
             """.stripMargin)
        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 10, struct(1, 'f_1'), 1000)
              """.stripMargin)
        spark.sql(s"select * from $tableName")

        spark.sql("set hoodie.schema.on.read.enable=true")
        spark.sql(s"alter table $tableName add columns(date_to_string_col date)")
        spark.sql(
          s"""
             | insert into $tableName
             | values (2, 'a2', 20, struct(2, 'f_2'), date '2023-03-22', 1001)
              """.stripMargin)
        spark.sql(s"alter table $tableName alter column date_to_string_col type string")

        // struct and string (converted from date) column must be read to ensure that non-vectorized reader is used
        // not checking results as we just need to ensure that the table can be read without any errors thrown
        spark.sql(s"select * from $tableName")
      }
    }
  }

  test("Test FLOAT to DECIMAL schema evolution (lost in scale)") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        // Using INMEMORY index for mor table so that log files will be created instead of parquet
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price float,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey = 'id',
             |  type = '$tableType',
             |  orderingFields = 'ts'
             |  ${if (tableType.equals("mor")) ", hoodie.index.type = 'INMEMORY'" else ""}
             | )
           """.stripMargin)

        spark.sql(s"insert into $tableName values (1, 'a1', 10.024, 1000)")

        assertResult(tableType.equals("mor"))(DataSourceTestUtils.isLogFileOnly(tmp.getCanonicalPath))

        spark.sql("set hoodie.schema.on.read.enable=true")
        spark.sql(s"alter table $tableName alter column price type decimal(4, 2)")

        // Not checking answer as this is an unsafe casting operation, just need to make sure that error is not thrown
        spark.sql(s"select id, name, cast(price as string), ts from $tableName")

        // clear after using INMEMORY index
        HoodieInMemoryHashIndex.clear()
      }
    }
  }

  test("Test DOUBLE or STRING to DECIMAL schema evolution (lost in scale)") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        // Using INMEMORY index for mor table so that log files will be created instead of parquet
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}'
             | tblproperties (
             |  primaryKey = 'id',
             |  type = '$tableType',
             |  orderingFields = 'ts'
             |  ${if (tableType.equals("mor")) ", hoodie.index.type = 'INMEMORY'" else ""}
             | )
           """.stripMargin)

        spark.sql(s"insert into $tableName values " +
          // testing the rounding behaviour to ensure that HALF_UP is used for positive values
          "(1, 'a1', 10.024, 1000)," +
          "(2, 'a2', 10.025, 1000)," +
          "(3, 'a3', 10.026, 1000)," +
          // testing the rounding behaviour to ensure that HALF_UP is used for negative values
          "(4, 'a4', -10.024, 1000)," +
          "(5, 'a5', -10.025, 1000)," +
          "(6, 'a6', -10.026, 1000)," +
          // testing the GENERAL rounding behaviour (HALF_UP and HALF_EVEN will retain the same result)
          "(7, 'a7', 10.034, 1000)," +
          "(8, 'a8', 10.035, 1000)," +
          "(9, 'a9', 10.036, 1000)," +
          // testing the GENERAL rounding behaviour (HALF_UP and HALF_EVEN will retain the same result)
          "(10, 'a10', -10.034, 1000)," +
          "(11, 'a11', -10.035, 1000)," +
          "(12, 'a12', -10.036, 1000)")

        assertResult(tableType.equals("mor"))(DataSourceTestUtils.isLogFileOnly(tmp.getCanonicalPath))

        spark.sql("set hoodie.schema.on.read.enable=true")
        spark.sql(s"alter table $tableName alter column price type decimal(4, 2)")

        checkAnswer(s"select id, name, cast(price as string), ts from $tableName order by id")(
          Seq(1, "a1", "10.02", 1000),
          Seq(2, "a2", "10.03", 1000),
          Seq(3, "a3", "10.03", 1000),
          Seq(4, "a4", "-10.02", 1000),
          Seq(5, "a5", "-10.03", 1000),
          Seq(6, "a6", "-10.03", 1000),
          Seq(7, "a7", "10.03", 1000),
          Seq(8, "a8", "10.04", 1000),
          Seq(9, "a9", "10.04", 1000),
          Seq(10, "a10", "-10.03", 1000),
          Seq(11, "a11", "-10.04", 1000),
          Seq(12, "a12", "-10.04", 1000)
        )

        spark.sql(s"alter table $tableName alter column price type decimal(4, 2)")

        checkAnswer(s"select id, name, cast(price as string), ts from $tableName order by id")(
          Seq(1, "a1", "10.02", 1000),
          Seq(2, "a2", "10.03", 1000),
          Seq(3, "a3", "10.03", 1000),
          Seq(4, "a4", "-10.02", 1000),
          Seq(5, "a5", "-10.03", 1000),
          Seq(6, "a6", "-10.03", 1000),
          Seq(7, "a7", "10.03", 1000),
          Seq(8, "a8", "10.04", 1000),
          Seq(9, "a9", "10.04", 1000),
          Seq(10, "a10", "-10.03", 1000),
          Seq(11, "a11", "-10.04", 1000),
          Seq(12, "a12", "-10.04", 1000)
        )

        // clear after using INMEMORY index
        HoodieInMemoryHashIndex.clear()
      }
    }
  }

  test("Test extract partition values from path when schema evolution is enabled") {
    Seq("cow", "mor").foreach { tableType =>
      withTable(generateTableName) { tableName =>
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | ts bigint,
             | region string,
             | dt date
             |) using hudi
             |tblproperties (
             | primaryKey = 'id',
             | type = '$tableType',
             | orderingFields = 'ts'
             |)
             |partitioned by (region, dt)""".stripMargin)

        withSQLConf("hoodie.datasource.read.extract.partition.values.from.path" -> "true",
          "hoodie.schema.on.read.enable" -> "true") {
          spark.sql(s"insert into $tableName partition (region='reg1', dt='2023-10-01') " +
            s"select 1, 'name1', 1000")
          checkAnswer(s"select id, name, ts, region, cast(dt as string) from $tableName where region='reg1'")(
            Seq(1, "name1", 1000, "reg1", "2023-10-01")
          )

          // apply schema evolution and perform a read again
          spark.sql(s"alter table $tableName add columns(price double)")
          checkAnswer(s"select id, name, ts, region, cast(dt as string) from $tableName where region='reg1'")(
            Seq(1, "name1", 1000, "reg1", "2023-10-01")
          )

          // ensure this won't be broken in the future
          // BooleanSimplification is always applied when calling HoodieDataSourceHelper#getNonPartitionFilters
          checkAnswer(s"select id, name, ts, region, cast(dt as string) from $tableName where not(region='reg2' or id=2)")(
            Seq(1, "name1", 1000, "reg1", "2023-10-01")
          )
        }
      }
    }
  }
}
