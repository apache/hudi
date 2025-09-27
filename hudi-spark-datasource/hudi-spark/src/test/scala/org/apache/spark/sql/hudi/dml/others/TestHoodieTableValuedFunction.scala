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

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionStatsIndexKey
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.hudi.testutils.DataSourceTestUtils

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestHoodieTableValuedFunction extends HoodieSparkSqlTestBase {

  test(s"Test hudi_query Table-Valued Function") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        spark.sql("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + "=upsert")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |location '${tmp.getCanonicalPath}/$tableName'
             |""".stripMargin
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
             | """.stripMargin
        )

        checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 1000),
          Seq(3, "a3", 30.0, 1000)
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1_1', 10, 1100), (2, 'a2_2', 20, 1100), (3, 'a3_3', 30, 1100)
             | """.stripMargin
        )

        if (tableType == "cow") {
          checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
            Seq(1, "a1_1", 10.0, 1100),
            Seq(2, "a2_2", 20.0, 1100),
            Seq(3, "a3_3", 30.0, 1100)
          )
        } else {
          checkAnswer(s"select id, name, price, ts from hudi_query('$tableName', 'read_optimized')")(
            Seq(1, "a1", 10.0, 1000),
            Seq(2, "a2", 20.0, 1000),
            Seq(3, "a3", 30.0, 1000)
          )
        }
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }

  test(s"Test hudi_table_changes latest_state") {
    withTempDir { tmp =>
      Seq(
        ("cow", true),
        ("mor", true),
        ("cow", false),
        ("mor", false)
      ).foreach { parameters =>
        val tableType = parameters._1
        val isTableId = parameters._2
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        val identifier = if (isTableId) tableName else tablePath
        spark.sql("set hoodie.sql.insert.mode = non-strict")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |location '$tablePath'
             |""".stripMargin
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
             | """.stripMargin
        )

        val fs = HadoopFSUtils.getFs(tablePath, spark.sessionState.newHadoopConf())
        val firstCompletionTime = DataSourceTestUtils.latestCommitCompletionTime(fs, tablePath)

        checkAnswer(
          s"""select id,
             |name,
             |price,
             |ts
             |from hudi_table_changes('$identifier', 'latest_state', 'earliest')
             |""".stripMargin
        )(
          Seq(1, "a1", 10.0, 1000),
          Seq(2, "a2", 20.0, 1000),
          Seq(3, "a3", 30.0, 1000)
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1_1', 10, 1100), (2, 'a2_2', 20, 1100), (3, 'a3_3', 30, 1100)
             | """.stripMargin
        )
        val secondCompletionTime = DataSourceTestUtils.latestCommitCompletionTime(fs, tablePath)

        checkAnswer(
          s"""select id,
             |name,
             |price,
             |ts
             |from hudi_table_changes(
             |'$identifier',
             |'latest_state',
             |'$firstCompletionTime')
             |""".stripMargin
        )(
          Seq(1, "a1_1", 10.0, 1100),
          Seq(2, "a2_2", 20.0, 1100),
          Seq(3, "a3_3", 30.0, 1100)
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1_1', 10, 1200), (2, 'a2_2', 20, 1200), (3, 'a3_3', 30, 1200)
             | """.stripMargin
        )

        // should not include the first and latest instant
        checkAnswer(
          s"""select id,
             | name,
             | price,
             | ts
             | from hudi_table_changes(
             | '$identifier',
             | 'latest_state',
             | '$firstCompletionTime',
             | '$secondCompletionTime')
             | """.stripMargin
        )(
          Seq(1, "a1_1", 10.0, 1100),
          Seq(2, "a2_2", 20.0, 1100),
          Seq(3, "a3_3", 30.0, 1100)
        )
      }
    }
  }

  test(s"Test hudi_filesystem_view") {
    withTempDir { tmp =>
      Seq(
        ("cow", true),
        ("mor", true),
        ("cow", false),
        ("mor", false)
      ).foreach { parameters =>
        val tableType = parameters._1
        val isTableId = parameters._2

        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        val identifier = if (isTableId) tableName else tablePath
        spark.sql("set hoodie.sql.insert.mode = non-strict")

        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double
             |) using hudi
             |partitioned by (price)
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id'
             |)
             |location '$tablePath'
             |""".stripMargin
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 10.0), (2, 'a2', 20.0), (3, 'a3', 30.0)
             | """.stripMargin
        )
        spark.sql(
          s"""
             | insert into $tableName
             | values (4, 'a4', 10.0), (5, 'a5', 20.0), (6, 'a6', 30.0)
             | """.stripMargin
        )
        val result1DF = spark.sql(s"select * from hudi_filesystem_view('$identifier', 'price*')")
        val result1Array = result1DF.select(
          col("Partition_Path")
        ).orderBy("Partition_Path").take(10)
        checkAnswer(result1Array)(
          Seq("price=10.0"),
          Seq("price=10.0"),
          Seq("price=20.0"),
          Seq("price=20.0"),
          Seq("price=30.0"),
          Seq("price=30.0")
        )
      }
    }
  }

  test(s"Test hudi_table_changes cdc") {
    withTempDir { tmp =>
      Seq(
        ("cow", true),
        ("mor", true),
        ("cow", false),
        ("mor", false)
      ).foreach { parameters =>
        val tableType = parameters._1
        val isTableId = parameters._2
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        val identifier = if (isTableId) tableName else tablePath
        spark.sql("set hoodie.sql.insert.mode = upsert")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  'hoodie.table.cdc.enabled' = 'true',
             |  'hoodie.table.cdc.supplemental.logging.mode' = 'data_before_after'
             |)
             |location '$tablePath'
             |""".stripMargin
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
             | """.stripMargin
        )
        val originSchema = spark.read.format("hudi").load(tablePath).schema
        val firstInstant = spark.sql(s"select min(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

        val cdcDataOnly1 = spark.sql(
          s"""select
             | op,
             | before,
             | after
             |from hudi_table_changes('$identifier', 'cdc', 'earliest')
             |""".stripMargin
        )

        val change1 = cdcDataOnly1.select(
          col("op"),
          col("before"),
          from_json(col("after"), originSchema).as("after")
        ).select(
          col("op"),
          col("before"),
          col("after.id"),
          col("after.name"),
          col("after.price"),
          col("after.ts")
        ).orderBy("after.id").take(10)
        checkAnswer(change1)(
          Seq("i", null, 1, "a1", 10.0, 1000),
          Seq("i", null, 2, "a2", 20.0, 1000),
          Seq("i", null, 3, "a3", 30.0, 1000)
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1_1', 10, 1100), (2, 'a2_2', 20, 1100), (3, 'a3_3', 30, 1100)
             | """.stripMargin
        )
        val secondInstant = spark.sql(s"select max(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

        val cdcDataOnly2 = spark.sql(
          s"""select
             | op,
             | before,
             | after
             |from hudi_table_changes(
             |'$identifier',
             |'cdc',
             |'$firstInstant')
             |""".stripMargin
        )

        val change2 = cdcDataOnly2.select(
          col("op"),
          from_json(col("before"), originSchema).as("before"),
          from_json(col("after"), originSchema).as("after")
        ).select(
          col("op"),
          col("before.id"),
          col("before.name"),
          col("before.price"),
          col("before.ts"),
          col("after.id"),
          col("after.name"),
          col("after.price"),
          col("after.ts")
        ).orderBy("after.id").take(10)
        checkAnswer(change2)(
          Seq("u", 1, "a1", 10.0, 1000, 1, "a1_1", 10.0, 1100),
          Seq("u", 2, "a2", 20.0, 1000, 2, "a2_2", 20.0, 1100),
          Seq("u", 3, "a3", 30.0, 1000, 3, "a3_3", 30.0, 1100)
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1_1', 11, 1200), (2, 'a2_2', 21, 1200), (3, 'a3_3', 31, 1200)
             | """.stripMargin
        )

        // should not include the first and latest instant
        val cdcDataOnly3 = spark.sql(
          s"""select
             | op,
             | before,
             | after
             | from hudi_table_changes(
             | '$identifier',
             | 'cdc',
             | '$firstInstant',
             | '$secondInstant')
             | """.stripMargin
        )

        val change3 = cdcDataOnly3.select(
          col("op"),
          from_json(col("before"), originSchema).as("before"),
          from_json(col("after"), originSchema).as("after")
        ).select(
          col("op"),
          col("before.id"),
          col("before.name"),
          col("before.price"),
          col("before.ts"),
          col("after.id"),
          col("after.name"),
          col("after.price"),
          col("after.ts")
        ).orderBy("after.id").take(10)
        checkAnswer(change3)(
          Seq("u", 1, "a1", 10.0, 1000, 1, "a1_1", 10.0, 1100),
          Seq("u", 2, "a2", 20.0, 1000, 2, "a2_2", 20.0, 1100),
          Seq("u", 3, "a3", 30.0, 1000, 3, "a3_3", 30.0, 1100)
        )
      }
    }
  }

  test(s"Test hudi_query_timeline") {
    withTempDir { tmp =>
      Seq(
        ("cow", true),
        ("mor", true),
        ("cow", false),
        ("mor", false)
      ).foreach { parameters =>
        val tableType = parameters._1
        val isTableId = parameters._2
        val action = tableType match {
          case "cow" => "commit"
          case "mor" => "deltacommit"
        }
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        val identifier = if (isTableId) tableName else tablePath
        spark.sql("set hoodie.sql.insert.mode = non-strict")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |location '$tablePath'
             |""".stripMargin
        )

        spark.sql("set hoodie.merge.allow.duplicate.on.inserts = false")
        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000), (3, 'a3', 30, 1000)
             | """.stripMargin
        )

        val result1Array = spark.sql(s"select * from hudi_query_timeline('$identifier', 'true')").select(
          col("action"),
          col("state"),
          col("total_files_updated"),
          col("total_partitions_written"),
          col("total_records_written"),
          col("total_updated_records_written"),
          col("total_write_errors")
        ).orderBy("timestamp").take(10)
        checkAnswer(result1Array)(
          Seq(action, "COMPLETED", 0, 1, 3, 0, 0)
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (4, 'a4', 10, 1100), (5, 'a2_2', 20, 1100), (6, 'a3_3', 30, 1100)
             | """.stripMargin
        )

        val result2DF = spark.sql(s"select * from hudi_query_timeline('$identifier', 'false')")
        val result2Array = result2DF.select(
          col("action"),
          col("state"),
          col("total_files_updated"),
          col("total_partitions_written"),
          col("total_records_written"),
          col("total_updated_records_written"),
          col("total_write_errors")
        ).orderBy("timestamp").take(10)
        checkAnswer(result2Array)(
          Seq(action, "COMPLETED", 0, 1, 3, 0, 0),
          Seq(action, "COMPLETED", 1, 1, 6, 0, 0)
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1_1', 10, 1200), (2, 'a2_2', 20, 1200), (3, 'a3_3', 30, 1200)
             | """.stripMargin
        )

        val result3DF = spark.sql(s"select * from hudi_query_timeline('$identifier', 'true')")
        val result3Array = result3DF.select(
          col("action"),
          col("state"),
          col("total_files_updated"),
          col("total_partitions_written"),
          col("total_records_written"),
          col("total_updated_records_written"),
          col("total_write_errors")
        ).orderBy("timestamp").take(10)
        checkAnswer(result3Array)(
          Seq(action, "COMPLETED", 0, 1, 3, 0, 0),
          Seq(action, "COMPLETED", 1, 1, 6, 0, 0),
          Seq(action, "COMPLETED", 1, 1, 6, 3, 0)
        )

        val result4DF = spark.sql(
          s"select action, state from hudi_query_timeline('$identifier', 'true') where total_files_updated > 0 "
        )
        val result4Array = result4DF.take(10)
        checkAnswer(result4Array)(
          Seq(action, "COMPLETED"),
          Seq(action, "COMPLETED")
        )

        val result5DF = spark.sql(
          s"select * from hudi_query_timeline('$identifier', 'false')  where action = '$action'"
        )
        val result5Array = result5DF.select(
          col("action"),
          col("state"),
          col("total_files_updated"),
          col("total_partitions_written"),
          col("total_records_written"),
          col("total_updated_records_written"),
          col("total_write_errors")
        ).orderBy("timestamp").take(10)
        checkAnswer(result5Array)(
          Seq(action, "COMPLETED", 0, 1, 3, 0, 0),
          Seq(action, "COMPLETED", 1, 1, 6, 0, 0),
          Seq(action, "COMPLETED", 1, 1, 6, 3, 0)
        )


        val result6DF = spark.sql(
          s"select action, state from hudi_query_timeline('$identifier', 'false')  where timestamp > '202312190000000'"
        )
        val result6Array = result6DF.take(10)
        checkAnswer(result6Array)(
          Seq(action, "COMPLETED"),
          Seq(action, "COMPLETED"),
          Seq(action, "COMPLETED")
        )
      }
    }
  }

  test(s"Test hudi_metadata Table-Valued Function") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val identifier = tableName
        spark.sql("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + "=upsert")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  ts long,
             |  price int
             |) using hudi
             |partitioned by (price)
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.datasource.write.recordkey.field = 'id',
             |  hoodie.metadata.record.index.enable = 'true',
             |  hoodie.metadata.index.column.stats.enable = 'true',
             |  hoodie.metadata.index.column.stats.column.list = 'price'
             |)
             |location '${tmp.getCanonicalPath}/$tableName'
             |""".stripMargin
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 1000, 10), (2, 'a2', 2000, 20), (3, 'a3', 3000, 30)
             | """.stripMargin
        )

        val result2DF = spark.sql(
          s"select type, key, filesystemmetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.ALL_PARTITIONS.getRecordType}"
        )
        assert(result2DF.count() == 1)

        val result3DF = spark.sql(
          s"select type, key, filesystemmetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.FILES.getRecordType}"
        )
        assert(result3DF.count() == 3)

        val result4DF = spark.sql(
          s"select type, key, ColumnStatsMetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.COLUMN_STATS.getRecordType}"
        )
        // 3 meta columns are always indexed so 3 stats per column * (3 meta cols + 1 data col) = 12
        assert(result4DF.count() == 12)

        val result5DF = spark.sql(
          s"select type, key, recordIndexMetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.RECORD_INDEX.getRecordType}"
        )
        assert(result5DF.count() == 3)

        val result6DF = spark.sql(
          s"select type, key, BloomFilterMetadata from hudi_metadata('$identifier') where BloomFilterMetadata is not null"
        )
        assert(result6DF.count() == 0)

        // partition stats enabled by default
        val result7DF = spark.sql(
          s"select type, key, ColumnStatsMetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}"
        )
        assert(result7DF.count() == 12)
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }

  test(s"Test hudi_metadata Table-Valued Function For PARTITION_STATS index") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val identifier = tableName
        spark.sql("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + "=upsert")
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
             |  preCombineField = 'ts',
             |  hoodie.datasource.write.recordkey.field = 'id',
             |  hoodie.metadata.index.partition.stats.enable = 'true',
             |  hoodie.metadata.index.column.stats.enable = 'true',
             |  hoodie.metadata.index.column.stats.column.list = 'price',
             |  hoodie.populate.meta.fields = 'false'
             |)
             |location '${tmp.getCanonicalPath}/$tableName'
             |""".stripMargin
        )

        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 1000, 10), (2, 'a2', 2000, 20), (3, 'a3', 3000, 30), (4, 'a4', 2000, 10), (5, 'a5', 3000, 20), (6, 'a6', 4000, 30)
             | """.stripMargin
        )

        val result2DF = spark.sql(
          s"select type, key, filesystemmetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.ALL_PARTITIONS.getRecordType}"
        )
        assert(result2DF.count() == 1)

        val result3DF = spark.sql(
          s"select type, key, filesystemmetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.FILES.getRecordType}"
        )
        assert(result3DF.count() == 3)

        val result4DF = spark.sql(
          s"select * from hudi_metadata('$identifier') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}"
        )
        assert(result4DF.count() == 3)
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value from hudi_metadata('$identifier') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}")(
          Seq(getPartitionStatsIndexKey("ts=10", "price"), 1000),
          Seq(getPartitionStatsIndexKey("ts=20", "price"), 2000),
          Seq(getPartitionStatsIndexKey("ts=30", "price"), 3000)
        )
        checkAnswer(s"select key, ColumnStatsMetadata.maxValue.member1.value from hudi_metadata('$identifier') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}")(
          Seq(getPartitionStatsIndexKey("ts=10", "price"), 2000),
          Seq(getPartitionStatsIndexKey("ts=20", "price"), 3000),
          Seq(getPartitionStatsIndexKey("ts=30", "price"), 4000)
        )
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }

  test("Test hudi_table_changes cdc ordering issue") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql("set hoodie.sql.insert.mode = upsert")
      spark.sql(
        s"""
           |create table $tableName (
           |  rider string,
           |  driver string,
           |  fare double,
           |  ts long
           |) using hudi
           |tblproperties (
           |  type = 'cow',
           |  primaryKey = 'rider',
           |  preCombineField = 'ts',
           |  'hoodie.table.cdc.enabled' = 'true',
           |  'hoodie.table.cdc.supplemental.logging.mode' = 'data_before_after'
           |)
           |location '$tablePath'
           |""".stripMargin
      )

      spark.sql(
        s"""
           | insert into $tableName
           | values
           |   ('rider-E', 'driver-1', 10.0, 1695516137),
           |   ('rider-G', 'driver-2', 20.0, 1695516137),
           |   ('rider-C', 'driver-3', 30.0, 1695091554),
           |   ('rider-A', 'driver-4', 40.0, 1695516137),
           |   ('rider-F', 'driver-5', 50.0, 1695516137)
           | """.stripMargin
      )

      Thread.sleep(1000)

      spark.sql(
        s"""
           | insert into $tableName
           | values
           |   ('rider-G', 'driver-updated', 25.0, 1695516200),
           |   ('rider-C', 'driver-updated', 35.0, 1695091600),
           |   ('rider-F', 'driver-updated', 55.0, 1695516200)
           | """.stripMargin
      )

      Thread.sleep(1000)

      spark.sql(
        s"""
           | delete from $tableName where rider = 'rider-E'
           | """.stripMargin
      )

      val cdcDataUnordered = spark.sql(
        s"""select
           | op,
           | ts_ms,
           | get_json_object(before, '$$.ts') AS before_ts,
           | get_json_object(before, '$$.rider') as before_rider,
           | get_json_object(after, '$$.rider') AS after_rider
           |from hudi_table_changes('$tablePath', 'cdc', 'earliest')
           |""".stripMargin
      )

      val unorderedResults = cdcDataUnordered.collect()
      val unorderedCount = unorderedResults.length

      val cdcDataOrdered = spark.sql(
        s"""select
           | op,
           | ts_ms,
           | get_json_object(before, '$$.ts') AS before_ts,
           | get_json_object(before, '$$.rider') as before_rider,
           | get_json_object(after, '$$.rider') AS after_rider
           |from hudi_table_changes('$tablePath', 'cdc', 'earliest')
           |order by ts_ms asc
           |""".stripMargin
      )

      val orderedResults = cdcDataOrdered.collect()
      val orderedCount = orderedResults.length

      println(s"Unordered CDC results count: $unorderedCount")
      unorderedResults.foreach(row => println(s"Unordered: ${row.mkString(", ")}"))

      println(s"Ordered CDC results count: $orderedCount")
      orderedResults.foreach(row => println(s"Ordered: ${row.mkString(", ")}"))

      assert(unorderedCount == orderedCount,
        s"CDC query ordering issue: unordered count ($unorderedCount) != ordered count ($orderedCount)")

      val unorderedSet = unorderedResults.map(_.mkString(",")).toSet
      val orderedSet = orderedResults.map(_.mkString(",")).toSet

      assert(unorderedSet == orderedSet,
        s"CDC query results differ between ordered and unordered queries")

      // 5 inserts + 3 updates + 1 delete = 9 total
      val expectedCount = 9
      assert(orderedCount == expectedCount,
        s"Expected $expectedCount CDC operations, but got $orderedCount")
    }
  }
}
