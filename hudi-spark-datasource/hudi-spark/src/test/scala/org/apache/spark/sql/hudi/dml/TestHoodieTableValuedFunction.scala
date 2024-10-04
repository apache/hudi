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

package org.apache.spark.sql.hudi.dml

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_INSERT_INTO_OPERATION
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionStatsIndexKey
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestHoodieTableValuedFunction extends HoodieSparkSqlTestBase {

  test(s"Test hudi_query Table-Valued Function") {
    if (HoodieSparkUtils.gteqSpark3_3) {
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
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }

  test(s"Test hudi_table_changes latest_state") {
    if (HoodieSparkUtils.gteqSpark3_3) {
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

          val firstInstant = spark.sql(s"select min(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

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
          val secondInstant = spark.sql(s"select max(_hoodie_commit_time) as commitTime from  $tableName order by commitTime").first().getString(0)

          checkAnswer(
            s"""select id,
               |name,
               |price,
               |ts
               |from hudi_table_changes(
               |'$identifier',
               |'latest_state',
               |'$firstInstant')
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
               | '$firstInstant',
               | '$secondInstant')
               | """.stripMargin
          )(
            Seq(1, "a1_1", 10.0, 1100),
            Seq(2, "a2_2", 20.0, 1100),
            Seq(3, "a3_3", 30.0, 1100)
          )
        }
      }
    }
  }

  test(s"Test hudi_filesystem_view") {
    if (HoodieSparkUtils.gteqSpark3_3) {
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
          result1DF.show(false)
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
  }

  test(s"Test hudi_table_changes cdc") {
    if (HoodieSparkUtils.gteqSpark3_3) {
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
  }

  test(s"Test hudi_query_timeline") {
    if (HoodieSparkUtils.gteqSpark3_3) {
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
          result2DF.show(false)
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
          result3DF.show(false)
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
          result5DF.show(false)
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
          result6DF.show(false)
          val result6Array = result6DF.take(10)
          checkAnswer(result6Array)(
            Seq(action, "COMPLETED"),
            Seq(action, "COMPLETED"),
            Seq(action, "COMPLETED")
          )
        }
      }
    }
  }

  test(s"Test hudi_metadata Table-Valued Function") {
    if (HoodieSparkUtils.gteqSpark3_3) {
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
          assert(result4DF.count() == 3)

          val result5DF = spark.sql(
            s"select type, key, recordIndexMetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.RECORD_INDEX.getRecordType}"
          )
          assert(result5DF.count() == 3)

          val result6DF = spark.sql(
            s"select type, key, BloomFilterMetadata from hudi_metadata('$identifier') where BloomFilterMetadata is not null"
          )
          assert(result6DF.count() == 0)

          // no partition stats by default
          val result7DF = spark.sql(
            s"select type, key, ColumnStatsMetadata from hudi_metadata('$identifier') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}"
          )
          assert(result7DF.count() == 0)
        }
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }

  test(s"Test hudi_metadata Table-Valued Function For PARTITION_STATS index") {
    if (HoodieSparkUtils.gteqSpark3_3) {
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
               |  hoodie.metadata.index.column.stats.column.list = 'price'
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
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
  }
}
