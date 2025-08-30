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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.functional.TestBootstrap
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, Row}

import java.time.Instant
import java.util

class TestBootstrapProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call run_bootstrap Procedure") {
    withTempDir { tmp =>
      val NUM_OF_RECORDS = 100
      val PARTITION_FIELD = "datestr"
      val RECORD_KEY_FIELD = "_row_key"

      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}"

      val srcName: String = "source"
      val sourcePath = basePath + StoragePath.SEPARATOR + srcName
      val tablePath = basePath + StoragePath.SEPARATOR + tableName
      val jsc = new JavaSparkContext(spark.sparkContext)

      // generate test data
      val partitions = util.Arrays.asList("2018", "2019", "2020")
      val timestamp: Long = Instant.now.toEpochMilli
      for (i <- 0 until partitions.size) {
        val df: Dataset[Row] = TestBootstrap.generateTestRawTripDataset(timestamp, i * NUM_OF_RECORDS, i * NUM_OF_RECORDS + NUM_OF_RECORDS, null, jsc, spark.sqlContext)
        df.write.parquet(sourcePath + StoragePath.SEPARATOR + PARTITION_FIELD + "=" + partitions.get(i))
      }

      spark.sql("set hoodie.bootstrap.parallelism = 20")
      spark.sql("set hoodie.metadata.index.column.stats.enable = false")
      checkAnswer(
        s"""call run_bootstrap(
           |table => '$tableName',
           |base_path => '$tablePath',
           |table_type => '${HoodieTableType.COPY_ON_WRITE.name}',
           |bootstrap_path => '$sourcePath',
           |rowKey_field => '$RECORD_KEY_FIELD',
           |partition_path_field => '$PARTITION_FIELD',
           |bootstrap_overwrite => true)""".stripMargin) {
        Seq(0)
      }

      // create table
      spark.sql(
        s"""
           |create table $tableName using hudi
           |location '$tablePath'
           |tblproperties(primaryKey = '$RECORD_KEY_FIELD')
           |""".stripMargin)

      // show bootstrap's index partitions
      var result = spark.sql(s"""call show_bootstrap_partitions(table => '$tableName')""".stripMargin).collect()
      assertResult(3) {
        result.length
      }

      // show bootstrap's index mapping
      result = spark.sql(
        s"""call show_bootstrap_mapping(table => '$tableName')""".stripMargin).collect()
      assertResult(10) {
        result.length
      }

      // cluster with row writer disabled and assert that records match with that before clustering
      // NOTE: the row writer path is already tested in TestDataSourceForBootstrap
      val beforeClusterDf = spark.sql(s"select * from $tableName")
      spark.sql("set hoodie.datasource.write.row.writer.enable = false")
      spark.sql(s"""call run_clustering(table => '$tableName')""".stripMargin)
      assertResult(0)(spark.sql(s"select * from $tableName").except(beforeClusterDf).count())

      spark.sessionState.conf.unsetConf("unset hoodie.metadata.index.column.stats.enable") // HUDI-8774
    }
  }

  test("Test Call run_bootstrap Procedure with properties") {
    withTempDir { tmp =>
      val NUM_OF_RECORDS = 100
      val PARTITION_FIELD = "datestr"
      val RECORD_KEY_FIELD = "_row_key"

      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}"

      val srcName: String = "source"
      val sourcePath = basePath + StoragePath.SEPARATOR + srcName
      val tablePath = basePath + StoragePath.SEPARATOR + tableName
      val jsc = new JavaSparkContext(spark.sparkContext)

      // generate test data
      val partitions = util.Arrays.asList("2018", "2019", "2020")
      val timestamp: Long = Instant.now.toEpochMilli
      for (i <- 0 until partitions.size) {
        val df: Dataset[Row] = TestBootstrap.generateTestRawTripDataset(timestamp, i * NUM_OF_RECORDS, i * NUM_OF_RECORDS + NUM_OF_RECORDS, null, jsc, spark.sqlContext)
        df.write.parquet(sourcePath + StoragePath.SEPARATOR + PARTITION_FIELD + "=" + partitions.get(i))
      }

      spark.sql("set hoodie.bootstrap.parallelism = 20")
      spark.sql("set hoodie.metadata.index.column.stats.enable = false")
      checkAnswer(
        s"""call run_bootstrap(
           |table => '$tableName',
           |base_path => '$tablePath',
           |table_type => '${HoodieTableType.COPY_ON_WRITE.name}',
           |bootstrap_path => '$sourcePath',
           |rowKey_field => '$RECORD_KEY_FIELD',
           |partition_path_field => '$PARTITION_FIELD',
           |options => 'hoodie.datasource.write.hive_style_partitioning=true',
           |bootstrap_overwrite => true)""".stripMargin) {
        Seq(0)
      }

      // create table
      spark.sql(
        s"""
           |create table $tableName using hudi
           |location '$tablePath'
           |tblproperties(primaryKey = '$RECORD_KEY_FIELD')
           |""".stripMargin)

      // show bootstrap's index partitions
      var result = spark.sql(s"""call show_bootstrap_partitions(table => '$tableName')""".stripMargin).collect()
      assertResult(3) {
        result.length
      }

      // show bootstrap's index mapping
      result = spark.sql(
        s"""call show_bootstrap_mapping(table => '$tableName')""".stripMargin).collect()
      assertResult(10) {
        result.length
      }

      val metaClient = createMetaClient(spark, tablePath)

      assertResult("true") {
        metaClient.getTableConfig.getString(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE)
      };
      spark.sessionState.conf.unsetConf("unset hoodie.metadata.index.column.stats.enable")
    }
  }

  test("Test Call run_bootstrap Procedure with no-partitioned") {
    withTempDir { tmp =>
      val NUM_OF_RECORDS = 100
      val RECORD_KEY_FIELD = "_row_key"

      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}"

      val srcName: String = "source"
      val sourcePath = basePath + StoragePath.SEPARATOR + srcName
      val tablePath = basePath + StoragePath.SEPARATOR + tableName
      val jsc = new JavaSparkContext(spark.sparkContext)

      // generate test data
      val timestamp: Long = Instant.now.toEpochMilli
      val df: Dataset[Row] = TestBootstrap.generateTestRawTripDataset(timestamp, 0, NUM_OF_RECORDS, null, jsc, spark.sqlContext)
      df.write.parquet(sourcePath)

      spark.sql("set hoodie.bootstrap.parallelism = 20")
      spark.sql("set hoodie.metadata.index.column.stats.enable = false")
      // run bootstrap
      checkAnswer(
        s"""call run_bootstrap(
           |table => '$tableName',
           |base_path => '$tablePath',
           |table_type => '${HoodieTableType.COPY_ON_WRITE.name}',
           |bootstrap_path => '$sourcePath',
           |rowKey_field => '$RECORD_KEY_FIELD',
           |key_generator_class => 'NON_PARTITION',
           |bootstrap_overwrite => true)""".stripMargin) {
        Seq(0)
      }

      // create table
      spark.sql(
        s"""
           |create table $tableName using hudi
           |location '$tablePath'
           |tblproperties(primaryKey = '$RECORD_KEY_FIELD')
           |""".stripMargin)

      // use new hudi table
      val originCount = spark.sql(s"select count(*) from $tableName").collect()(0).getLong(0)
      spark.sql(s"delete from $tableName where _row_key = 'trip_0'")
      val afterDeleteCount = spark.sql(s"select count(*) from $tableName").collect()(0).getLong(0)
      assert(originCount != afterDeleteCount)

      // cluster with row writer disabled and assert that records match with that before clustering
      // NOTE: the row writer path is already tested in TestDataSourceForBootstrap
      val beforeClusterDf = spark.sql(s"select * from $tableName")
      spark.sql("set hoodie.datasource.write.row.writer.enable = false")
      spark.sql(s"""call run_clustering(table => '$tableName')""".stripMargin)
      assertResult(0)(spark.sql(s"select * from $tableName").except(beforeClusterDf).count())
      spark.sessionState.conf.unsetConf("unset hoodie.metadata.index.column.stats.enable")
    }
  }

  test("Test Call run_bootstrap Procedure about MOR with full record") {
    withTempDir { tmp =>
      val NUM_OF_RECORDS = 100
      val PARTITION_FIELD = "datestr"
      val RECORD_KEY_FIELD = "_row_key"

      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}"

      val srcName: String = "source"
      val sourcePath = basePath + StoragePath.SEPARATOR + srcName
      val tablePath = basePath + StoragePath.SEPARATOR + tableName
      val jsc = new JavaSparkContext(spark.sparkContext)

      // generate test data
      val partitions = util.Arrays.asList("2018", "2019", "2020")
      val timestamp: Long = Instant.now.toEpochMilli
      for (i <- 0 until partitions.size) {
        val df: Dataset[Row] = TestBootstrap.generateTestRawTripDataset(timestamp, i * NUM_OF_RECORDS, i * NUM_OF_RECORDS + NUM_OF_RECORDS, null, jsc, spark.sqlContext)
        df.write.parquet(sourcePath + StoragePath.SEPARATOR + PARTITION_FIELD + "=" + partitions.get(i))
      }

      spark.sql("set hoodie.bootstrap.parallelism = 20")
      spark.sql("set hoodie.table.ordering.fields=timestamp")
      spark.sql("set hoodie.metadata.index.column.stats.enable = false")

      checkAnswer(
        s"""call run_bootstrap(
           |table => '$tableName',
           |base_path => '$tablePath',
           |table_type => '${HoodieTableType.MERGE_ON_READ.name}',
           |bootstrap_path => '$sourcePath',
           |rowKey_field => '$RECORD_KEY_FIELD',
           |selector_class => 'org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector',
           |partition_path_field => '$PARTITION_FIELD',
           |bootstrap_overwrite => true)""".stripMargin) {
        Seq(0)
      }
      spark.sessionState.conf.unsetConf("unset hoodie.metadata.index.column.stats.enable")
    }
  }
}
