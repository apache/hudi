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

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.functional.TestBootstrap
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.apache.spark.sql.{Dataset, Row}

import java.time.Instant
import java.util

class TestBootstrapProcedure extends HoodieSparkSqlTestBase {

  test("Test Call run_bootstrap Procedure") {
    withTempDir { tmp =>
      val NUM_OF_RECORDS = 100
      val PARTITION_FIELD = "datestr"
      val RECORD_KEY_FIELD = "_row_key"

      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}"

      val srcName: String = "source"
      val sourcePath = basePath + Path.SEPARATOR + srcName
      val tablePath = basePath + Path.SEPARATOR + tableName
      val jsc = new JavaSparkContext(spark.sparkContext)

      // generate test data
      val partitions = util.Arrays.asList("2018", "2019", "2020")
      val timestamp: Long = Instant.now.toEpochMilli
      for (i <- 0 until partitions.size) {
        val df: Dataset[Row] = TestBootstrap.generateTestRawTripDataset(timestamp, i * NUM_OF_RECORDS, i * NUM_OF_RECORDS + NUM_OF_RECORDS, null, jsc, spark.sqlContext)
        df.write.parquet(sourcePath + Path.SEPARATOR + PARTITION_FIELD + "=" + partitions.get(i))
      }

      // run bootstrap
      checkAnswer(
        s"""call run_bootstrap(
           |table => '$tableName',
           |basePath => '$tablePath',
           |tableType => '${HoodieTableType.COPY_ON_WRITE.name}',
           |bootstrapPath => '$sourcePath',
           |rowKeyField => '$RECORD_KEY_FIELD',
           |partitionPathField => '$PARTITION_FIELD',
           |bootstrapOverwrite => true)""".stripMargin) {
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
      assertResult(3) {
        result.length
      }
    }
  }
}
