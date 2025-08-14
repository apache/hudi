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

package org.apache.hudi.examples.spark

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieRecordPayload, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.examples.common.{HoodieExampleDataGenerator, HoodieExampleSparkUtils}

import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * Simple example to run a compaction job for MOR table.
 * To run this example, you should:
 *   1. For running in IDE, set VM options `-Dspark.master=local[2]`
 *   2. For running in shell, using `spark-submit`
 *
 * Usage: HoodieMorCompactionJob <tablePath> <tableName>.
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieMorCompactionJob file:///tmp/hoodie/hudi_mor_table hudi_mor_table`
 */
object HoodieMorCompactionJob {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: HoodieMorCompactionJob <tablePath> <tableName>")
      System.exit(1)
    }

    val spark = HoodieExampleSparkUtils.defaultSparkSession("Hudi MOR table compaction via Spark example")
    val dataGen = new HoodieExampleDataGenerator[HoodieAvroPayload]
    val tablePath = args(0)
    val tableName = args(1)

    insertData(spark, tablePath, tableName, dataGen, HoodieTableType.MERGE_ON_READ.name())
    updateData(spark, tablePath, tableName, dataGen, HoodieTableType.MERGE_ON_READ.name())
    val cfg = HoodieWriteConfig.newBuilder()
      .withPath(tablePath)
      .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA)
      .withParallelism(2, 2)
      .forTable(tableName)
      .withCompactionConfig(HoodieCompactionConfig.newBuilder()
        .withInlineCompaction(true)
        .withMaxNumDeltaCommitsBeforeCompaction(1).build())
      .build()
    val client = new SparkRDDWriteClient[HoodieRecordPayload[Nothing]](new HoodieSparkEngineContext(spark.sparkContext), cfg)
    try {
      val instant = client.scheduleCompaction(Option.empty())
      val result = client.compact(instant.get())
      client.commitCompaction(instant.get(), result, org.apache.hudi.common.util.Option.empty())
      client.clean()
    } catch {
      case e: Exception => System.err.println(s"Compaction failed due to", e)
    } finally {
      client.close()
      spark.stop()
    }
  }

  def insertData(spark: SparkSession, tablePath: String, tableName: String,
                 dataGen: HoodieExampleDataGenerator[HoodieAvroPayload], tableType: String): Unit = {
    val commitTime: String = System.currentTimeMillis().toString
    val inserts = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20)).asScala.toSeq
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(HoodieTableConfig.ORDERING_FIELDS.key, "ts").
      option(RECORDKEY_FIELD.key, "uuid").
      option(PARTITIONPATH_FIELD.key, "partitionpath").
      option(TBL_NAME.key, tableName).
      option(TABLE_TYPE.key, tableType).
      mode(Overwrite).
      save(tablePath)
  }

  def updateData(spark: SparkSession, tablePath: String, tableName: String,
                 dataGen: HoodieExampleDataGenerator[HoodieAvroPayload], tableType: String): Unit = {
    val commitTime: String = System.currentTimeMillis().toString
    val updates = dataGen.convertToStringList(dataGen.generateUpdates(commitTime, 10)).asScala.toSeq
    val df = spark.read.json(spark.sparkContext.parallelize(updates, 1))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(HoodieTableConfig.ORDERING_FIELDS.key, "ts").
      option(RECORDKEY_FIELD.key, "uuid").
      option(PARTITIONPATH_FIELD.key, "partitionpath").
      option(TBL_NAME.key, tableName).
      option(TABLE_TYPE.key, tableType).
      mode(Append).
      save(tablePath)
  }
}
