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

package org.apache.hudi.functional

import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.engine.EngineType
import org.apache.hudi.common.model.{HoodieFailedWritesCleaningPolicy, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.USE_TRANSITION_TIME
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}

import org.apache.spark.api.java.JavaRDD

import scala.collection.JavaConverters._

class TestStreamSourceReadByStateTransitionTime extends TestStreamingSource {

  override val handlingMode: HollowCommitHandling = USE_TRANSITION_TIME

  private val dataGen = new HoodieTestDataGenerator(System.currentTimeMillis())

  test("Test streaming read out of order data") {
    HoodieTableType.values().foreach { tableType =>
      withTempDir { inputDir =>
        val tablePath = s"${inputDir.getCanonicalPath}/test_stream_${tableType.name()}"
        HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(tableType)
          .setTableName(s"test_stream_${tableType.name()}")
          .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
          .setPreCombineField("timestamp")
          .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

        val writeConfig = HoodieWriteConfig.newBuilder()
          .withEngineType(EngineType.SPARK)
          .withPath(tablePath)
          .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
          .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
          .withProps(commonOptions.asJava)
          .build()

        val context = new HoodieSparkEngineContext(sparkContext)
        val writeClient = new SparkRDDWriteClient(context, writeConfig)
        val instantTime1 = makeNewCommitTime(1, "%09d")
        val instantTime2 = makeNewCommitTime(2,"%09d")

        val records1 = sparkContext.parallelize(dataGen.generateInserts(instantTime1, 10).asScala.toSeq, 2)
        val records2 = sparkContext.parallelize(dataGen.generateInserts(instantTime2, 15).asScala.toSeq, 2)

        writeClient.startCommitWithTime(instantTime1)
        writeClient.startCommitWithTime(instantTime2)
        writeClient.insert(records2.toJavaRDD().asInstanceOf[JavaRDD[HoodieRecord[Nothing]]], instantTime2)
        val df = spark.readStream
          .format("hudi")
          .option(DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), handlingMode.name())
          .load(tablePath)

        testStream(df) (
          AssertOnQuery { q => q.processAllAvailable(); true },
          // Should read all records from instantTime2
          assertCountMatched(15, true),

          AssertOnQuery { _ =>
            writeClient.insert(records1.toJavaRDD().asInstanceOf[JavaRDD[HoodieRecord[Nothing]]], instantTime1)
            true
          },
          AssertOnQuery { q => q.processAllAvailable(); true },

          // Should read all records from instantTime1
          assertCountMatched(10, true),
          StopStream
        )
        writeClient.close()
      }
    }
  }

  def assertCountMatched(count: Int, lastOnly: Boolean): CheckAnswerRowsByFunc = CheckAnswerRowsByFunc(rows => {
    assert(rows.size == count)
  }, lastOnly = lastOnly)
}
