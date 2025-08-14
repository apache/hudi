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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.client.{SparkRDDWriteClient, WriteClientTestUtils}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.engine.EngineType
import org.apache.hudi.common.model.{HoodieFailedWritesCleaningPolicy, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig.{DELETE_PARALLELISM_VALUE, INSERT_PARALLELISM_VALUE, UPSERT_PARALLELISM_VALUE}
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.streaming.StreamTest

import scala.collection.JavaConverters._

class TestStreamSourceReadByStateTransitionTime extends StreamTest  {

  protected val commonOptions: Map[String, String] = Map(
    RECORDKEY_FIELD.key -> "id",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    INSERT_PARALLELISM_VALUE.key -> "4",
    UPSERT_PARALLELISM_VALUE.key -> "4",
    DELETE_PARALLELISM_VALUE.key -> "4",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition_path"
  )

  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

  override protected def sparkConf = {
    super.sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  }

  private val dataGen = new HoodieTestDataGenerator(System.currentTimeMillis())

  test("Test streaming read out of order data") {
    HoodieTableType.values().foreach { tableType =>
      withTempDir { inputDir =>
        val tablePath = s"${inputDir.getCanonicalPath}/test_stream_${tableType.name()}"
        HoodieTableMetaClient.newTableBuilder()
          .setTableType(tableType)
          .setTableName(s"test_stream_${tableType.name()}")
          .setOrderingFields("timestamp")
          .setPartitionFields("partition_path")
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

        WriteClientTestUtils.startCommitWithTime(writeClient, instantTime1)
        WriteClientTestUtils.startCommitWithTime(writeClient, instantTime2)
        writeClient.commit(instantTime2, writeClient.insert(records2.toJavaRDD().asInstanceOf[JavaRDD[HoodieRecord[Nothing]]], instantTime2))
        val df = spark.readStream
          .format("hudi")
          .load(tablePath)

        testStream(df) (
          AssertOnQuery { q => q.processAllAvailable(); true },
          // Should read all records from instantTime2
          assertCountMatched(15, true),

          AssertOnQuery { _ =>
            writeClient.commit(instantTime1, writeClient.insert(records1.toJavaRDD().asInstanceOf[JavaRDD[HoodieRecord[Nothing]]], instantTime1))
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
