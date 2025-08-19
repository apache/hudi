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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.SparkDatasetMixin
import org.apache.hudi.client.{SparkRDDWriteClient, WriteClientTestUtils}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.{getCommitTimeAtUTC, TRIP_EXAMPLE_SCHEMA}
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.api.java.JavaSparkContext

import java.util.{Collections, Properties}

import scala.collection.JavaConverters._

class TestTTLProcedure extends HoodieSparkProcedureTestBase with SparkDatasetMixin {

  test("Test Call run_ttl Procedure by Table") {
    withSQLConf("hoodie.partition.ttl.automatic" -> "false") {
      withTempDir { tmp => {
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        initTable(basePath)

        val writeConfig = getConfigBuilder(basePath, tableName, true).build()
        val client = getHoodieWriteClient(writeConfig)
        val dataGen = new HoodieTestDataGenerator(0xDEED)
        val partitionPaths = dataGen.getPartitionPaths()
        val partitionPath0 = partitionPaths(0)
        val instant0 = getCommitTimeAtUTC(0)

        writeRecordsForPartition(client, dataGen, partitionPath0, instant0)

        val instant1 = getCommitTimeAtUTC(1000)
        val partitionPath1 = partitionPaths(1)
        writeRecordsForPartition(client, dataGen, partitionPath1, instant1)

        val currentInstant = WriteClientTestUtils.createNewInstantTime()
        val partitionPath2 = partitionPaths(2)
        writeRecordsForPartition(client, dataGen, partitionPath2, currentInstant)
        spark.sql(
          s"""
             | create table $tableName using hudi
             | location '$basePath'
             | tblproperties (
             |   primaryKey = '_row_key',
             |   orderingFields = '_row_key',
             |   type = 'cow'
             | )
             |""".stripMargin)

        checkAnswer(s"call run_ttl(table => '$tableName', retain_days => 1)")(
          Seq(partitionPath0),
          Seq(partitionPath1)
        )
        client.close()
      }
      }
    }
  }

  private def writeRecordsForPartition(client: SparkRDDWriteClient[Nothing],
                                       dataGen: HoodieTestDataGenerator,
                                       partition: String, instantTime: String): Unit = {
    val records: java.util.List[HoodieRecord[Nothing]] =
      dataGen.generateInsertsForPartition(instantTime, 10, partition)
        .asInstanceOf[java.util.List[HoodieRecord[Nothing]]]
    // Use this JavaRDD to call the insert method
    WriteClientTestUtils.startCommitWithTime(client, instantTime, HoodieTimeline.COMMIT_ACTION)
    val statuses = client.insert(spark.sparkContext.parallelize(records.asScala.toSeq).toJavaRDD(), instantTime)
    client.commit(instantTime, statuses)
  }

  private def getHoodieWriteClient(cfg: HoodieWriteConfig): SparkRDDWriteClient[Nothing] = {
    val writeClient = new SparkRDDWriteClient[Nothing](
      new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)), cfg
    )
    writeClient
  }

  private def initTable(basePath: String): Unit = {
    val props = new Properties()
    props.put("hoodie.datasource.write.partitionpath.field", "partition_path")
    props.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator")
    props.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path")
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key")
    HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE, props);
  }

  protected def getConfigBuilder(basePath: String, tableName: String, autoCommit: Boolean): HoodieWriteConfig.Builder =
    HoodieWriteConfig
      .newBuilder
      .withPath(basePath)
      .withSchema(TRIP_EXAMPLE_SCHEMA)
      .forTable(tableName)
      .withProps(Collections.singletonMap(HoodieTableConfig.ORDERING_FIELDS.key(), "_row_key"))
}
