/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import org.apache.hudi.DummyActiveAction
import org.apache.hudi.client.common.HoodieJavaEngineContext
import org.apache.hudi.client.timeline.versioning.v2.LSMTimelineWriter
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieCommitMetadata, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.timeline.{ActiveAction, HoodieInstant, LSMTimeline}
import org.apache.hudi.common.table.timeline.versioning.v2.CompletionTimeQueryViewV2
import org.apache.hudi.common.testutils.{HoodieTestTable, HoodieTestUtils}
import org.apache.hudi.common.testutils.HoodieTestUtils.{convertMetadataToByteArray, INSTANT_GENERATOR, TIMELINE_FACTORY}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.table.HoodieJavaTable

import org.apache.hadoop.fs.Path
import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}

import java.util

import scala.collection.JavaConverters._

object LSMTimelineReadBenchmark extends HoodieBenchmarkBase {

  /**
   * Java HotSpot(TM) 64-Bit Server VM 1.8.0_351-b10 on Mac OS X 13.4.1
   * Apple M2
   * pref load archived instants:              Best Time(ms)   Avg Time(ms)   Stdev(ms)    Rate(M/s)   Per Row(ns)   Relative
   * ------------------------------------------------------------------------------------------------------------------------
   * read slim instants                                  494            521          27          0.5        1899.6       1.0X
   * read instants with commit metadata                 2544           2625         116          0.1        9785.9       0.2X
   * read start time                                     156            177          26          1.7         601.1       3.2X
   */
  private def readArchivedInstantsBenchmark(): Unit = {
    withTempDir(f => {
      val tableName = "testTable"
      val tablePath = new Path(f.getCanonicalPath, tableName).toUri.toString
      val metaClient = HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf, tablePath, HoodieTableType.COPY_ON_WRITE, tableName)

      val writeConfig = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.INMEMORY).build())
        .withMarkersType("DIRECT")
        .build()
      val engineContext = new HoodieJavaEngineContext(HoodieTestUtils.getDefaultStorageConf)
      val writer = LSMTimelineWriter.getInstance(writeConfig, HoodieJavaTable.create(writeConfig, engineContext).asInstanceOf[HoodieJavaTable[HoodieAvroPayload]])

      val startTs = System.currentTimeMillis()
      val startInstant = startTs + 1 + ""
      val commitsNum = 260000
      val batchSize = 10
      val instantBuffer = new util.ArrayList[ActiveAction]()
      for (i <- 1 to commitsNum) {
        val instantTime = startTs + i + ""
        val completionTime = startTs + i + 1000 + ""
        val action = if (i % 2 == 0) "delta_commit" else "commit"
        val instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, action, instantTime, completionTime)
        val metadata: HoodieCommitMetadata = HoodieTestTable.of(metaClient).createCommitMetadata(instantTime, WriteOperationType.INSERT, util.Arrays.asList("par1", "par2"), 10, false)
        instantBuffer.add(new DummyActiveAction(instant, convertMetadataToByteArray(metadata)))
        if (i % batchSize == 0) {
          // archive 10 instants each time
          writer.write(instantBuffer, org.apache.hudi.common.util.Option.empty(), org.apache.hudi.common.util.Option.empty())
          writer.compactAndClean(engineContext)
          instantBuffer.clear()
        }
      }

      val benchmark = new HoodieBenchmark("pref load archived instants", commitsNum, 3)
      benchmark.addCase("read slim instants") { _ =>
        TIMELINE_FACTORY.createArchivedTimeline(metaClient)
      }
      benchmark.addCase("read instants with commit metadata") { _ =>
        TIMELINE_FACTORY.createArchivedTimeline(metaClient, startInstant)
      }
      // for scala compatibility
      val earliestStartTimeFunc: java.util.function.Function[String, String] = new java.util.function.Function[String, String] {
        override def apply(s: String): String = {
          (s.toLong - 1000) + ""
        }
      }
      benchmark.addCase("read start time") { _ =>
        new CompletionTimeQueryViewV2(metaClient).getInstantTimes(startTs + 1 + 1000 + "", startTs + commitsNum + 1000 + "", earliestStartTimeFunc)
      }
      benchmark.run()
      val totalSize = LSMTimeline.latestSnapshotManifest(metaClient, metaClient.getArchivePath).getFiles.asScala
        .map(f => f.getFileLen)
        .sum
      println("Total file size in bytes: " + totalSize)
    })
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    readArchivedInstantsBenchmark()
  }
}
