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

import org.apache.hudi.{DataSourceWriteOptions, HoodieStreamingSink}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.embedded.EmbeddedTimelineService
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, Trigger}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

/** Injects failure after Hudi commits but before addBatch returns to Spark's checkpoint writer. */
class RecoveryTestSource extends StreamSinkProvider {
  override def createSink(context: SQLContext, options: Map[String, String], partitions: Seq[String],
                          mode: OutputMode): Sink = {
    val sink = new RecoveryTestSink(context, options, partitions, mode)
    RecoveryTestSource.sinks.put(options("path"), sink)
    sink
  }
}

object RecoveryTestSource {
  val sinks = new ConcurrentHashMap[String, RecoveryTestSink]()
}

class RecoveryTestSink(context: SQLContext, options: Map[String, String], partitions: Seq[String], mode: OutputMode)
  extends HoodieStreamingSink(context, options, partitions, mode) {
  val released = new CountDownLatch(1)
  @volatile var serverPort = -1
  @volatile var resourcesCleared = false

  private def field(name: String): Option[AnyRef] = {
    val f = classOf[HoodieStreamingSink].getDeclaredField(name)
    f.setAccessible(true)
    f.get(this).asInstanceOf[Option[AnyRef]]
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    super.addBatch(batchId, data)
    val client = field("writeClient").get.asInstanceOf[SparkRDDWriteClient[_]]
    val service = field("embeddedTimelineService").get.asInstanceOf[EmbeddedTimelineService]
    serverPort = service.getRemoteFileSystemViewConfig(client.getConfig).getRemoteViewServerPort
    if (options.getOrElse("test.fail.after.commit", "false").toBoolean) {
      throw new IllegalStateException("injected failure after Hudi commit before Spark checkpoint")
    }
  }

  override protected def reset(force: Boolean): Unit = {
    super.reset(force)
    resourcesCleared = field("writeClient").isEmpty && field("embeddedTimelineService").isEmpty
    released.countDown()
  }
}

class TestStreamingSinkRecovery extends HoodieSparkClientTestBase {
  @ParameterizedTest
  @ValueSource(strings = Array("available-now", "stop", "failure"))
  def testTerminalCleanupAndCommittedBatchReplay(termination: String): Unit = {
    val spark = sqlContext.sparkSession
    val source = s"$basePath/source"
    val target = s"$basePath/target"
    val checkpoint = s"$basePath/checkpoint"
    val input = spark.sql("select 1 as id, 10L as ts, 'first' as value")
    input.coalesce(1).write.json(source)
    val options = Map(
      "hoodie.table.name" -> "recovery",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.partitionpath.field" -> "",
      "hoodie.datasource.write.operation" -> "insert",
      "hoodie.metadata.enable" -> "false",
      "hoodie.embed.timeline.server" -> "true",
      "hoodie.embed.timeline.server.reuse.enabled" -> "false",
      "hoodie.insert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.STREAMING_RETRY_CNT.key -> "1",
      "checkpointLocation" -> checkpoint,
      "test.fail.after.commit" -> (termination == "failure").toString)
    val unrelated = spark.readStream.schema(input.schema).json(source).writeStream
      .format("memory").queryName("unrelated_recovery_query").start()
    val query = spark.readStream.schema(input.schema).json(source).writeStream
      .format(classOf[RecoveryTestSource].getName).options(options)
      .trigger(if (termination == "stop") Trigger.ProcessingTime(100) else Trigger.AvailableNow())
      .start(target)
    try {
      if (termination == "failure") {
        val error = assertThrows(classOf[StreamingQueryException], () => query.awaitTermination(60000))
        assertTrue(error.getMessage.contains("injected failure after Hudi commit"))
        assertFalse(new java.io.File(s"$checkpoint/commits/0").exists())
      } else if (termination == "stop") {
        query.processAllAvailable()
        query.stop()
      } else {
        assertTrue(query.awaitTermination(60000))
      }
      val sink = RecoveryTestSource.sinks.get(target)
      assertTrue(sink.released.await(30, TimeUnit.SECONDS), "Sink cleanup did not complete")
      assertTrue(sink.resourcesCleared, "Sink still retains its client or timeline service")
      assertTrue(sink.serverPort > 0)
      val socket = new Socket()
      try {
        assertThrows(classOf[java.io.IOException], () => socket.connect(new InetSocketAddress("127.0.0.1", sink.serverPort), 1000))
      } finally {
        socket.close()
      }
      unrelated.processAllAvailable()
      assertTrue(unrelated.isActive)
      assertEquals(1L, spark.table("unrelated_recovery_query").count())

      def commits: Seq[String] = HoodieTestUtils.createMetaClient(storage, target).getActiveTimeline
        .getCommitsTimeline.filterCompletedInstants().getInstants.asScala.map(_.requestedTime()).toSeq
      val before = commits
      assertEquals(1, before.size)
      val replay = spark.readStream.schema(input.schema).json(source).writeStream.format("hudi")
        .options(options - "test.fail.after.commit").trigger(Trigger.AvailableNow()).start(target)
      try {
        assertTrue(replay.awaitTermination(60000))
      } finally {
        replay.stop()
      }
      assertEquals(before, commits, "Restart recommitted a completed Hudi batch")
      assertEquals(input.collect().toSeq, spark.read.format("hudi").load(target).select("id", "ts", "value").collect().toSeq)
      assertTrue(new java.io.File(s"$checkpoint/commits/0").exists())
    } finally {
      query.stop()
      unrelated.stop()
      RecoveryTestSource.sinks.remove(target)
    }
  }
}
