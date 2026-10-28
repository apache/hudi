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

import org.apache.hudi.{DefaultSource, HoodieStreamingSink}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.WriteConcurrencyMode
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters._

class TestStreamingSinkOptions extends HoodieSparkClientTestBase {
  private val options = Map(
    HoodieWriteConfig.TBL_NAME.key -> "streaming_options",
    RECORDKEY_FIELD.key -> "id",
    ORDERING_FIELDS.key -> "ts",
    PARTITIONPATH_FIELD.key -> "",
    "hoodie.insert.shuffle.parallelism" -> "1",
    "hoodie.upsert.shuffle.parallelism" -> "1",
    "hoodie.delete.shuffle.parallelism" -> "1",
    "hoodie.metadata.enable" -> "false")

  private def rows(session: SparkSession): DataFrame = session.sql("select 1 as id, 1L as ts")

  private def meta(path: String): HoodieTableMetaClient = HoodieTestUtils.createMetaClient(storage, path)

  private def operations(path: String): Seq[String] = {
    val timeline = meta(path).getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
    timeline.getInstants.asScala.map(instant => timeline.readCommitMetadata(instant).getOperationType.name()).toSeq
  }

  private def start(session: SparkSession, source: String, target: String, checkpoint: String,
                    extraOptions: Map[String, String] = Map.empty, once: Boolean = true): StreamingQuery = {
    session.readStream.schema(rows(session).schema).json(source).writeStream
      .format("org.apache.hudi").options(options ++ extraOptions)
      .option("checkpointLocation", checkpoint)
      .trigger(if (once) Trigger.Once() else Trigger.ProcessingTime(100))
      .start(target)
  }

  private def run(session: SparkSession, name: String, target: String,
                  extraOptions: Map[String, String] = Map.empty): Unit = {
    val source = s"$basePath/source_$name"
    rows(session).write.json(source)
    val query = start(session, source, target, s"$basePath/checkpoint_$name", extraOptions)
    try {
      assertTrue(query.awaitTermination(60000), "Streaming query did not finish within 60 seconds")
    } finally {
      query.stop()
    }
  }

  @ParameterizedTest
  @CsvSource(Array("none,none,UPSERT", "insert,none,INSERT", "insert,upsert,UPSERT"))
  def testSessionOperationAndExplicitPrecedence(sessionOperation: String, explicitOperation: String,
                                               expected: String): Unit = {
    val session = sqlContext.sparkSession.newSession()
    // Bare hoodie.* session state must not leak into the writer, even when no prefixed setting exists.
    session.conf.set(OPERATION.key, "delete")
    if (sessionOperation != "none") {
      session.sql(s"SET spark.${OPERATION.key}=$sessionOperation")
    }
    val explicit = if (explicitOperation == "none") Map.empty[String, String] else Map(OPERATION.key -> explicitOperation)
    val target = s"$basePath/target"
    run(session, "first", target, explicit)
    assertEquals(Seq(expected), operations(target))
    assertEquals(1L, session.read.format("hudi").load(target).count())
    // A fresh Spark checkpoint replays batch 0; the sink must still recognize the committed batch.
    run(session, "replay", target, explicit)
    assertEquals(Seq(expected), operations(target))
  }

  @Test
  def testPrefixedExplicitDeleteAgreesWithCheckpointDeduplication(): Unit = {
    val session = sqlContext.sparkSession.newSession()
    val target = s"$basePath/target"
    run(session, "insert", target)
    session.conf.set(s"spark.${OPERATION.key}", "upsert")
    // The delete stream also starts at batch 0. A noncanonical operation at canSkipBatch
    // would treat it as upsert and skip it, even though the writer would resolve delete.
    run(session, "delete", target, Map(s"spark.${OPERATION.key}" -> "delete"))
    assertEquals(Seq("UPSERT", "DELETE"), operations(target))
    assertEquals(0L, session.read.format("hudi").load(target).count())
  }

  @Test
  def testFlowSettingsAreCapturedAtSinkCreation(): Unit = {
    val session = sqlContext.sparkSession.newSession()
    val source = s"$basePath/source"
    storage.createDirectory(new StoragePath(source))
    session.sql(s"SET spark.${OPERATION.key}=insert")
    val insertQuery = start(session, source, s"$basePath/inserts", s"$basePath/insert_checkpoint", once = false)
    try {
      session.sql(s"SET spark.${OPERATION.key}=upsert")
      val upsertQuery = start(session, source, s"$basePath/upserts", s"$basePath/upsert_checkpoint", once = false)
      try {
        // Both queries must retain their captured settings after the shared session changes again.
        session.sql(s"SET spark.${OPERATION.key}=delete")
        rows(session).write.mode("append").json(source)
        insertQuery.processAllAvailable()
        upsertQuery.processAllAvailable()
        rows(session).write.mode("append").json(source)
        insertQuery.processAllAvailable()
        upsertQuery.processAllAvailable()
        assertEquals(Seq("INSERT", "INSERT"), operations(s"$basePath/inserts"))
        assertEquals(Seq("UPSERT", "UPSERT"), operations(s"$basePath/upserts"))
        assertEquals(2L, session.read.format("hudi").load(s"$basePath/inserts").count())
        assertEquals(1L, session.read.format("hudi").load(s"$basePath/upserts").count())
      } finally {
        upsertQuery.stop()
      }
    } finally {
      insertQuery.stop()
    }
  }

  @Test
  def testMultiWriterValidationUsesCapturedSessionOptions(): Unit = {
    val session = sqlContext.sparkSession.newSession()
    val source = new DefaultSource
    val opts = options + ("path" -> s"$basePath/target")
    session.conf.set(s"spark.${HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key}", WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name())
    val error = assertThrows(classOf[IllegalStateException], () =>
      source.createSink(session.sqlContext, opts, Seq.empty, OutputMode.Append()))
    assertTrue(error.getMessage.contains(STREAMING_CHECKPOINT_IDENTIFIER.key))
    session.conf.set(s"spark.${STREAMING_CHECKPOINT_IDENTIFIER.key}", "flow")
    assertTrue(source.createSink(session.sqlContext, opts, Seq.empty, OutputMode.Append()).isInstanceOf[HoodieStreamingSink])
    session.conf.unset(s"spark.${STREAMING_CHECKPOINT_IDENTIFIER.key}")
    assertTrue(source.createSink(session.sqlContext,
      opts + (s"spark.${HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key}" -> WriteConcurrencyMode.SINGLE_WRITER.name()),
      Seq.empty, OutputMode.Append()).isInstanceOf[HoodieStreamingSink])
  }

  @Test
  def testCapturedSettingsCannotChangeExistingTableIdentity(): Unit = {
    val session = sqlContext.sparkSession.newSession()
    val target = s"$basePath/target"
    run(session, "first", target)
    session.conf.set(s"spark.${RECORDKEY_FIELD.key}", "ts")
    val source = new DefaultSource
    val opts = (options - RECORDKEY_FIELD.key) + ("path" -> target)
    val error = assertThrows(classOf[HoodieException], () =>
      source.createSink(session.sqlContext, opts, Seq.empty, OutputMode.Append()))
    assertTrue(error.getMessage.contains("RecordKey"))
    // Explicit identity wins over ambient settings, but must still agree with the persisted table.
    assertTrue(source.createSink(session.sqlContext, opts + (RECORDKEY_FIELD.key -> "id"),
      Seq.empty, OutputMode.Append()).isInstanceOf[HoodieStreamingSink])
    assertThrows(classOf[HoodieException], () =>
      source.createSink(session.sqlContext, opts + (RECORDKEY_FIELD.key -> "ts"), Seq.empty, OutputMode.Append()))
    assertEquals(Seq("UPSERT"), operations(target))
  }
}
