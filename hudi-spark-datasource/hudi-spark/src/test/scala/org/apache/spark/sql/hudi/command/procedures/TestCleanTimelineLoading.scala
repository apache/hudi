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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.avro.model.{HoodieCleanerPlan, HoodieCleanMetadata, HoodieCleanPartitionMetadata, HoodieLSMTimelineInstant}
import org.apache.hudi.common.HoodieTableFormat
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{ArchivedTimelineLoader, HoodieActiveTimeline, HoodieArchivedTimeline, HoodieInstant, HoodieInstantReader, HoodieTimeline, TimelineFactory, TimelineLayout}
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeAvroMetadata
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.hudi.procedure.HoodieSparkProcedureTestBase
import org.mockito.ArgumentMatchers.{any, eq => equalTo}
import org.mockito.Mockito.{doAnswer, mock, when}
import org.mockito.invocation.InvocationOnMock

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.function.{BiConsumer, Function}
import java.util.stream.Stream

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class TestCleanTimelineLoading extends HoodieSparkProcedureTestBase {
  private val layout = TimelineLayout.fromVersion(new TimelineLayoutVersion(2))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext
  }

  test("active plans filling the limit do not load any archived payloads") {
    val fixture = new Fixture(Seq("04"), Seq("03", "02", "01"))
    val rows = new ShowCleansPlanProcedure().getCleanerPlans(fixture.metaClient, 1, showArchived = true)
    assertResult(Seq("04"))(rows.map(_.getString(0)))
    assertResult("KEEP_LATEST_COMMITS")(rows.head.getString(5))
    assert(fixture.loaded.isEmpty)
  }

  test("only archived plans in the final top N are loaded") {
    val fixture = new Fixture(Seq("04"), Seq("03", "02", "01"))
    val rows = new ShowCleansPlanProcedure().getCleanerPlans(fixture.metaClient, 2, showArchived = true)
    assertResult(Seq("04", "03"))(rows.map(_.getString(0)))
    assert(rows.forall(_.getString(5) == "KEEP_LATEST_COMMITS"))
    assertResult(Seq("03"))(fixture.loaded.toSeq)
  }

  test("partition row limit continues past empty cleans and stops before older payloads") {
    val fixture = new Fixture(Seq("04"), Seq("03", "02", "01"),
      Map("04" -> 0, "03" -> 0, "02" -> 2, "01" -> 1))
    val timeline = ShowCleansProcedure.getCleanTimeline(fixture.metaClient, showArchived = true,
      loadPlans = false, instantLimit = Int.MaxValue, batchSize = 1)
    val rows = new ShowCleansProcedure(true).getCleansWithPartitionMetadata(timeline, 1)
    assertResult(1)(rows.size)
    assertResult("02")(rows.head.getString(0))
    assertResult(Seq("03", "02"))(fixture.loaded.toSeq)
  }

  test("partition row limit spans multiple partitions of one clean and continues to the next clean") {
    val fixture = new Fixture(Seq("04"), Seq("03", "02", "01", "00"),
      Map("04" -> 0, "03" -> 0, "02" -> 2, "01" -> 1, "00" -> 2))
    val timeline = ShowCleansProcedure.getCleanTimeline(fixture.metaClient, showArchived = true,
      loadPlans = false, instantLimit = Int.MaxValue, batchSize = 1)
    val rows = new ShowCleansProcedure(true).getCleansWithPartitionMetadata(timeline, 3)
    assertResult(Seq("02", "02", "01"))(rows.map(_.getString(0)))
    assertResult(Set("partition=0", "partition=1"))(rows.take(2).map(_.getString(4)).toSet)
    assertResult("partition=0")(rows.last.getString(4))
    assertResult(Seq("03", "02", "01"))(fixture.loaded.toSeq)
  }

  test("active partition rows filling the limit leave archived metadata unloaded") {
    val fixture = new Fixture(Seq("04"), Seq("03", "02", "01"), Map("04" -> 2))
    val timeline = ShowCleansProcedure.getCleanTimeline(fixture.metaClient, showArchived = true,
      loadPlans = false, instantLimit = Int.MaxValue, batchSize = 1)
    val rows = new ShowCleansProcedure(true).getCleansWithPartitionMetadata(timeline, 1)
    assertResult(1)(rows.size)
    assertResult("04")(rows.head.getString(0))
    assert(fixture.loaded.isEmpty)
  }

  private class Fixture(activeTimes: Seq[String], archivedTimes: Seq[String], partitionCounts: Map[String, Int] = Map.empty) {
    val loaded = ArrayBuffer.empty[String]
    val metaClient: HoodieTableMetaClient = mock(classOf[HoodieTableMetaClient])
    private val factory = mock(classOf[TimelineFactory])
    private val format = mock(classOf[HoodieTableFormat])
    private val loader = mock(classOf[ArchivedTimelineLoader])
    private val active = mock(classOf[HoodieActiveTimeline])
    private val archived = mock(classOf[HoodieArchivedTimeline])
    when(factory.createArchivedTimelineLoader()).thenReturn(loader)
    when(factory.createDefaultTimeline(any[Stream[HoodieInstant]](), any[HoodieInstantReader]()))
      .thenAnswer((invocation: InvocationOnMock) => layout.getTimelineFactory.createDefaultTimeline(
        invocation.getArgument[Stream[HoodieInstant]](0), invocation.getArgument[HoodieInstantReader](1)))
    when(format.getTimelineFactory).thenReturn(factory)
    when(metaClient.getTableFormat).thenReturn(format)
    when(metaClient.getTimelineLayoutVersion).thenReturn(new TimelineLayoutVersion(2))
    when(metaClient.getInstantGenerator).thenReturn(layout.getInstantGenerator)
    when(metaClient.getActiveTimeline).thenReturn(active)
    when(metaClient.getArchivedTimeline).thenReturn(archived)

    private def instant(time: String): HoodieInstant =
      layout.getInstantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, time, time)

    private val activeTimeline = factory.createDefaultTimeline(activeTimes.reverse.map(instant).asJava.stream(), new HoodieInstantReader {
      override def getContentStream(instant: HoodieInstant): ByteArrayInputStream = {
        val bytes = if (instant.isRequested) planBytes else metadataBytes(instant.requestedTime())
        new ByteArrayInputStream(bytes)
      }
    })
    when(active.getCleanerTimeline).thenReturn(activeTimeline)
    activeTimes.foreach { time =>
      Seq(HoodieInstant.State.REQUESTED, HoodieInstant.State.COMPLETED).foreach { state =>
        val clean = layout.getInstantGenerator.createNewInstant(state, HoodieTimeline.CLEAN_ACTION, time)
        when(active.getInstantContentStream(equalTo(clean))).thenAnswer((invocation: InvocationOnMock) =>
          activeTimeline.getInstantContentStream(invocation.getArgument[HoodieInstant](0)))
      }
    }
    private val archivedTimeline = factory.createDefaultTimeline(archivedTimes.reverse.map(instant).asJava.stream(), new HoodieInstantReader {})
    when(archived.getCleanerTimeline).thenReturn(archivedTimeline)
    doAnswer((invocation: InvocationOnMock) => {
      val range = invocation.getArgument[HoodieArchivedTimeline.TimeRangeFilter](1)
      val mode = invocation.getArgument[HoodieArchivedTimeline.LoadMode](2)
      val filter = invocation.getArgument[Function[GenericRecord, java.lang.Boolean]](3)
      val consumer = invocation.getArgument[BiConsumer[String, GenericRecord]](4)
      archivedTimes.filter(range.isInRange).foreach { time =>
        val record = new HoodieLSMTimelineInstant()
        record.setAction(HoodieTimeline.CLEAN_ACTION)
        val bytes = if (mode == HoodieArchivedTimeline.LoadMode.PLAN) planBytes else metadataBytes(time)
        if (mode == HoodieArchivedTimeline.LoadMode.PLAN) record.setPlan(ByteBuffer.wrap(bytes))
        else record.setMetadata(ByteBuffer.wrap(bytes))
        if (filter.apply(record)) {
          loaded += time
          consumer.accept(time, record)
        }
      }
      null
    }).when(loader).loadInstants(any(classOf[HoodieTableMetaClient]), any(classOf[HoodieArchivedTimeline.TimeRangeFilter]),
      any(classOf[HoodieArchivedTimeline.LoadMode]), any[Function[GenericRecord, java.lang.Boolean]](), any[BiConsumer[String, GenericRecord]]())

    private def planBytes: Array[Byte] = serializeAvroMetadata(HoodieCleanerPlan.newBuilder()
      .setPolicy("KEEP_LATEST_COMMITS").setVersion(2).build(), classOf[HoodieCleanerPlan]).get()

    private def metadataBytes(time: String): Array[Byte] = {
      val partitions = (0 until partitionCounts.getOrElse(time, 1)).map { i =>
        val path = s"partition=$i"
        path -> HoodieCleanPartitionMetadata.newBuilder().setPartitionPath(path).setPolicy("KEEP_LATEST_COMMITS")
          .setDeletePathPatterns(Seq.empty[String].asJava).setSuccessDeleteFiles(Seq.empty[String].asJava)
          .setFailedDeleteFiles(Seq.empty[String].asJava).setIsPartitionDeleted(false).build()
      }.toMap.asJava
      val metadata = HoodieCleanMetadata.newBuilder().setStartCleanTime(time).setTimeTakenInMillis(1)
        .setTotalFilesDeleted(0).setEarliestCommitToRetain(time).setPartitionMetadata(partitions).setVersion(2).build()
      serializeAvroMetadata(metadata, classOf[HoodieCleanMetadata]).get()
    }
  }
}
