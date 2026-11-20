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

package org.apache.spark.sql.hudi.streaming

import org.apache.hudi.DataSourceReadOptions.{INCREMENTAL_READ_SKIP_COMPACT, STREAMING_MAX_COMMITS_PER_TRIGGER}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieArchivedTimeline, HoodieInstant, HoodieInstantReader, HoodieTimeline}
import org.apache.hudi.common.table.timeline.versioning.v2.{BaseTimelineV2, InstantGeneratorV2}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{anyBoolean, anyString}
import org.mockito.Mockito.{mock, never, verify, when}

import java.util.stream.Stream

import scala.collection.JavaConverters._

class TestHoodieSourceAdmissionControl {
  private class TestSource(limit: Option[String]) extends HoodieSourceAdmissionControl {
    var writes: Seq[String] = Seq("001", "003", "005")
    var candidateStarts: Seq[String] = Seq.empty
    var latest: Option[Offset] = Some(HoodieSourceOffset("006")) // clean/rollback after the last write
    override protected def admissionParameters: Map[String, String] = limit
      .map(STREAMING_MAX_COMMITS_PER_TRIGGER.key -> _).toMap
    override protected def admissionInitialOffset: HoodieSourceOffset = HoodieSourceOffset("000")
    override protected def completedWriteOffsets(start: String): Seq[String] = {
      candidateStarts = candidateStarts :+ start
      writes
    }
    override def getOffset: Option[Offset] = latest
    override def schema: StructType = new StructType()
    override def getBatch(start: Option[Offset], end: Offset): DataFrame = throw new UnsupportedOperationException()
    override def stop(): Unit = {}
  }

  @Test
  def testWriteOnlyCapAndAllAvailableOverride(): Unit = {
    val source = new TestSource(Some("2"))
    val limit = source.getDefaultReadLimit
    assertEquals(HoodieSourceOffset("003"), source.latestOffset(null, limit))
    assertEquals(HoodieSourceOffset("006"), source.latestOffset(HoodieSourceOffset("003"), limit))
    assertEquals(HoodieSourceOffset("006"), source.latestOffset(null, ReadLimit.allAvailable()))
    assertEquals(HoodieSourceOffset("006"), source.latestOffset(HoodieSourceOffset("006"), limit))
    assertEquals(Seq("000", "003"), source.candidateStarts)
    val unbounded = new TestSource(None)
    assertEquals(HoodieSourceOffset("006"), unbounded.latestOffset(null, unbounded.getDefaultReadLimit))
    source.writes = Seq("005", "001", "003", "003")
    assertEquals(HoodieSourceOffset("003"), source.latestOffset(null, limit))
    assertEquals(HoodieSourceOffset("006"), source.latestOffset(HoodieSourceOffset("003"), limit))
  }

  @Test
  def testAvailableNowPinsEvenAnEmptyTimeline(): Unit = {
    val source = new TestSource(Some("2"))
    source.prepareForTriggerAvailableNow()
    source.latest = Some(HoodieSourceOffset("009"))
    source.writes = source.writes :+ "009"
    assertEquals(HoodieSourceOffset("006"), source.latestOffset(HoodieSourceOffset("003"), source.getDefaultReadLimit))
    val empty = new TestSource(None)
    empty.latest = None
    empty.prepareForTriggerAvailableNow()
    empty.latest = Some(HoodieSourceOffset("001"))
    assertNull(empty.latestOffset(null, empty.getDefaultReadLimit))
  }

  @Test
  def testCapIncludesArchivedWritesAndSkipsCompaction(): Unit = {
    val generator = new InstantGeneratorV2()
    def instant(action: String, requested: String, completion: String): HoodieInstant =
      generator.createNewInstant(HoodieInstant.State.COMPLETED, action, requested, completion)
    val reader = mock(classOf[HoodieInstantReader])
    val archived = mock(classOf[HoodieArchivedTimeline])
    val overlapping = instant("deltacommit", "006", "007")
    when(archived.getWriteTimeline).thenReturn(new BaseTimelineV2(Stream.of(
      instant("deltacommit", "001", "002"), instant("commit", "003", "009"),
      instant("deltacommit", "004", "005"), overlapping), reader))
    val active = mock(classOf[HoodieActiveTimeline])
    val activeTimeline = new BaseTimelineV2(Seq(overlapping).asJava.stream(), reader)
    when(active.getWriteTimeline).thenReturn(activeTimeline)
    when(active.getTimelineOfActions(HoodieTimeline.WRITE_AND_SAVEPOINT_ACTIONS)).thenReturn(activeTimeline)
    val metaClient = mock(classOf[HoodieTableMetaClient])
    when(metaClient.getArchivedTimeline("001", false)).thenReturn(archived)
    when(metaClient.getActiveTimeline).thenReturn(active)
    when(metaClient.getTableType).thenReturn(HoodieTableType.MERGE_ON_READ)
    when(metaClient.getTableConfig).thenReturn(mock(classOf[HoodieTableConfig]))
    val parameters = Map(INCREMENTAL_READ_SKIP_COMPACT.key -> "true")
    val activeOnly = HoodieSourceAdmissionControl.readTimeline(metaClient, parameters, "006", byCompletionTime = false)
    assertEquals(Seq("006"), HoodieSourceAdmissionControl.commitOffsets(activeOnly, byCompletionTime = false))
    verify(metaClient, never()).getArchivedTimeline(anyString(), anyBoolean())

    val timeline = HoodieSourceAdmissionControl.readTimeline(metaClient, parameters, "001", byCompletionTime = false)
    verify(metaClient).getArchivedTimeline("001", false)
    assertEquals(Seq("001", "004", "006"), HoodieSourceAdmissionControl.commitOffsets(timeline, byCompletionTime = false))
    val source = new TestSource(Some("1"))
    source.writes = HoodieSourceAdmissionControl.commitOffsets(timeline, byCompletionTime = true)
    source.latest = Some(HoodieSourceOffset("009"))
    assertEquals(HoodieSourceOffset("002"), source.latestOffset(null, source.getDefaultReadLimit))
    assertEquals(HoodieSourceOffset("005"), source.latestOffset(HoodieSourceOffset("002"), source.getDefaultReadLimit))
    assertEquals(HoodieSourceOffset("009"), source.latestOffset(HoodieSourceOffset("005"), source.getDefaultReadLimit))
  }

  @Test
  def testCompletionTimeArchiveLookbackAndLegacyInstants(): Unit = {
    val generator = new InstantGeneratorV2()
    def write(requested: String, completion: String): HoodieInstant =
      generator.createNewInstant(HoodieInstant.State.COMPLETED, "commit", requested, completion)
    val reader = mock(classOf[HoodieInstantReader])
    val activeTimeline = new BaseTimelineV2(Seq(write("20261119120000000", "20261119130000000")).asJava.stream(), reader)
    val archiveTimeline = new BaseTimelineV2(Stream.of(
      write("20261118130000000", null),
      write("20261119100000000", "20261119130000000")), reader)
    val active = mock(classOf[HoodieActiveTimeline])
    val archived = mock(classOf[HoodieArchivedTimeline])
    val metaClient = mock(classOf[HoodieTableMetaClient])
    when(metaClient.getActiveTimeline).thenReturn(active)
    when(active.getWriteTimeline).thenReturn(activeTimeline)
    when(active.getTimelineOfActions(HoodieTimeline.WRITE_AND_SAVEPOINT_ACTIONS)).thenReturn(activeTimeline)
    when(metaClient.getArchivedTimeline("20261118110000000", false)).thenReturn(archived)
    when(archived.getWriteTimeline).thenReturn(archiveTimeline)

    val timeline = HoodieSourceAdmissionControl.readTimeline(metaClient, Map.empty, "20261119110000000", byCompletionTime = true)
    verify(metaClient).getArchivedTimeline("20261118110000000", false)
    assertEquals(Seq("20261119130000000", "20261119130000000"),
      HoodieSourceAdmissionControl.commitOffsets(timeline, byCompletionTime = true))
  }

  @Test
  def testInvalidLimitsFailBeforeReading(): Unit = {
    Seq("0", "-1", "1.5", "garbage", "2147483648").foreach { value =>
      assertThrows(classOf[IllegalArgumentException], () => new TestSource(Some(value)).getDefaultReadLimit)
    }
  }
}
