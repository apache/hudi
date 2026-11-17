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

import org.apache.hudi.DataSourceReadOptions.STREAMING_MAX_COMMITS_PER_TRIGGER

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows}
import org.junit.jupiter.api.Test

class TestHoodieSourceAdmissionControl {
  private class TestSource(limit: Option[String]) extends HoodieSourceAdmissionControl {
    var writes: Seq[String] = Seq("001", "003", "005")
    var latest: Option[Offset] = Some(HoodieSourceOffset("006")) // clean/rollback after the last write
    override protected def admissionParameters: Map[String, String] = limit
      .map(STREAMING_MAX_COMMITS_PER_TRIGGER.key -> _).toMap
    override protected def admissionInitialOffset: HoodieSourceOffset = HoodieSourceOffset("000")
    override protected def completedWriteOffsets: Seq[String] = writes
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
  def testInvalidLimitsFailBeforeReading(): Unit = {
    Seq("0", "-1", "1.5", "garbage", "2147483648").foreach { value =>
      assertThrows(classOf[IllegalArgumentException], () => new TestSource(Some(value)).getDefaultReadLimit)
    }
  }
}
