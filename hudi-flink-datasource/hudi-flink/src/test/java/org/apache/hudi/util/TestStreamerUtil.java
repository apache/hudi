/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for StreamerUtil Kafka offset methods.
 */
public class TestStreamerUtil {

  @Test
  public void testExtractKafkaOffsetMetadata() throws Exception {
    HoodieInstant mockInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata mockCommitMetadata = mock(HoodieCommitMetadata.class);

    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(StreamerUtil.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200");

    when(mockCommitMetadata.getExtraMetadata()).thenReturn(extraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(any(HoodieInstant.class), any(HoodieTimeline.class)))
          .thenReturn(mockCommitMetadata);

      String result = StreamerUtil.extractKafkaOffsetMetadata(mockInstant, mockTimeline);
      assertEquals("kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200", result);
    }
  }

  @Test
  public void testExtractKafkaOffsetMetadataNotFound() throws Exception {
    HoodieInstant mockInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata mockCommitMetadata = mock(HoodieCommitMetadata.class);

    Map<String, String> extraMetadata = new HashMap<>();

    when(mockCommitMetadata.getExtraMetadata()).thenReturn(extraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(any(HoodieInstant.class), any(HoodieTimeline.class)))
          .thenReturn(mockCommitMetadata);

      String result = StreamerUtil.extractKafkaOffsetMetadata(mockInstant, mockTimeline);
      assertNull(result);
    }
  }

  @Test
  public void testParseKafkaOffsetsValidInput() {
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200;kafka_metadata%3Atopic%3A2:300";

    Map<Integer, Long> result = StreamerUtil.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(3, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(200L, result.get(1));
    assertEquals(300L, result.get(2));
  }

  @Test
  public void testParseKafkaOffsetsEmptyString() {
    Map<Integer, Long> result = StreamerUtil.parseKafkaOffsets("");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseKafkaOffsetsNullString() {
    Map<Integer, Long> result = StreamerUtil.parseKafkaOffsets(null);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseKafkaOffsetsWithClusterMetadata() {
    // Include cluster metadata which should be skipped
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;kafka_cluster%3Atopicname%3Aclustername;kafka_metadata%3Atopic%3A1:200";

    Map<Integer, Long> result = StreamerUtil.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(2, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(200L, result.get(1));
  }

  @Test
  public void testParseKafkaOffsetsMalformedEntry() {
    // Entry without colon should be skipped
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;invalidentry;kafka_metadata%3Atopic%3A1:200";

    Map<Integer, Long> result = StreamerUtil.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(2, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(200L, result.get(1));
  }

  @Test
  public void testParseKafkaOffsetsInvalidNumber() {
    // Entry with invalid offset number should be skipped
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:notanumber;kafka_metadata%3Atopic%3A2:300";

    Map<Integer, Long> result = StreamerUtil.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(2, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(300L, result.get(2));
  }

  @Test
  public void testCalculateKafkaOffsetDifference() throws Exception {
    HoodieInstant currentInstant = mock(HoodieInstant.class);
    HoodieInstant previousInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata currentMetadata = mock(HoodieCommitMetadata.class);
    HoodieCommitMetadata previousMetadata = mock(HoodieCommitMetadata.class);

    when(currentInstant.requestedTime()).thenReturn("20250828120000");
    when(previousInstant.requestedTime()).thenReturn("20250828110000");

    Map<String, String> currentExtraMetadata = new HashMap<>();
    currentExtraMetadata.put(StreamerUtil.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:150;kafka_metadata%3Atopic%3A1:250;kafka_metadata%3Atopic%3A2:350");

    Map<String, String> previousExtraMetadata = new HashMap<>();
    previousExtraMetadata.put(StreamerUtil.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200;kafka_metadata%3Atopic%3A2:300");

    when(currentMetadata.getExtraMetadata()).thenReturn(currentExtraMetadata);
    when(previousMetadata.getExtraMetadata()).thenReturn(previousExtraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(currentInstant, mockTimeline))
          .thenReturn(currentMetadata);
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(previousInstant, mockTimeline))
          .thenReturn(previousMetadata);

      long result = StreamerUtil.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline);

      // (150-100) + (250-200) + (350-300) = 50 + 50 + 50 = 150
      assertEquals(150L, result);
    }
  }

  @Test
  public void testCalculateKafkaOffsetDifferenceWithNewPartition() throws Exception {
    HoodieInstant currentInstant = mock(HoodieInstant.class);
    HoodieInstant previousInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata currentMetadata = mock(HoodieCommitMetadata.class);
    HoodieCommitMetadata previousMetadata = mock(HoodieCommitMetadata.class);

    when(currentInstant.requestedTime()).thenReturn("20250828120000");
    when(previousInstant.requestedTime()).thenReturn("20250828110000");

    Map<String, String> currentExtraMetadata = new HashMap<>();
    // Current has partition 2 which didn't exist in previous
    currentExtraMetadata.put(StreamerUtil.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:150;kafka_metadata%3Atopic%3A1:250;kafka_metadata%3Atopic%3A2:100");

    Map<String, String> previousExtraMetadata = new HashMap<>();
    // Previous only has partitions 0 and 1
    previousExtraMetadata.put(StreamerUtil.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200");

    when(currentMetadata.getExtraMetadata()).thenReturn(currentExtraMetadata);
    when(previousMetadata.getExtraMetadata()).thenReturn(previousExtraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(currentInstant, mockTimeline))
          .thenReturn(currentMetadata);
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(previousInstant, mockTimeline))
          .thenReturn(previousMetadata);

      long result = StreamerUtil.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline);

      // (150-100) + (250-200) + (100-0) = 50 + 50 + 100 = 200
      assertEquals(200L, result);
    }
  }

  @Test
  public void testCalculateKafkaOffsetDifferenceNullInstants() {
    long result = StreamerUtil.calculateKafkaOffsetDifference(null, null, null);
    assertEquals(0L, result);
  }

  @Test
  public void testCalculateKafkaOffsetDifferenceNoMetadata() throws Exception {
    HoodieInstant currentInstant = mock(HoodieInstant.class);
    HoodieInstant previousInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata currentMetadata = mock(HoodieCommitMetadata.class);
    HoodieCommitMetadata previousMetadata = mock(HoodieCommitMetadata.class);

    when(currentInstant.requestedTime()).thenReturn("20250828120000");
    when(previousInstant.requestedTime()).thenReturn("20250828110000");

    Map<String, String> currentExtraMetadata = new HashMap<>();
    Map<String, String> previousExtraMetadata = new HashMap<>();

    when(currentMetadata.getExtraMetadata()).thenReturn(currentExtraMetadata);
    when(previousMetadata.getExtraMetadata()).thenReturn(previousExtraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(currentInstant, mockTimeline))
          .thenReturn(currentMetadata);
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(previousInstant, mockTimeline))
          .thenReturn(previousMetadata);

      long result = StreamerUtil.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline);

      assertEquals(0L, result);
    }
  }

  @Test
  public void testCalculateKafkaOffsetDifferenceEmptyOffsets() throws Exception {
    HoodieInstant currentInstant = mock(HoodieInstant.class);
    HoodieInstant previousInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata currentMetadata = mock(HoodieCommitMetadata.class);
    HoodieCommitMetadata previousMetadata = mock(HoodieCommitMetadata.class);

    when(currentInstant.requestedTime()).thenReturn("20250828120000");
    when(previousInstant.requestedTime()).thenReturn("20250828110000");

    Map<String, String> currentExtraMetadata = new HashMap<>();
    currentExtraMetadata.put(StreamerUtil.HOODIE_METADATA_KEY, "");

    Map<String, String> previousExtraMetadata = new HashMap<>();
    previousExtraMetadata.put(StreamerUtil.HOODIE_METADATA_KEY, "");

    when(currentMetadata.getExtraMetadata()).thenReturn(currentExtraMetadata);
    when(previousMetadata.getExtraMetadata()).thenReturn(previousExtraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(currentInstant, mockTimeline))
          .thenReturn(currentMetadata);
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(previousInstant, mockTimeline))
          .thenReturn(previousMetadata);

      long result = StreamerUtil.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline);

      assertEquals(0L, result);
    }
  }
}
