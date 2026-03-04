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

package org.apache.hudi.utils;

import org.apache.hudi.client.model.PartialUpdateFlinkRecordMerger;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.util.KafkaOffsetParseUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link StreamerUtil}.
 */
class TestStreamerUtil {

  @TempDir
  File tempFile;

  @Test
  void testInferMergingBehavior() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    // default merge behavior
    Triple<RecordMergeMode, String, String> mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(EventTimeAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set commit time merge mode
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(OverwriteWithLatestAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set partial update merger.
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.RECORD_MERGER_IMPLS, PartialUpdateFlinkRecordMerger.class.getName());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(PartialUpdateAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set partial update payload
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, PartialUpdateAvroPayload.class.getName());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(PartialUpdateAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set partial update payload
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, PartialUpdateAvroPayload.class.getName());
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.EIGHT.versionCode());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(PartialUpdateAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());
  }

  @Test
  void testInitTableWithSpecificVersion() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for partitioned table.
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "p0,p1");
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.SIX.versionCode());
    StreamerUtil.initTableIfNotExists(conf);

    // Validate the partition fields & preCombineField in hoodie.properties.
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertArrayEquals(metaClient1.getTableConfig().getPartitionFields().get(), new String[] {"p0", "p1"});
    assertNotNull(metaClient1.getTableConfig().getKeyGeneratorClassName());
    assertEquals(HoodieTableVersion.SIX, metaClient1.getTableConfig().getTableVersion());
  }

  @Test
  void testInitTableIfNotExists() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for partitioned table.
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "p0,p1");
    StreamerUtil.initTableIfNotExists(conf);

    // Validate the partition fields & preCombineField in hoodie.properties.
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertTrue(metaClient1.getTableConfig().getPartitionFields().isPresent(),
        "Missing partition columns in the hoodie.properties.");
    assertArrayEquals(metaClient1.getTableConfig().getPartitionFields().get(), new String[] {"p0", "p1"});
    assertEquals(metaClient1.getTableConfig().getOrderingFieldsStr().get(), "ts");
    assertEquals(metaClient1.getTableConfig().getKeyGeneratorClassName(), SimpleAvroKeyGenerator.class.getName());
    assertEquals(HoodieTableVersion.current(), metaClient1.getTableConfig().getTableVersion());

    // Test for non-partitioned table.
    conf.removeConfig(FlinkOptions.PARTITION_PATH_FIELD);
    FileIOUtils.deleteDirectory(tempFile);
    StreamerUtil.initTableIfNotExists(conf);
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertFalse(metaClient2.getTableConfig().getPartitionFields().isPresent());
    assertEquals(metaClient2.getTableConfig().getKeyGeneratorClassName(), SimpleAvroKeyGenerator.class.getName());
  }

  @Test
  void testMedianInstantTime() {
    String higher = "20210705125921";
    String lower = "20210705125806";
    String expectedMedianInstant = "20210705125844499";
    String median1 = StreamerUtil.medianInstantTime(higher, lower).get();
    assertThat(median1, is(expectedMedianInstant));
    // test symmetry
    assertThrows(IllegalArgumentException.class,
        () -> StreamerUtil.medianInstantTime(lower, higher),
        "The first argument should have newer instant time");
    // test very near instant time
    assertFalse(StreamerUtil.medianInstantTime("20211116115634", "20211116115633").isPresent());
  }

  @Test
  void testInstantTimeDiff() {
    String higher = "20210705125921";
    String lower = "20210705125806";
    long diff = StreamerUtil.instantTimeDiffSeconds(higher, lower);
    assertThat(diff, is(75L));
  }

  @Test
  public void testAddCheckpointIdIntoMetadata() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for write extra metadata.
    conf.set(FlinkOptions.WRITE_EXTRA_METADATA_ENABLED, true);

    HashMap<String, String> metadata = new HashMap<>();
    StreamerUtil.addFlinkCheckpointIdIntoMetaData(conf, metadata, 123L);
    assertEquals(metadata.get(StreamerUtil.FLINK_CHECKPOINT_ID), "123");
  }

  @Test
  void testTableExist() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    String basePath = tempFile.getAbsolutePath();

    assertFalse(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));

    try (FileSystem fs = HadoopFSUtils.getFs(basePath, HadoopConfigurations.getHadoopConf(conf))) {
      fs.mkdirs(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
      assertFalse(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));

      fs.create(new Path(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME), HoodieTableConfig.HOODIE_PROPERTIES_FILE));
      assertTrue(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));
    }
  }

  // ---- KafkaOffsetParseUtils tests ----

  @Test
  void testExtractKafkaOffsetMetadata() throws Exception {
    HoodieInstant mockInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata mockCommitMetadata = mock(HoodieCommitMetadata.class);

    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(KafkaOffsetParseUtils.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200");

    when(mockCommitMetadata.getExtraMetadata()).thenReturn(extraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(any(HoodieInstant.class), any(HoodieTimeline.class)))
          .thenReturn(mockCommitMetadata);

      String result = KafkaOffsetParseUtils.extractKafkaOffsetMetadata(mockInstant, mockTimeline);
      assertEquals("kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200", result);
    }
  }

  @Test
  void testExtractKafkaOffsetMetadataNotFound() throws Exception {
    HoodieInstant mockInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata mockCommitMetadata = mock(HoodieCommitMetadata.class);

    Map<String, String> extraMetadata = new HashMap<>();

    when(mockCommitMetadata.getExtraMetadata()).thenReturn(extraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(any(HoodieInstant.class), any(HoodieTimeline.class)))
          .thenReturn(mockCommitMetadata);

      String result = KafkaOffsetParseUtils.extractKafkaOffsetMetadata(mockInstant, mockTimeline);
      assertNull(result);
    }
  }

  @Test
  void testParseKafkaOffsetsValidInput() {
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200;kafka_metadata%3Atopic%3A2:300";

    Map<Integer, Long> result = KafkaOffsetParseUtils.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(3, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(200L, result.get(1));
    assertEquals(300L, result.get(2));
  }

  @Test
  void testParseKafkaOffsetsEmptyString() {
    Map<Integer, Long> result = KafkaOffsetParseUtils.parseKafkaOffsets("");
    assertTrue(result.isEmpty());
  }

  @Test
  void testParseKafkaOffsetsNullString() {
    Map<Integer, Long> result = KafkaOffsetParseUtils.parseKafkaOffsets(null);
    assertTrue(result.isEmpty());
  }

  @Test
  void testParseKafkaOffsetsWithClusterMetadata() {
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;kafka_cluster%3Atopicname%3Aclustername;kafka_metadata%3Atopic%3A1:200";

    Map<Integer, Long> result = KafkaOffsetParseUtils.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(2, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(200L, result.get(1));
  }

  @Test
  void testParseKafkaOffsetsMalformedEntry() {
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;invalidentry;kafka_metadata%3Atopic%3A1:200";

    Map<Integer, Long> result = KafkaOffsetParseUtils.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(2, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(200L, result.get(1));
  }

  @Test
  void testParseKafkaOffsetsInvalidNumber() {
    String kafkaOffsetString = "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:notanumber;kafka_metadata%3Atopic%3A2:300";

    Map<Integer, Long> result = KafkaOffsetParseUtils.parseKafkaOffsets(kafkaOffsetString);

    assertEquals(2, result.size());
    assertEquals(100L, result.get(0));
    assertEquals(300L, result.get(2));
  }

  @Test
  void testCalculateKafkaOffsetDifference() throws Exception {
    HoodieInstant currentInstant = mock(HoodieInstant.class);
    HoodieInstant previousInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata currentMetadata = mock(HoodieCommitMetadata.class);
    HoodieCommitMetadata previousMetadata = mock(HoodieCommitMetadata.class);

    when(currentInstant.requestedTime()).thenReturn("20250828120000");
    when(previousInstant.requestedTime()).thenReturn("20250828110000");

    Map<String, String> currentExtraMetadata = new HashMap<>();
    currentExtraMetadata.put(KafkaOffsetParseUtils.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:150;kafka_metadata%3Atopic%3A1:250;kafka_metadata%3Atopic%3A2:350");

    Map<String, String> previousExtraMetadata = new HashMap<>();
    previousExtraMetadata.put(KafkaOffsetParseUtils.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200;kafka_metadata%3Atopic%3A2:300");

    when(currentMetadata.getExtraMetadata()).thenReturn(currentExtraMetadata);
    when(previousMetadata.getExtraMetadata()).thenReturn(previousExtraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(currentInstant, mockTimeline))
          .thenReturn(currentMetadata);
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(previousInstant, mockTimeline))
          .thenReturn(previousMetadata);

      long result = KafkaOffsetParseUtils.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline);

      // (150-100) + (250-200) + (350-300) = 50 + 50 + 50 = 150
      assertEquals(150L, result);
    }
  }

  @Test
  void testCalculateKafkaOffsetDifferenceWithNewPartition() throws Exception {
    HoodieInstant currentInstant = mock(HoodieInstant.class);
    HoodieInstant previousInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata currentMetadata = mock(HoodieCommitMetadata.class);
    HoodieCommitMetadata previousMetadata = mock(HoodieCommitMetadata.class);

    when(currentInstant.requestedTime()).thenReturn("20250828120000");
    when(previousInstant.requestedTime()).thenReturn("20250828110000");

    Map<String, String> currentExtraMetadata = new HashMap<>();
    currentExtraMetadata.put(KafkaOffsetParseUtils.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:150;kafka_metadata%3Atopic%3A1:250;kafka_metadata%3Atopic%3A2:100");

    Map<String, String> previousExtraMetadata = new HashMap<>();
    previousExtraMetadata.put(KafkaOffsetParseUtils.HOODIE_METADATA_KEY, "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200");

    when(currentMetadata.getExtraMetadata()).thenReturn(currentExtraMetadata);
    when(previousMetadata.getExtraMetadata()).thenReturn(previousExtraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(currentInstant, mockTimeline))
          .thenReturn(currentMetadata);
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(previousInstant, mockTimeline))
          .thenReturn(previousMetadata);

      long result = KafkaOffsetParseUtils.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline);

      // (150-100) + (250-200) + (100-0) = 50 + 50 + 100 = 200
      assertEquals(200L, result);
    }
  }

  @Test
  void testCalculateKafkaOffsetDifferenceNullInstants() {
    assertThrows(IllegalArgumentException.class, () ->
        KafkaOffsetParseUtils.calculateKafkaOffsetDifference(null, null, null));
  }

  @Test
  void testCalculateKafkaOffsetDifferenceNoMetadata() throws Exception {
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

      assertThrows(HoodieException.class, () ->
          KafkaOffsetParseUtils.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline));
    }
  }

  @Test
  void testCalculateKafkaOffsetDifferenceEmptyOffsets() throws Exception {
    HoodieInstant currentInstant = mock(HoodieInstant.class);
    HoodieInstant previousInstant = mock(HoodieInstant.class);
    HoodieTimeline mockTimeline = mock(HoodieTimeline.class);
    HoodieCommitMetadata currentMetadata = mock(HoodieCommitMetadata.class);
    HoodieCommitMetadata previousMetadata = mock(HoodieCommitMetadata.class);

    when(currentInstant.requestedTime()).thenReturn("20250828120000");
    when(previousInstant.requestedTime()).thenReturn("20250828110000");

    Map<String, String> currentExtraMetadata = new HashMap<>();
    currentExtraMetadata.put(KafkaOffsetParseUtils.HOODIE_METADATA_KEY, "");

    Map<String, String> previousExtraMetadata = new HashMap<>();
    previousExtraMetadata.put(KafkaOffsetParseUtils.HOODIE_METADATA_KEY, "");

    when(currentMetadata.getExtraMetadata()).thenReturn(currentExtraMetadata);
    when(previousMetadata.getExtraMetadata()).thenReturn(previousExtraMetadata);

    try (MockedStatic<TimelineUtils> mockedTimelineUtils = Mockito.mockStatic(TimelineUtils.class)) {
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(currentInstant, mockTimeline))
          .thenReturn(currentMetadata);
      mockedTimelineUtils.when(() -> TimelineUtils.getCommitMetadata(previousInstant, mockTimeline))
          .thenReturn(previousMetadata);

      assertThrows(HoodieException.class, () ->
          KafkaOffsetParseUtils.calculateKafkaOffsetDifference(currentInstant, previousInstant, mockTimeline));
    }
  }
}

