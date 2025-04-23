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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers;
import org.apache.hudi.common.util.LogicalClock;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.kafka010.KafkaOffsetRange;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.collection.JavaConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests against {@link KafkaOffsetGen}.
 */
class TestKafkaOffsetGen {

  private final String testTopicName = "hoodie_test_" + UUID.randomUUID();
  private static KafkaTestUtils testUtils;
  private HoodieIngestionMetrics metrics = mock(HoodieIngestionMetrics.class);

  @BeforeAll
  static void setup() {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterAll
  static void teardown() {
    testUtils.teardown();
  }

  private TypedProperties getConsumerConfigs(String autoOffsetReset, String kafkaCheckpointType) {
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.source.kafka.checkpoint.type", kafkaCheckpointType);
    props.put("auto.offset.reset", autoOffsetReset);
    props.put("hoodie.deltastreamer.source.kafka.topic", testTopicName);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("key.deserializer", StringDeserializer.class.getName());
    props.setProperty("value.deserializer", StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  private TypedProperties getConsumerConfigs(String topicName, String autoOffsetReset, String kafkaCheckpointType) {
    TypedProperties props = getConsumerConfigs(autoOffsetReset, kafkaCheckpointType);
    props.put("hoodie.streamer.source.kafka.topic", topicName);
    return props;
  }

  @Test
  public void testGetNextOffsetRangesFromEarliest() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", "string"));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);

    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 5000, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testGetNextOffsetRangesFromLatest() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", "string"));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(1000, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testGetNextOffsetRangesFromCheckpoint() {
    String lastCheckpointString = testTopicName + ",0:250";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", "string"));

    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.of(lastCheckpointString), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(250, nextOffsetRanges[0].fromOffset());
    assertEquals(750, nextOffsetRanges[0].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testGetNextOffsetRangesFromTimestampCheckpointType() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", "timestamp"));

    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.of(String.valueOf(System.currentTimeMillis() - 100000)), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testGetNextOffsetRangesFromMultiplePartitions() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 2);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", "string"));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 499, metrics);
    assertEquals(3, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(249, nextOffsetRanges[0].untilOffset());
    assertEquals(249, nextOffsetRanges[1].fromOffset());
    assertEquals(250, nextOffsetRanges[1].untilOffset());
    assertEquals(0, nextOffsetRanges[2].fromOffset());
    assertEquals(249, nextOffsetRanges[2].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testGetNextOffsetRangesFromGroup() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 2);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecordsByPartitions(dataGenerator.generateInserts("000", 1000), 2));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("group", "string"));
    String lastCheckpointString = testTopicName + ",0:250,1:249";
    kafkaOffsetGen.commitOffsetToKafka(lastCheckpointString);
    // don't pass lastCheckpointString as we want to read from group committed offset
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(250, nextOffsetRanges[0].fromOffset());
    assertEquals(400, nextOffsetRanges[0].untilOffset());
    assertEquals(249, nextOffsetRanges[1].fromOffset());
    assertEquals(399, nextOffsetRanges[1].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);

    // try w/ 1 partition already exhausted. both partitions need to be returned as part of offset ranges
    lastCheckpointString = testTopicName + ",0:400,1:500";
    kafkaOffsetGen.commitOffsetToKafka(lastCheckpointString);
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(3, nextOffsetRanges.length);
    assertEquals(400, nextOffsetRanges[0].fromOffset());
    assertEquals(450, nextOffsetRanges[0].untilOffset());
    assertEquals(450, nextOffsetRanges[1].fromOffset());
    assertEquals(500, nextOffsetRanges[1].untilOffset());
    assertEquals(0, nextOffsetRanges[1].partition());
    assertEquals(500, nextOffsetRanges[2].fromOffset());
    assertEquals(500, nextOffsetRanges[2].untilOffset());
    assertEquals(1, nextOffsetRanges[2].partition());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);

    // if there is just 1 msg to consume from just 1 partition.
    lastCheckpointString = testTopicName + ",0:499,1:500";
    kafkaOffsetGen.commitOffsetToKafka(lastCheckpointString);
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(2, nextOffsetRanges.length);
    assertEquals(499, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());
    assertEquals(0, nextOffsetRanges[0].partition());
    assertEquals(500, nextOffsetRanges[1].fromOffset());
    assertEquals(500, nextOffsetRanges[1].untilOffset());
    assertEquals(1, nextOffsetRanges[1].partition());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);

    // committed offsets are not present for the consumer group
    kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("group", "string"));
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(500, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());
    assertEquals(500, nextOffsetRanges[1].fromOffset());
    assertEquals(500, nextOffsetRanges[1].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testGetNextOffsetRangesWithMinPartitionsForSinglePartition() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    TypedProperties props = getConsumerConfigs("earliest", "string");

    // default no minPartition set
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(props);
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(300, nextOffsetRanges[0].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);

    props.put(KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.key(), 2L);
    kafkaOffsetGen = new KafkaOffsetGen(props);
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(150, nextOffsetRanges[0].untilOffset());
    assertEquals(150, nextOffsetRanges[1].fromOffset());
    assertEquals(300, nextOffsetRanges[1].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testGetNextOffsetRangesWithMinPartitionsForMultiPartition() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 2);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    TypedProperties props = getConsumerConfigs("earliest", "string");

    // default no minPartition or minPartition less than TopicPartitions
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(props);
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(2, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].partition());
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(150, nextOffsetRanges[0].untilOffset());
    assertEquals(1, nextOffsetRanges[1].partition());
    assertEquals(0, nextOffsetRanges[1].fromOffset());
    assertEquals(150, nextOffsetRanges[1].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);

    props.put(KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.key(), 1L);
    kafkaOffsetGen = new KafkaOffsetGen(props);
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(2, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].partition());
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(150, nextOffsetRanges[0].untilOffset());
    assertEquals(1, nextOffsetRanges[1].partition());
    assertEquals(0, nextOffsetRanges[1].fromOffset());
    assertEquals(150, nextOffsetRanges[1].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);

    // minPartition more than TopicPartitions
    props.put(KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.key(), 4L);
    kafkaOffsetGen = new KafkaOffsetGen(props);
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(4, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].partition());
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(75, nextOffsetRanges[0].untilOffset());
    assertEquals(0, nextOffsetRanges[1].partition());
    assertEquals(75, nextOffsetRanges[1].fromOffset());
    assertEquals(150, nextOffsetRanges[1].untilOffset());
    assertEquals(1, nextOffsetRanges[2].partition());
    assertEquals(0, nextOffsetRanges[2].fromOffset());
    assertEquals(75, nextOffsetRanges[2].untilOffset());
    assertEquals(1, nextOffsetRanges[3].partition());
    assertEquals(75, nextOffsetRanges[3].fromOffset());
    assertEquals(150, nextOffsetRanges[3].untilOffset());
    assertKafkaOffsetRangesFromSQLLibrary(kafkaOffsetGen, nextOffsetRanges);
  }

  @Test
  public void testCheckTopicExists() {
    TypedProperties props = getConsumerConfigs("latest", "string");
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(props);
    testUtils.createTopic(testTopicName, 1);
    boolean topicExists = kafkaOffsetGen.checkTopicExists(new KafkaConsumer(props));
    assertTrue(topicExists);
    props.put("hoodie.deltastreamer.source.kafka.topic", "random");
    kafkaOffsetGen = new KafkaOffsetGen(props);
    topicExists = kafkaOffsetGen.checkTopicExists(new KafkaConsumer(props));
    assertFalse(topicExists);
  }

  @Test
  public void testTopicNameNotPresentInProps() {
    assertThrows(HoodieNotSupportedException.class, () -> new KafkaOffsetGen(new TypedProperties()));
  }

  @ParameterizedTest
  @CsvSource(value = {"null", "86400000", "-1"})
  void testGetTopicRetentionMs(String retentionMsStr) {
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", "string"));
    Long retentionMs = "null".equals(retentionMsStr) ? null : Long.valueOf(retentionMsStr);
    if (retentionMs != null) {
      Properties props = new Properties();
      props.put(TopicConfig.RETENTION_MS_CONFIG, retentionMsStr);
      testUtils.createTopic(testTopicName, 1, props);
    } else {
      testUtils.createTopic(testTopicName, 1);
    }

    Long topicRetentionMs = kafkaOffsetGen.getTopicRetentionMs(testTopicName);
    if (retentionMs != null) {
      assertEquals(retentionMs, topicRetentionMs);
    } else {
      assertEquals(TimeUnit.DAYS.toMillis(7), topicRetentionMs);
    }
  }

  static Stream<Arguments> getNullRetentionMsTopicConfigTestArgs() {
    return Stream.of(
        // RETENTION_MS is null
        Arguments.of(
            new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, null)))),
        // RETENTION_MS is not present
        Arguments.of(
            new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, "1024")))));
  }

  @ParameterizedTest
  @MethodSource("getNullRetentionMsTopicConfigTestArgs")
  void testGetTopicRetentionMs_retentionMsConfigIsNullOrNotSet(Config topicConfig) {
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", "string"));
    try (MockedStatic<AdminClient> staticMock = mockStatic(AdminClient.class)) {
      mockDescribeTopicConfigs(staticMock, kafkaOffsetGen.getKafkaParams(), topicConfig);

      Long topicRetentionMs = kafkaOffsetGen.getTopicRetentionMs(testTopicName);
      assertNull(topicRetentionMs);
    }
  }

  @Test
  void testGetTopicRetentionMs_Failure() {
    // In case of KafkaException, retentionMs will be null
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", "string"));
    try (MockedStatic<AdminClient> staticMock = mockStatic(AdminClient.class)) {
      AdminClient mock = mock(AdminClient.class);
      staticMock.when(() -> AdminClient.create(kafkaOffsetGen.getKafkaParams())).thenReturn(mock);
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
      when(mock.describeConfigs(Collections.singleton(resource))).thenThrow(new KafkaException("something went wrong"));

      Long topicRetentionMs = kafkaOffsetGen.getTopicRetentionMs(testTopicName);
      assertNull(topicRetentionMs);
    }
  }

  @Test
  void testResolveFromOffsetsWithRetention_skipBufferMinutesNotSet() {
    KafkaConsumer mockConsumer = mock(KafkaConsumer.class);
    Map<TopicPartition, Long> fromOffsets = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:10,1:23", testTopicName));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", "string"));
    Map<TopicPartition, Long> offsets =
        kafkaOffsetGen.resolveFromOffsetsWithRetention(mockConsumer, new HashMap<>(fromOffsets), Collections.emptySet());
    assertEquals(fromOffsets, offsets);
    verify(mockConsumer, never()).offsetsForTimes(any());
  }

  @Test
  void testResolveFromOffsetsWithRetention_topicRetentionMsNotSet() {
    KafkaConsumer mockConsumer = mock(KafkaConsumer.class);
    Map<TopicPartition, Long> fromOffsets = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:10,1:23", testTopicName));

    TypedProperties consumerConfigs = getConsumerConfigs("earliest", "string");
    consumerConfigs.put(KafkaSourceConfig.OFFSET_SKIP_BUFFER_MINUTES.key(), "30");
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(consumerConfigs);

    try (MockedStatic<AdminClient> staticMock = mockStatic(AdminClient.class)) {
      mockDescribeTopicConfigs(staticMock, kafkaOffsetGen.getKafkaParams(), new Config(Collections.emptyList()));

      Map<TopicPartition, Long> offsets =
          kafkaOffsetGen.resolveFromOffsetsWithRetention(mockConsumer, new HashMap<>(fromOffsets), Collections.emptySet());
      assertEquals(fromOffsets, offsets);
      verify(mockConsumer, never()).offsetsForTimes(any());
      Long topicRetentionMs = kafkaOffsetGen.getTopicRetentionMs(testTopicName);
      assertNull(topicRetentionMs);
    }
  }

  @Test
  void testResolveFromOffsetsWithRetention_failureWhenGettingOffsetsForTimes() {
    long retentionMs = 7200000;
    long currentEpochMillis = Instant.now().toEpochMilli();
    long offsetSkipIntervalMinutes = 30;
    final long retentionTs = currentEpochMillis - retentionMs + TimeUnit.MINUTES.toMillis(offsetSkipIntervalMinutes);
    Map<TopicPartition, Long> fromOffsets = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:10,1:23", testTopicName));
    Map<TopicPartition, Long> topicPartitionsTimestamp = fromOffsets.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> retentionTs));

    // setup KafkaConsumer mocks
    KafkaConsumer mockConsumer = mock(KafkaConsumer.class);
    LogicalClock mockClock = mock(LogicalClock.class);
    when(mockConsumer.offsetsForTimes(topicPartitionsTimestamp)).thenThrow(new KafkaException("something went wrong"));
    when(mockClock.currentEpoch()).thenReturn(currentEpochMillis);

    TypedProperties consumerConfigs = getConsumerConfigs("earliest", "string");
    consumerConfigs.put(KafkaSourceConfig.OFFSET_SKIP_BUFFER_MINUTES.key(), String.valueOf(offsetSkipIntervalMinutes));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(consumerConfigs, mockClock);

    try (MockedStatic<AdminClient> staticMock = mockStatic(AdminClient.class)) {
      Config topicConfig = new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs))));
      mockDescribeTopicConfigs(staticMock, kafkaOffsetGen.getKafkaParams(), topicConfig);

      Map<TopicPartition, Long> offsets =
          kafkaOffsetGen.resolveFromOffsetsWithRetention(mockConsumer, new HashMap<>(fromOffsets), fromOffsets.keySet());
      assertEquals(fromOffsets, offsets);
      verify(mockConsumer, times(1)).offsetsForTimes(topicPartitionsTimestamp);
      verify(mockClock, times(1)).currentEpoch();
    }
  }

  static Stream<Arguments> resolveEarliestOffsetsWithRetentionTestArgs() {
    long ts = System.currentTimeMillis();
    String topicName = "kafka-topic-" + UUID.randomUUID();
    // earliest offsets
    Map<TopicPartition, Long> fromOffsets = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:10,1:23", topicName));
    List<TopicPartition> topicPartitions =
        Arrays.asList(new TopicPartition(topicName, 0), new TopicPartition(topicName, 1));

    // offsets are available for all partitions
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp1 = new HashMap<>();
    offsetAndTimestamp1.put(topicPartitions.get(0), new OffsetAndTimestamp(15, ts));
    offsetAndTimestamp1.put(topicPartitions.get(1), new OffsetAndTimestamp(26, ts));
    Map<TopicPartition, Long> expectedOffsets1 = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:15,1:26", topicName));

    // offsets are not available for some partitions
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp2 = new HashMap<>();
    offsetAndTimestamp2.put(topicPartitions.get(0), new OffsetAndTimestamp(15, ts));
    offsetAndTimestamp2.put(topicPartitions.get(1), null);
    Map<TopicPartition, Long> expectedOffsets2 = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:15,1:23", topicName));

    // offsets are not available for any partitions
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp3 = new HashMap<>();
    offsetAndTimestamp3.put(topicPartitions.get(0), null);
    offsetAndTimestamp3.put(topicPartitions.get(1), null);

    // fromOffsets are derived from lastCheckpoint and there are offsets
    // available after retention + buffer time
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp4 = new HashMap<>();
    offsetAndTimestamp4.put(topicPartitions.get(0), new OffsetAndTimestamp(15, ts));
    offsetAndTimestamp4.put(topicPartitions.get(1), new OffsetAndTimestamp(26, ts));
    Map<TopicPartition, Long> fromOffsets4 = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:25,1:37", topicName));
    Map<TopicPartition, Long> expectedOffsets4 = KafkaOffsetGen.CheckpointUtils.strToOffsets(String.format("%s,0:25,1:37", topicName));

    return Stream.of(
        Arguments.of(fromOffsets, topicPartitions, offsetAndTimestamp1, expectedOffsets1),
        Arguments.of(fromOffsets, topicPartitions, offsetAndTimestamp2, expectedOffsets2),
        Arguments.of(fromOffsets, topicPartitions, offsetAndTimestamp3, fromOffsets),
        Arguments.of(fromOffsets4, topicPartitions, offsetAndTimestamp4, expectedOffsets4)
    );
  }

  @ParameterizedTest
  @MethodSource("resolveEarliestOffsetsWithRetentionTestArgs")
  void testResolveEarliestOffsetsWithRetention(
      Map<TopicPartition, Long> fromOffsets,
      List<TopicPartition> topicPartitions,
      Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp,
      Map<TopicPartition, Long> expectedOffsets) {
    String topicName = topicPartitions.get(0).topic();
    long retentionMs = 7200000;
    long skipOffsetBufferMinutes = 30;
    long currentEpochMillis = Instant.now().toEpochMilli();

    long retentionTs = currentEpochMillis - retentionMs + TimeUnit.MINUTES.toMillis(skipOffsetBufferMinutes);
    KafkaConsumer mockConsumer = mock(KafkaConsumer.class);
    LogicalClock mockClock = mock(LogicalClock.class);

    TypedProperties consumerConfigs = getConsumerConfigs(topicName, "earliest", "string");
    consumerConfigs.put(KafkaSourceConfig.OFFSET_SKIP_BUFFER_MINUTES.key(), String.valueOf(skipOffsetBufferMinutes));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(consumerConfigs, mockClock);

    // topic partition timestamp
    Map<TopicPartition, Long> topicPartitionsTimestamp = new HashMap<>();
    topicPartitionsTimestamp.put(topicPartitions.get(0), retentionTs);
    topicPartitionsTimestamp.put(topicPartitions.get(1), retentionTs);

    when(mockConsumer.offsetsForTimes(topicPartitionsTimestamp)).thenReturn(offsetAndTimestamp);
    when(mockClock.currentEpoch()).thenReturn(currentEpochMillis);

    try (MockedStatic<AdminClient> staticMock = mockStatic(AdminClient.class)) {
      Config topicConfig = new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs))));
      mockDescribeTopicConfigs(staticMock, kafkaOffsetGen.getKafkaParams(), topicConfig, topicName);

      Map<TopicPartition, Long> offsets = kafkaOffsetGen.resolveFromOffsetsWithRetention(mockConsumer, fromOffsets, new HashSet<>(topicPartitions));
      assertEquals(expectedOffsets, offsets);
      verify(mockClock, times(1)).currentEpoch();
      verify(mockConsumer, times(1)).offsetsForTimes(topicPartitionsTimestamp);
    }
  }

  void mockDescribeTopicConfigs(MockedStatic<AdminClient> staticMock, Map kafkaParams, Config topicConfig) {
    mockDescribeTopicConfigs(staticMock, kafkaParams, topicConfig, testTopicName);
  }

  void mockDescribeTopicConfigs(MockedStatic<AdminClient> staticMock, Map kafkaParams, Config topicConfig, String topicName) {
    AdminClient mock = mock(AdminClient.class);
    staticMock.when(() -> AdminClient.create(kafkaParams)).thenReturn(mock);

    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    DescribeConfigsResult mockResult = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> future = KafkaFuture.completedFuture(Collections.singletonMap(resource, topicConfig));

    when(mock.describeConfigs(Collections.singleton(resource))).thenReturn(mockResult);
    when(mockResult.all()).thenReturn(future);
  }

  void assertKafkaOffsetRangesFromSQLLibrary(KafkaOffsetGen kafkaOffsetGen, OffsetRange[] offsetRangesFromStreaming) {
    KafkaOffsetRange[] kafkaOffsetRangesFromSQL = JavaConverters.seqAsJavaList(kafkaOffsetGen.toKafkaOffsetRanges(offsetRangesFromStreaming)).toArray(new KafkaOffsetRange[0]);
    assertEquals(kafkaOffsetRangesFromSQL.length, offsetRangesFromStreaming.length);
    for (int i = 0; i < kafkaOffsetRangesFromSQL.length; i++) {
      assertEquals(kafkaOffsetRangesFromSQL[i].topic(), offsetRangesFromStreaming[i].topic());
      assertEquals(kafkaOffsetRangesFromSQL[i].fromOffset(), offsetRangesFromStreaming[i].fromOffset());
      assertEquals(kafkaOffsetRangesFromSQL[i].untilOffset(), offsetRangesFromStreaming[i].untilOffset());
    }
  }
}
