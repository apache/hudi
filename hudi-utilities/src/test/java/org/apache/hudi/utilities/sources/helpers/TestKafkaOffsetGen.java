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
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.LogicalClock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.testutils.KafkaTestUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
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

import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE_STRING;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests against {@link KafkaOffsetGen}.
 */
public class TestKafkaOffsetGen {

  private final String testTopicName = "hoodie_test_" + UUID.randomUUID();
  private final HoodieIngestionMetrics metrics = mock(HoodieIngestionMetrics.class);
  private static KafkaTestUtils testUtils;

  @BeforeAll
  public static void setup() throws Exception {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterAll
  public static void teardown() throws Exception {
    testUtils.teardown();
  }

  @AfterEach
  void cleanupTopics() {
    testUtils.deleteTopics();
  }

  private TypedProperties getConsumerConfigs(String autoOffsetReset, String kafkaCheckpointType) {
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.source.kafka.checkpoint.type", kafkaCheckpointType);
    props.put("auto.offset.reset", autoOffsetReset);
    props.put("hoodie.streamer.source.kafka.topic", testTopicName);
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

    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_STRING));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());

    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 5000, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromLatest() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_STRING));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(1000, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromCheckpoint() {
    String lastCheckpointString = testTopicName + ",0:250";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_STRING));

    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(lastCheckpointString)), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(250, nextOffsetRanges[0].fromOffset());
    assertEquals(750, nextOffsetRanges[0].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromTimestampCheckpointType() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_TIMESTAMP));

    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(String.valueOf(System.currentTimeMillis() - 100000))), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());
  }

  /**
   * When the requested timestamp is later than every record in the topic,
   * {@link org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes} returns {@code null}
   * for every partition. In that case we must fall back to the partition's end offset (its tip),
   * not to offset 0 / earliest — otherwise the entire partition would be replayed even though the
   * user asked for a strictly later checkpoint.
   */
  @Test
  public void testGetNextOffsetRangesFromTimestampCheckpointTypeWithNoOffsetsAfterTimestamp() throws Exception {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    // Ensure the checkpoint we pass is strictly after every published record's timestamp.
    Thread.sleep(10);
    long checkpointAfterProduction = System.currentTimeMillis();

    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_TIMESTAMP));

    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(String.valueOf(checkpointAfterProduction))), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    // Fallback to end offset (the tip): from == until == 1000, nothing to consume.
    assertEquals(1000, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
  }

  /**
   * Mixed case: some partitions have records at/after the requested timestamp and some don't.
   * Only the partitions with no matching record should fall back to their end offset; partitions
   * that do have matching records should still resume at the offset returned by
   * {@link org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes}. This is the user-visible
   * bug the fallback change is targeting.
   */
  @Test
  public void testGetNextOffsetRangesFromTimestampCheckpointTypeWithPartialOffsets() throws Exception {
    testUtils.createTopic(testTopicName, 2);
    int recordsPerPartition = 500;

    // Publish `recordsPerPartition` records to partition 0 first, then take a checkpoint after
    // them. Any record produced later goes to partition 1 and is guaranteed to have a timestamp
    // strictly greater than the checkpoint.
    sendMessagesToPartition(testTopicName, 0, recordsPerPartition);
    Thread.sleep(10);
    long checkpointBetweenBatches = System.currentTimeMillis();
    Thread.sleep(10);
    sendMessagesToPartition(testTopicName, 1, recordsPerPartition);

    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_TIMESTAMP));

    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(String.valueOf(checkpointBetweenBatches))), recordsPerPartition, metrics);

    // computeOffsetRanges may split a single partition into multiple sub-ranges when
    // eventsPerPartition < partition size, so group by partition and verify the aggregate.
    Map<Integer, List<OffsetRange>> byPartition = Arrays.stream(nextOffsetRanges)
        .collect(Collectors.groupingBy(OffsetRange::partition));
    assertEquals(2, byPartition.size(), "expected ranges for exactly 2 partitions");

    // Partition 0: all records predate the checkpoint => fromOffset == untilOffset == 500
    List<OffsetRange> p0Ranges = byPartition.get(0);
    assertEquals(recordsPerPartition, p0Ranges.get(0).fromOffset(),
        "partition 0 should start at the end offset (tip)");
    assertEquals(recordsPerPartition, p0Ranges.get(p0Ranges.size() - 1).untilOffset(),
        "partition 0 should end at the end offset (nothing to consume)");

    // Partition 1: records were produced after the checkpoint => consume from offset 0 to 500
    List<OffsetRange> p1Ranges = byPartition.get(1);
    assertEquals(0, p1Ranges.get(0).fromOffset(),
        "partition 1 should start from offset 0");
    assertEquals(recordsPerPartition, p1Ranges.get(p1Ranges.size() - 1).untilOffset(),
        "partition 1 should consume all records");
    assertEquals(recordsPerPartition, KafkaOffsetGen.CheckpointUtils.totalNewMessages(nextOffsetRanges),
        "total new messages should equal recordsPerPartition");
    for (int i = 0; i < p1Ranges.size() - 1; i++) {
      assertEquals(p1Ranges.get(i).untilOffset(), p1Ranges.get(i + 1).fromOffset(),
          "partition 1 sub-ranges should be contiguous");
    }
  }

  /**
   * Publish {@code count} simple string records to a specific partition of {@code topic}. Used to
   * simulate a mixed "some partitions have records after ts, some don't" state that the default
   * partitioner-based {@link KafkaTestUtils#sendMessages} cannot deterministically produce.
   */
  private void sendMessagesToPartition(String topic, int partition, int count) {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.brokerAddress());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      for (int i = 0; i < count; i++) {
        producer.send(new ProducerRecord<>(topic, partition, null, "msg-" + partition + "-" + i));
      }
      producer.flush();
    }
  }

  @Test
  public void testGetNextOffsetRangesFromSingleOffsetCheckpoint() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET));

    // long positive value of offset => get it
    String lastCheckpointString = "250";
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(lastCheckpointString)), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(250, nextOffsetRanges[0].fromOffset());
    assertEquals(750, nextOffsetRanges[0].untilOffset());

    // negative offset value => get by autoOffsetReset config
    lastCheckpointString = "-2";
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(lastCheckpointString)), 500, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(1000, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());

    // incorrect offset value => get by autoOffsetReset config
    kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET));
    lastCheckpointString = "garbage";
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(lastCheckpointString)), 5000, metrics);
    assertEquals(1, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(1000, nextOffsetRanges[0].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromSingleOffsetCheckpointNotApplicable() {
    testUtils.createTopic(testTopicName, 2);
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET));

    // incorrect number of partitions => exception (number of partitions is more than 1)
    String lastCheckpointString = "250";
    Exception exception = assertThrows(HoodieException.class,
        () -> kafkaOffsetGen.getNextOffsetRanges(
            Option.of(new StreamerCheckpointV2(lastCheckpointString)), 500, metrics));
    assertTrue(exception.getMessage().startsWith("Kafka topic " + testTopicName + " has 2 partitions (more than 1)"));
  }

  @Test
  public void testGetNextOffsetRangesFromMultiplePartitions() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 2);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_STRING));
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 499, metrics);
    assertEquals(3, nextOffsetRanges.length);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(249, nextOffsetRanges[0].untilOffset());
    assertEquals(249, nextOffsetRanges[1].fromOffset());
    assertEquals(250, nextOffsetRanges[1].untilOffset());
    assertEquals(0, nextOffsetRanges[2].fromOffset());
    assertEquals(249, nextOffsetRanges[2].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesFromGroup() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 2);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecordsByPartitions(dataGenerator.generateInserts("000", 1000), 2));
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("group", KAFKA_CHECKPOINT_TYPE_STRING));
    String lastCheckpointString = testTopicName + ",0:250,1:249";
    kafkaOffsetGen.commitOffsetToKafka(lastCheckpointString);
    // don't pass lastCheckpointString as we want to read from group committed offset
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(250, nextOffsetRanges[0].fromOffset());
    assertEquals(400, nextOffsetRanges[0].untilOffset());
    assertEquals(249, nextOffsetRanges[1].fromOffset());
    assertEquals(399, nextOffsetRanges[1].untilOffset());

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

    // committed offsets are not present for the consumer group
    kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("group", KAFKA_CHECKPOINT_TYPE_STRING));
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(500, nextOffsetRanges[0].fromOffset());
    assertEquals(500, nextOffsetRanges[0].untilOffset());
    assertEquals(500, nextOffsetRanges[1].fromOffset());
    assertEquals(500, nextOffsetRanges[1].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesWithMinPartitionsForSinglePartition() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 1);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    TypedProperties props = getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_STRING);

    // default no minPartition set
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(props);
    OffsetRange[] nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(300, nextOffsetRanges[0].untilOffset());

    props.put(KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.key(), 2L);
    // just to check warn-message manually if props contains deprecated config
    props.put(KafkaSourceConfig.KAFKA_FETCH_PARTITION_TIME_OUT.key(), 1L);
    kafkaOffsetGen = new KafkaOffsetGen(props);
    nextOffsetRanges = kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 300, metrics);
    assertEquals(0, nextOffsetRanges[0].fromOffset());
    assertEquals(150, nextOffsetRanges[0].untilOffset());
    assertEquals(150, nextOffsetRanges[1].fromOffset());
    assertEquals(300, nextOffsetRanges[1].untilOffset());
  }

  @Test
  public void testGetNextOffsetRangesWithMinPartitionsForMultiPartition() {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.createTopic(testTopicName, 2);
    testUtils.sendMessages(testTopicName, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    TypedProperties props = getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_STRING);

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
  }

  @Test
  public void testCheckTopicExists() {
    TypedProperties props = getConsumerConfigs("latest", KAFKA_CHECKPOINT_TYPE_STRING);
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(props);
    testUtils.createTopic(testTopicName, 1);
    boolean topicExists = kafkaOffsetGen.checkTopicExists(new KafkaConsumer(props));
    assertTrue(topicExists);
    props.put("hoodie.streamer.source.kafka.topic", "random");
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

  static Stream<Arguments> getOffsetsByTimestampArgs() {
    long ts = System.currentTimeMillis();
    String topicName = "kafka-topic-" + UUID.randomUUID();
    List<TopicPartition> topicPartitions =
        Arrays.asList(new TopicPartition(topicName, 0), new TopicPartition(topicName, 1));

    // end offsets used as fallback when offsetsForTimes returns null
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(topicPartitions.get(0), 50L);
    endOffsets.put(topicPartitions.get(1), 80L);

    // none-null: every partition resolves via offsetsForTimes
    Map<TopicPartition, OffsetAndTimestamp> allResolved = new HashMap<>();
    allResolved.put(topicPartitions.get(0), new OffsetAndTimestamp(20, ts));
    allResolved.put(topicPartitions.get(1), new OffsetAndTimestamp(35, ts));
    String expectedAllResolved = String.format("%s,0:20,1:35", topicName);

    // some-null: one partition resolves, one falls back to end offset
    Map<TopicPartition, OffsetAndTimestamp> someNull = new HashMap<>();
    someNull.put(topicPartitions.get(0), new OffsetAndTimestamp(20, ts));
    someNull.put(topicPartitions.get(1), null);
    String expectedSomeNull = String.format("%s,0:20,1:80", topicName);

    // all-null: every partition falls back to end offset (pre-0.10.0 format / empty partitions)
    Map<TopicPartition, OffsetAndTimestamp> allNull = new HashMap<>();
    allNull.put(topicPartitions.get(0), null);
    allNull.put(topicPartitions.get(1), null);
    String expectedAllNull = String.format("%s,0:50,1:80", topicName);

    return Stream.of(
        Arguments.of(topicName, topicPartitions, endOffsets, allResolved, expectedAllResolved),
        Arguments.of(topicName, topicPartitions, endOffsets, someNull, expectedSomeNull),
        Arguments.of(topicName, topicPartitions, endOffsets, allNull, expectedAllNull)
    );
  }

  @ParameterizedTest
  @MethodSource("getOffsetsByTimestampArgs")
  void testGetOffsetsByTimestamp(
      String topicName,
      List<TopicPartition> topicPartitions,
      Map<TopicPartition, Long> endOffsets,
      Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp,
      String expectedCheckpoint) {
    long timestamp = System.currentTimeMillis();

    KafkaConsumer mockConsumer = mock(KafkaConsumer.class);
    List<PartitionInfo> partitionInfoList = topicPartitions.stream()
        .map(tp -> new org.apache.kafka.common.PartitionInfo(tp.topic(), tp.partition(), null, null, null))
        .collect(Collectors.toList());

    Map<TopicPartition, Long> topicPartitionsTimestamp = new HashMap<>();
    topicPartitions.forEach(tp -> topicPartitionsTimestamp.put(tp, timestamp));

    when(mockConsumer.endOffsets(new HashSet<>(topicPartitions))).thenReturn(endOffsets);
    when(mockConsumer.offsetsForTimes(topicPartitionsTimestamp)).thenReturn(offsetAndTimestamp);

    TypedProperties consumerConfigs = getConsumerConfigs(topicName, "earliest", "string");
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(consumerConfigs);

    Option<String> result = kafkaOffsetGen.getOffsetsByTimestamp(
        mockConsumer, partitionInfoList, new HashSet<>(topicPartitions), topicName, timestamp);

    assertTrue(result.isPresent());
    // Parse both strings into offset maps for order-independent comparison
    assertEquals(
        KafkaOffsetGen.CheckpointUtils.strToOffsets(expectedCheckpoint),
        KafkaOffsetGen.CheckpointUtils.strToOffsets(result.get()));

    // endOffsets must be called BEFORE offsetsForTimes (verified via call order)
    org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(mockConsumer);
    inOrder.verify(mockConsumer).endOffsets(new HashSet<>(topicPartitions));
    inOrder.verify(mockConsumer).offsetsForTimes(topicPartitionsTimestamp);
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

  @Test
  public void testKafkaDelayCountMetricEmittedWithLag() {
    testUtils.createTopic(testTopicName, 1);
    String[] messages = new String[1000];
    for (int i = 0; i < 1000; i++) {
      messages[i] = String.format("{\"id\":\"%d\"}", i);
    }
    testUtils.sendMessages(testTopicName, messages);

    HoodieIngestionMetrics mockMetrics = mock(HoodieIngestionMetrics.class);
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_STRING));

    // Read first 250 messages, then simulate lag
    String lastCheckpointString = testTopicName + ",0:250";
    kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(lastCheckpointString)), 500, mockMetrics);

    // Verify metric was called with lag count of 750 (1000 - 250)
    verify(mockMetrics, times(1)).updateStreamerSourceDelayCount("kafkaDelayCount", 750L);
  }

  @Test
  public void testKafkaDelayCountMetricEmittedWithoutCheckpoint() {
    testUtils.createTopic(testTopicName, 1);
    String[] messages = new String[1000];
    for (int i = 0; i < 1000; i++) {
      messages[i] = String.format("{\"id\":\"%d\"}", i);
    }
    testUtils.sendMessages(testTopicName, messages);

    HoodieIngestionMetrics mockMetrics = mock(HoodieIngestionMetrics.class);
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_STRING));

    // First run without checkpoint
    kafkaOffsetGen.getNextOffsetRanges(Option.empty(), 500, mockMetrics);

    // Verify metric was called with 0 (no checkpoint = no lag)
    verify(mockMetrics, times(1)).updateStreamerSourceDelayCount("kafkaDelayCount", 0L);
  }

  @Test
  public void testKafkaDelayCountMetricEmittedWithMultiplePartitions() {
    testUtils.createTopic(testTopicName, 2);
    String[] messages = new String[1000];
    for (int i = 0; i < 1000; i++) {
      messages[i] = String.format("{\"id\":\"%d\"}", i);
    }
    testUtils.sendMessages(testTopicName, messages);

    HoodieIngestionMetrics mockMetrics = mock(HoodieIngestionMetrics.class);
    KafkaOffsetGen kafkaOffsetGen = new KafkaOffsetGen(getConsumerConfigs("earliest", KAFKA_CHECKPOINT_TYPE_STRING));

    // Checkpoint with some consumed messages, creating lag (0:250, 1:249 = 499)
    // Note: Cannot assert exact delay count because Kafka's message distribution across
    // partitions is non-deterministic when messages don't have explicit partition keys
    String lastCheckpointString = testTopicName + ",0:250,1:249";
    kafkaOffsetGen.getNextOffsetRanges(
        Option.of(new StreamerCheckpointV2(lastCheckpointString)), 300, mockMetrics);

    // Verify metric was called with a reasonable lag count
    ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
    verify(mockMetrics, times(1)).updateStreamerSourceDelayCount(eq("kafkaDelayCount"), delayCaptor.capture());
    assertTrue(delayCaptor.getValue() > 0, "Delay count should be greater than 0 when there is lag");
    assertTrue(delayCaptor.getValue() <= 1000, "Delay count should not exceed total messages sent");
  }
}
