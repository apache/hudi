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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Generic tests for all {@link KafkaSource} to ensure all implementations properly handle offsets, fetch limits, failure modes, etc.
 */
abstract class BaseTestKafkaSource extends SparkClientFunctionalTestHarness {
  protected static final String TEST_TOPIC_PREFIX = "hoodie_test_";
  protected static KafkaTestUtils testUtils;

  protected final HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);

  protected SchemaProvider schemaProvider;

  @BeforeAll
  public static void initClass() {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterAll
  public static void cleanupClass() {
    testUtils.teardown();
  }

  abstract TypedProperties createPropsForKafkaSource(String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy);

  abstract SourceFormatAdapter createSource(TypedProperties props);

  abstract void sendMessagesToKafka(String topic, int count, int numPartitions);

  @Test
  public void testKafkaSource() {

    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testKafkaSource";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    SourceFormatAdapter kafkaSource = createSource(props);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    Dataset<Row> fetch1AsRows = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
        schemaProvider.getSourceSchema().toString(), kafkaSource.getSource().getSparkSession());
    assertEquals(900, fetch1AsRows.count());

    // 2. Produce new data, extract new data
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(1100, fetch2.getBatch().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    InputBatch<JavaRDD<GenericRecord>> fetch3 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(fetch2.getBatch().get().count(), fetch3.getBatch().get().count());
    assertEquals(fetch2.getCheckpointForNextBatch(), fetch3.getCheckpointForNextBatch());
    // Same using Row API
    InputBatch<Dataset<Row>> fetch3AsRows =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(fetch2.getBatch().get().count(), fetch3AsRows.getBatch().get().count());
    assertEquals(fetch2.getCheckpointForNextBatch(), fetch3AsRows.getCheckpointForNextBatch());

    // 4. Extract with latest checkpoint => no new data returned
    InputBatch<JavaRDD<GenericRecord>> fetch4 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch2.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch4.getBatch());
    // Same using Row API
    InputBatch<Dataset<Row>> fetch4AsRows =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch2.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch4AsRows.getBatch());
  }

  // test case with kafka offset reset strategy
  @Test
  public void testKafkaSourceResetStrategy() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testKafkaSourceResetStrategy";
    testUtils.createTopic(topic, 2);

    TypedProperties earliestProps = createPropsForKafkaSource(topic, null, "earliest");
    SourceFormatAdapter earliestKafkaSource = createSource(earliestProps);

    TypedProperties latestProps = createPropsForKafkaSource(topic, null, "latest");
    SourceFormatAdapter latestKafkaSource = createSource(latestProps);

    // 1. Extract with a none data kafka checkpoint
    // => get a checkpoint string like "hoodie_test,0:0,1:0", latest checkpoint should be equals to earliest checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch0 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    InputBatch<JavaRDD<GenericRecord>> latFetch0 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch0.getBatch(), latFetch0.getBatch());
    assertEquals(earFetch0.getCheckpointForNextBatch(), latFetch0.getCheckpointForNextBatch());

    sendMessagesToKafka(topic, 1000, 2);

    // 2. Extract new checkpoint with a null / empty string pre checkpoint
    // => earliest fetch with max source limit will get all of data and a end offset checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch1 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);

    // => [a null pre checkpoint] latest reset fetch will get a end offset checkpoint same to earliest
    InputBatch<JavaRDD<GenericRecord>> latFetch1 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch1.getCheckpointForNextBatch(), latFetch1.getCheckpointForNextBatch());
  }

  @Test
  public void testProtoKafkaSourceInsertRecordsLessSourceLimit() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testKafkaSourceInsertRecordsLessSourceLimit";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, Long.MAX_VALUE, "earliest");
    SourceFormatAdapter kafkaSource = createSource(props);
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents", "500");

    /*
     1. maxEventsFromKafkaSourceProp set to more than generated insert records
     and sourceLimit less than the generated insert records num.
     */
    sendMessagesToKafka(topic, 400, 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 300);
    assertEquals(300, fetch1.getBatch().get().count());

    /*
     2. Produce new data, extract new data based on sourceLimit
     and sourceLimit less than the generated insert records num.
     */
    sendMessagesToKafka(topic, 600, 2);
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 300);
    assertEquals(300, fetch2.getBatch().get().count());
  }

  @Test
  public void testCommitOffsetToKafka() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testCommitOffsetToKafka";
    testUtils.createTopic(topic, 2);
    List<TopicPartition> topicPartitions = new ArrayList<>();
    TopicPartition topicPartition0 = new TopicPartition(topic, 0);
    topicPartitions.add(topicPartition0);
    TopicPartition topicPartition1 = new TopicPartition(topic, 1);
    topicPartitions.add(topicPartition1);

    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.put(ENABLE_KAFKA_COMMIT_OFFSET.key(), "true");
    SourceFormatAdapter kafkaSource = createSource(props);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    sendMessagesToKafka(topic, 1000, 2);

    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 599);
    // commit to kafka after first batch
    kafkaSource.getSource().onCommit(fetch1.getCheckpointForNextBatch());
    try (KafkaConsumer consumer = new KafkaConsumer(props)) {
      consumer.assign(topicPartitions);

      OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition0);
      assertNotNull(offsetAndMetadata);
      assertEquals(300, offsetAndMetadata.offset());
      offsetAndMetadata = consumer.committed(topicPartition1);
      assertNotNull(offsetAndMetadata);
      assertEquals(299, offsetAndMetadata.offset());
      // end offsets will point to 500 for each partition because we consumed less messages from first batch
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
      assertEquals(500L, endOffsets.get(topicPartition0));
      assertEquals(500L, endOffsets.get(topicPartition1));

      sendMessagesToKafka(topic, 500, 2);
      InputBatch<Dataset<Row>> fetch2 =
          kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);

      // commit to Kafka after second batch is processed completely
      kafkaSource.getSource().onCommit(fetch2.getCheckpointForNextBatch());

      offsetAndMetadata = consumer.committed(topicPartition0);
      assertNotNull(offsetAndMetadata);
      assertEquals(750, offsetAndMetadata.offset());
      offsetAndMetadata = consumer.committed(topicPartition1);
      assertNotNull(offsetAndMetadata);
      assertEquals(750, offsetAndMetadata.offset());

      endOffsets = consumer.endOffsets(topicPartitions);
      assertEquals(750L, endOffsets.get(topicPartition0));
      assertEquals(750L, endOffsets.get(topicPartition1));
    }
    // check failure case
    props.remove(ConsumerConfig.GROUP_ID_CONFIG);
    assertThrows(HoodieNotSupportedException.class,() -> kafkaSource.getSource().onCommit(""));
  }

  @Test
  public void testFailOnDataLoss() throws Exception {
    // create a topic with very short retention
    final String topic = TEST_TOPIC_PREFIX + "testFailOnDataLoss";
    Properties topicConfig = new Properties();
    topicConfig.setProperty("retention.ms", "8000");
    testUtils.createTopic(topic, 1, topicConfig);

    TypedProperties failOnDataLossProps = createPropsForKafkaSource(topic, null, "earliest");
    failOnDataLossProps.setProperty(KafkaOffsetGen.Config.ENABLE_FAIL_ON_DATA_LOSS.key(), Boolean.toString(true));
    SourceFormatAdapter kafkaSource = createSource(failOnDataLossProps);
    sendMessagesToKafka(topic, 10, 2);
    // send 10 records, extract 2 records to generate a checkpoint
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 2);
    assertEquals(2, fetch1.getBatch().get().count());

    // wait for the checkpoint to expire
    Thread.sleep(30000);
    Throwable t = assertThrows(HoodieDeltaStreamerException.class, () -> {
      kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    });
    assertEquals(
        "Some data may have been lost because they are not available in Kafka any more;"
            + " either the data was aged out by Kafka or the topic may have been deleted before all the data in the topic was processed.",
        t.getMessage());
    t = assertThrows(HoodieDeltaStreamerException.class, () -> {
      kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    });
    assertEquals(
        "Some data may have been lost because they are not available in Kafka any more;"
            + " either the data was aged out by Kafka or the topic may have been deleted before all the data in the topic was processed.",
        t.getMessage());
  }
}
