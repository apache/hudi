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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecords;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecordsByPartitions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Tests against {@link JsonKafkaSource}.
 */
public class TestJsonKafkaSource extends SparkClientFunctionalTestHarness {

  protected static final String TEST_TOPIC_PREFIX = "hoodie_test_";
  private static final URL SCHEMA_FILE_URL = TestJsonKafkaSource.class.getClassLoader().getResource("delta-streamer-config/source.avsc");
  protected static KafkaTestUtils testUtils;

  protected final HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);
  protected FilebasedSchemaProvider schemaProvider;

  @BeforeAll
  public static void initClass() throws Exception {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterAll
  public static void cleanupClass() {
    testUtils.teardown();
  }

  @BeforeEach
  public void init() throws Exception {
    String schemaFilePath = Objects.requireNonNull(SCHEMA_FILE_URL).toURI().getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    schemaProvider = new FilebasedSchemaProvider(props, jsc());
  }

  protected TypedProperties createPropsForJsonSource(String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", topic);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("auto.offset.reset", resetStrategy);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.defaultValue()));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  @Test
  public void testJsonKafkaSource() {

    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSource";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, null, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    Dataset<Row> fetch1AsRows = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
        schemaProvider.getSourceSchema().toString(), jsonSource.getSparkSession());
    assertEquals(900, fetch1AsRows.count());

    // 2. Produce new data, extract new data
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
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

  // test whether empty messages can be filtered
  @Test
  public void testJsonKafkaSourceFilterNullMsg() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceFilterNullMsg";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, null, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    // Send  1000 non-null messages to Kafka
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    // Send  100 null messages to Kafka
    testUtils.sendMessages(topic, new String[100]);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    // Verify that messages with null values are filtered
    assertEquals(1000, fetch1.getBatch().get().count());
  }

  // test case with kafka offset reset strategy
  @Test
  public void testJsonKafkaSourceResetStrategy() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceResetStrategy";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();

    TypedProperties earliestProps = createPropsForJsonSource(topic, null, "earliest");
    Source earliestJsonSource = new JsonKafkaSource(earliestProps, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter earliestKafkaSource = new SourceFormatAdapter(earliestJsonSource);

    TypedProperties latestProps = createPropsForJsonSource(topic, null, "latest");
    Source latestJsonSource = new JsonKafkaSource(latestProps, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter latestKafkaSource = new SourceFormatAdapter(latestJsonSource);

    // 1. Extract with a none data kafka checkpoint
    // => get a checkpoint string like "hoodie_test,0:0,1:0", latest checkpoint should be equals to earliest checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch0 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    InputBatch<JavaRDD<GenericRecord>> latFetch0 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch0.getBatch(), latFetch0.getBatch());
    assertEquals(earFetch0.getCheckpointForNextBatch(), latFetch0.getCheckpointForNextBatch());

    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    // 2. Extract new checkpoint with a null / empty string pre checkpoint
    // => earliest fetch with max source limit will get all of data and a end offset checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch1 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);

    // => [a null pre checkpoint] latest reset fetch will get a end offset checkpoint same to earliest
    InputBatch<JavaRDD<GenericRecord>> latFetch1 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch1.getCheckpointForNextBatch(), latFetch1.getCheckpointForNextBatch());
  }

  @Test
  public void testJsonKafkaSourceWithDefaultUpperCap() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceWithDefaultUpperCap";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, Long.MAX_VALUE, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    /*
    1. Extract without any checkpoint => get all the data, respecting default upper cap since both sourceLimit and
    maxEventsFromKafkaSourceProp are set to Long.MAX_VALUE
     */
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(1000, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on sourceLimit
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 1500);
    assertEquals(1000, fetch2.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceInsertRecordsLessSourceLimit() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceInsertRecordsLessSourceLimit";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, Long.MAX_VALUE, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents", "500");

    /*
     1. maxEventsFromKafkaSourceProp set to more than generated insert records
     and sourceLimit less than the generated insert records num.
     */
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 400)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 300);
    assertEquals(300, fetch1.getBatch().get().count());

    /*
     2. Produce new data, extract new data based on sourceLimit
     and sourceLimit less than the generated insert records num.
     */
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("001", 600)));
    InputBatch<Dataset<Row>> fetch2 =
            kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 300);
    assertEquals(300, fetch2.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceWithConfigurableUpperCap() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceWithConfigurableUpperCap";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, 500L, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on upper cap
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(500, fetch2.getBatch().get().count());

    //fetch data respecting source limit where upper cap > sourceLimit
    InputBatch<JavaRDD<GenericRecord>> fetch3 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), 400);
    assertEquals(400, fetch3.getBatch().get().count());

    //fetch data respecting source limit where upper cap < sourceLimit
    InputBatch<JavaRDD<GenericRecord>> fetch4 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch2.getCheckpointForNextBatch()), 600);
    assertEquals(600, fetch4.getBatch().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    InputBatch<JavaRDD<GenericRecord>> fetch5 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(fetch2.getBatch().get().count(), fetch5.getBatch().get().count());
    assertEquals(fetch2.getCheckpointForNextBatch(), fetch5.getCheckpointForNextBatch());

    // 4. Extract with latest checkpoint => no new data returned
    InputBatch<JavaRDD<GenericRecord>> fetch6 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch4.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch6.getBatch());
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

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, null, "earliest");
    props.put(ENABLE_KAFKA_COMMIT_OFFSET.key(), "true");
    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    testUtils.sendMessages(topic, jsonifyRecordsByPartitions(dataGenerator.generateInserts("000", 1000), topicPartitions.size()));

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
      Map endOffsets = consumer.endOffsets(topicPartitions);
      assertEquals(500L, endOffsets.get(topicPartition0));
      assertEquals(500L, endOffsets.get(topicPartition1));

      testUtils.sendMessages(topic, jsonifyRecordsByPartitions(dataGenerator.generateInserts("001", 500), topicPartitions.size()));
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
    topicConfig.setProperty("retention.ms", "10000");
    testUtils.createTopic(topic, 1, topicConfig);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties failOnDataLossProps = createPropsForJsonSource(topic, null, "earliest");
    failOnDataLossProps.setProperty(Config.ENABLE_FAIL_ON_DATA_LOSS.key(), Boolean.toString(true));

    Source jsonSource = new JsonKafkaSource(failOnDataLossProps, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 10)));
    // send 10 records, extract 2 records to generate a checkpoint
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 2);
    assertEquals(2, fetch1.getBatch().get().count());

    // wait for the checkpoint to expire
    Thread.sleep(10001);
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
