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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.ErrorEvent;
import org.apache.hudi.utilities.streamer.SourceFormatAdapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TARGET_TABLE;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.ENABLE_KAFKA_COMMIT_OFFSET;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_KEY_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecords;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecordsByPartitions;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecordsByPartitionsWithNullKafkaKey;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests against {@link JsonKafkaSource}.
 */
public class TestJsonKafkaSource extends BaseTestKafkaSource {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static final URL SCHEMA_FILE_URL = TestJsonKafkaSource.class.getClassLoader().getResource("streamer-config/source_short_trip_uber.avsc");

  @BeforeEach
  public void init() throws Exception {
    String schemaFilePath = Objects.requireNonNull(SCHEMA_FILE_URL).toURI().getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    schemaProvider = new FilebasedSchemaProvider(props, jsc());
  }

  @Override
  protected TypedProperties createPropsForKafkaSource(String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    return createPropsForJsonKafkaSource(testUtils.brokerAddress(), topic, maxEventsToReadFromKafkaSource, resetStrategy);
  }

  static TypedProperties createPropsForJsonKafkaSource(String brokerAddress, String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.source.kafka.topic", topic);
    props.setProperty("bootstrap.servers", brokerAddress);
    props.setProperty("auto.offset.reset", resetStrategy);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty("hoodie.streamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(KafkaSourceConfig.MAX_EVENTS_FROM_KAFKA_SOURCE.defaultValue()));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  @Override
  protected SourceFormatAdapter createSource(TypedProperties props) {
    return new SourceFormatAdapter(new JsonKafkaSource(props, jsc(), spark(), metrics, new DefaultStreamContext(schemaProvider, sourceProfile)));
  }

  // test whether empty messages can be filtered
  @Test
  public void testJsonKafkaSourceFilterNullMsg() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceFilterNullMsg";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    // Send  1000 non-null messages to Kafka
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 1000, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));
    // Send  100 null messages to Kafka
    testUtils.sendMessages(topic, new String[100]);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    // Verify that messages with null values are filtered
    assertEquals(1000, fetch1.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceWithDefaultUpperCap() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceWithDefaultUpperCap";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForKafkaSource(topic, Long.MAX_VALUE, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    /*
    1. Extract without any checkpoint => get all the data, respecting default upper cap since both sourceLimit and
    maxEventsFromKafkaSourceProp are set to Long.MAX_VALUE
     */
    List<HoodieRecord> send1 = dataGenerator.generateInsertsAsPerSchema("000", 1000, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA);
    testUtils.sendMessages(topic, jsonifyRecords(send1));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(1000, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on sourceLimit
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("001", 1000, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 1500);
    assertEquals(1000, fetch2.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceWithConfigurableUpperCap() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceWithConfigurableUpperCap";
    testUtils.createTopic(topic, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForKafkaSource(topic, 500L, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 1000, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on upper cap
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("001", 1000, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));
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

  @Override
  protected void sendMessagesToKafka(String topic, int count, int numPartitions) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages(topic, jsonifyRecordsByPartitions(dataGenerator.generateInsertsAsPerSchema("000", count, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA), numPartitions));
  }

  void sendNullKafkaKeyMessagesToKafka(String topic, int count, int numPartitions) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages(topic, jsonifyRecordsByPartitionsWithNullKafkaKey(dataGenerator.generateInsertsAsPerSchema("000", count, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA), numPartitions));
  }

  void sendJsonSafeMessagesToKafka(String topic, int count, int numPartitions) {
    try {
      Tuple2<String, String>[] keyValues = new Tuple2[count];
      HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
      String[] records = jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", count, HoodieTestDataGenerator.SHORT_TRIP_SCHEMA));
      for (int i = 0; i < count; i++) {
        // Drop fields that don't translate to json properly
        Map node = OBJECT_MAPPER.readValue(records[i], Map.class);
        node.remove("height");
        node.remove("current_date");
        node.remove("nation");
        keyValues[i] = new Tuple2<>(Integer.toString(i % numPartitions), OBJECT_MAPPER.writeValueAsString(node));
      }
      testUtils.sendMessages(topic, keyValues);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testErrorEventsForDataInRowFormat() throws IOException {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testErrorEventsForDataInRowFormat";

    testUtils.createTopic(topic, 2);
    List<TopicPartition> topicPartitions = new ArrayList<>();
    TopicPartition topicPartition0 = new TopicPartition(topic, 0);
    topicPartitions.add(topicPartition0);
    TopicPartition topicPartition1 = new TopicPartition(topic, 1);
    topicPartitions.add(topicPartition1);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    sendJsonSafeMessagesToKafka(topic, 1000, 2);
    testUtils.sendMessages(topic, new String[]{"error_event1", "error_event2"});

    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.put(ENABLE_KAFKA_COMMIT_OFFSET.key(), "true");
    props.put(ERROR_TABLE_BASE_PATH.key(),"/tmp/qurantine_table_test/json_kafka_row_events");
    props.put(ERROR_TARGET_TABLE.key(),"json_kafka_row_events");
    props.put("hoodie.errortable.validate.targetschema.enable", "true");
    props.put("hoodie.base.path","/tmp/json_kafka_row_events");
    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    Option<BaseErrorTableWriter> errorTableWriter = Option.of(getAnonymousErrorTableWriter(props));
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource, errorTableWriter, Option.of(props));
    assertEquals(1000, kafkaSource.fetchNewDataInRowFormat(Option.empty(),Long.MAX_VALUE).getBatch().get().count());
    assertEquals(2,((JavaRDD)errorTableWriter.get().getErrorEvents(
        HoodieActiveTimeline.createNewInstantTime(), Option.empty()).get()).count());
  }

  @Test
  public void testErrorEventsForDataInAvroFormat() throws IOException {

    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testErrorEventsForDataInAvroFormat";

    testUtils.createTopic(topic, 2);
    List<TopicPartition> topicPartitions = new ArrayList<>();
    TopicPartition topicPartition0 = new TopicPartition(topic, 0);
    topicPartitions.add(topicPartition0);
    TopicPartition topicPartition1 = new TopicPartition(topic, 1);
    topicPartitions.add(topicPartition1);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", 1000,
        HoodieTestDataGenerator.SHORT_TRIP_SCHEMA)));
    testUtils.sendMessages(topic, new String[]{"error_event1", "error_event2"});

    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.put(ENABLE_KAFKA_COMMIT_OFFSET.key(), "true");
    props.put(ERROR_TABLE_BASE_PATH.key(),"/tmp/qurantine_table_test/json_kafka_events");
    props.put(ERROR_TARGET_TABLE.key(),"json_kafka_events");
    props.put("hoodie.base.path","/tmp/json_kafka_events");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    Option<BaseErrorTableWriter> errorTableWriter = Option.of(getAnonymousErrorTableWriter(props));
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource, errorTableWriter, Option.of(props));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(),Long.MAX_VALUE);
    assertEquals(1000,fetch1.getBatch().get().count());
    assertEquals(2, ((JavaRDD)errorTableWriter.get().getErrorEvents(
        HoodieActiveTimeline.createNewInstantTime(), Option.empty()).get()).count());
  }

  private BaseErrorTableWriter getAnonymousErrorTableWriter(TypedProperties props) {
    return new BaseErrorTableWriter<ErrorEvent<String>>(new HoodieDeltaStreamer.Config(),
        spark(), props, new HoodieSparkEngineContext(jsc()), fs()) {
      List<JavaRDD<HoodieAvroRecord>> errorEvents = new LinkedList();

      @Override
      public void addErrorEvents(JavaRDD errorEvent) {
        errorEvents.add(errorEvent.map(r -> new HoodieAvroRecord<>(new HoodieKey(), null)));
      }

      @Override
      public Option<JavaRDD<HoodieAvroRecord>> getErrorEvents(String baseTableInstantTime,
                                                              Option commitedInstantTime) {
        return Option.of(errorEvents.stream().reduce((rdd1, rdd2) -> rdd1.union(rdd2)).get());
      }

      @Override
      public boolean upsertAndCommit(String baseTableInstantTime, Option commitedInstantTime) {
        return false;
      }
    };
  }

  @Test
  public void testAppendKafkaOffset() {
    final String topic = TEST_TOPIC_PREFIX + "testKafkaOffsetAppend";
    int numPartitions = 2;
    int numMessages = 30;
    testUtils.createTopic(topic, numPartitions);
    sendMessagesToKafka(topic, numMessages, numPartitions);

    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);
    Dataset<Row> dfNoOffsetInfo = kafkaSource.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE).getBatch().get().cache();
    assertEquals(numMessages, dfNoOffsetInfo.count());
    List<String> columns = Arrays.stream(dfNoOffsetInfo.columns()).collect(Collectors.toList());

    props.put(HoodieStreamerConfig.KAFKA_APPEND_OFFSETS.key(), "true");
    jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    kafkaSource = new SourceFormatAdapter(jsonSource);
    Dataset<Row> dfWithOffsetInfo = kafkaSource.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE).getBatch().get().cache();
    assertEquals(numMessages, dfWithOffsetInfo.count());
    for (int i = 0; i < numPartitions; i++) {
      assertEquals(numMessages / numPartitions, dfWithOffsetInfo.filter("_hoodie_kafka_source_partition=" + i).count());
    }
    assertEquals(0, dfWithOffsetInfo
        .drop(KAFKA_SOURCE_OFFSET_COLUMN, KAFKA_SOURCE_PARTITION_COLUMN, KAFKA_SOURCE_TIMESTAMP_COLUMN, KAFKA_SOURCE_KEY_COLUMN)
        .except(dfNoOffsetInfo).count());
    List<String> withKafkaOffsetColumns = Arrays.stream(dfWithOffsetInfo.columns()).collect(Collectors.toList());
    assertEquals(4, withKafkaOffsetColumns.size() - columns.size());
    List<String> appendList = Arrays.asList(KAFKA_SOURCE_OFFSET_COLUMN, KAFKA_SOURCE_PARTITION_COLUMN, KAFKA_SOURCE_TIMESTAMP_COLUMN, KAFKA_SOURCE_KEY_COLUMN);
    assertEquals(appendList, withKafkaOffsetColumns.subList(withKafkaOffsetColumns.size() - 4, withKafkaOffsetColumns.size()));

    // scenario with null kafka key
    sendNullKafkaKeyMessagesToKafka(topic, numMessages, numPartitions);
    jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    kafkaSource = new SourceFormatAdapter(jsonSource);
    Dataset<Row> dfWithOffsetInfoAndNullKafkaKey = kafkaSource.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE).getBatch().get().cache();
    // total of 2 * numMessages are in the topic at this point, half with a key and half with a null key. All should have the source offset.
    assertEquals(numMessages, dfWithOffsetInfoAndNullKafkaKey.toDF().filter("_hoodie_kafka_source_key is null").count());
    assertEquals(numMessages, dfWithOffsetInfoAndNullKafkaKey.toDF().filter("_hoodie_kafka_source_key is not null").count());
    assertEquals(numMessages * 2, dfWithOffsetInfoAndNullKafkaKey.toDF().filter("_hoodie_kafka_source_offset is not null").count());

    dfNoOffsetInfo.unpersist();
    dfWithOffsetInfo.unpersist();
    dfWithOffsetInfoAndNullKafkaKey.unpersist();
  }

  @Test
  public void testCreateSource() throws IOException {
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceCreation";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    Source jsonKafkaSource = UtilHelpers.createSource(JsonKafkaSource.class.getName(), props, jsc(), spark(), metrics, new DefaultStreamContext(schemaProvider, sourceProfile));
    assertEquals(Source.SourceType.JSON, jsonKafkaSource.getSourceType());
  }
}
