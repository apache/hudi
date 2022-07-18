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
import org.apache.hudi.utilities.schema.ProtoClassBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config;
import org.apache.hudi.utilities.test.proto.Nested;
import org.apache.hudi.utilities.test.proto.Sample;
import org.apache.hudi.utilities.test.proto.SampleEnum;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.Timestamps;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Tests against {@link ProtoKafkaSource}.
 */
public class TestProtoKafkaSource extends SparkClientFunctionalTestHarness {
  private static final Random RANDOM = new Random();
  private static final String TEST_TOPIC_PREFIX = "hoodie_test_";
  private static KafkaTestUtils testUtils;

  private final HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);

  @BeforeAll
  public static void initClass() {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterAll
  public static void cleanupClass() {
    testUtils.teardown();
  }

  protected TypedProperties createPropsForKafkaSource(String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", topic);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("auto.offset.reset", resetStrategy);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.defaultValue()));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.setProperty(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_CLASS_NAME, Sample.class.getName());
    return props;
  }

  @Test
  public void testProtoKafkaSource() {

    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testProtoKafkaSource";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    SchemaProvider schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(protoKafkaSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    sendMessages(topic, createSampleMessages(1000), 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    Dataset<Row> fetch1AsRows = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
        schemaProvider.getSourceSchema().toString(), protoKafkaSource.getSparkSession());
    assertEquals(900, fetch1AsRows.count());

    // 2. Produce new data, extract new data
    sendMessages(topic, createSampleMessages(1000), 2);
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

  @Test
  public void testProtoKafkaSourceWithFlattenWrappedPrimitives() {

    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testProtoKafkaSourceFlatten";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.setProperty(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_FLATTEN_WRAPPED_PRIMITIVES, "true");
    SchemaProvider schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(protoKafkaSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    sendMessages(topic, createSampleMessages(1000), 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    Dataset<Row> fetch1AsRows = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
        schemaProvider.getSourceSchema().toString(), protoKafkaSource.getSparkSession());
    assertEquals(900, fetch1AsRows.count());

    // 2. Produce new data, extract new data
    sendMessages(topic, createSampleMessages(1000), 2);
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
  public void testProtoKafkaSourceResetStrategy() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testProtoKafkaSourceResetStrategy";
    testUtils.createTopic(topic, 2);

    TypedProperties earliestProps = createPropsForKafkaSource(topic, null, "earliest");
    SchemaProvider schemaProvider = new ProtoClassBasedSchemaProvider(earliestProps, jsc());
    Source earliestprotoKafkaSource = new ProtoKafkaSource(earliestProps, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter earliestKafkaSource = new SourceFormatAdapter(earliestprotoKafkaSource);

    TypedProperties latestProps = createPropsForKafkaSource(topic, null, "latest");
    Source latestprotoKafkaSource = new ProtoKafkaSource(latestProps, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter latestKafkaSource = new SourceFormatAdapter(latestprotoKafkaSource);

    // 1. Extract with a none data kafka checkpoint
    // => get a checkpoint string like "hoodie_test,0:0,1:0", latest checkpoint should be equals to earliest checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch0 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    InputBatch<JavaRDD<GenericRecord>> latFetch0 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch0.getBatch(), latFetch0.getBatch());
    assertEquals(earFetch0.getCheckpointForNextBatch(), latFetch0.getCheckpointForNextBatch());

    sendMessages(topic, createSampleMessages(1000), 2);

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
    final String topic = TEST_TOPIC_PREFIX + "testProtoKafkaSourceInsertRecordsLessSourceLimit";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, Long.MAX_VALUE, "earliest");
    SchemaProvider schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(protoKafkaSource);
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents", "500");

    /*
     1. maxEventsFromKafkaSourceProp set to more than generated insert records
     and sourceLimit less than the generated insert records num.
     */
    sendMessages(topic, createSampleMessages(400), 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 300);
    assertEquals(300, fetch1.getBatch().get().count());

    /*
     2. Produce new data, extract new data based on sourceLimit
     and sourceLimit less than the generated insert records num.
     */
    sendMessages(topic, createSampleMessages(600), 2);
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
    SchemaProvider schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(protoKafkaSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    sendMessages(topic, createSampleMessages(1000), 2);

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

      sendMessages(topic, createSampleMessages(500), 2);
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

  private static List<Sample> createSampleMessages(int count) {
    return IntStream.range(0, count).mapToObj(unused -> {
      Sample.Builder builder = Sample.newBuilder()
          .setPrimitiveDouble(RANDOM.nextDouble())
          .setPrimitiveFloat(RANDOM.nextFloat())
          .setPrimitiveInt(RANDOM.nextInt())
          .setPrimitiveLong(RANDOM.nextLong())
          .setPrimitiveUnsignedInt(RANDOM.nextInt())
          .setPrimitiveUnsignedLong(RANDOM.nextLong())
          .setPrimitiveSignedInt(RANDOM.nextInt())
          .setPrimitiveSignedLong(RANDOM.nextLong())
          .setPrimitiveFixedInt(RANDOM.nextInt())
          .setPrimitiveFixedLong(RANDOM.nextLong())
          .setPrimitiveFixedSignedInt(RANDOM.nextInt())
          .setPrimitiveFixedSignedLong(RANDOM.nextLong())
          .setPrimitiveBoolean(RANDOM.nextBoolean())
          .setPrimitiveString(UUID.randomUUID().toString())
          .setPrimitiveBytes(ByteString.copyFrom(UUID.randomUUID().toString().getBytes()));

      // randomly set nested messages, lists, and maps to test edge cases
      if (RANDOM.nextBoolean()) {
        Map<String, Integer> primitiveMap = new HashMap<>();
        primitiveMap.put(UUID.randomUUID().toString(), RANDOM.nextInt());
        Map<String, Nested> messageMap = new HashMap<>();
        messageMap.put(UUID.randomUUID().toString(), generateRandomNestedMessage());
        builder.addAllRepeatedPrimitive(Arrays.asList(RANDOM.nextInt(), RANDOM.nextInt()))
            .putAllMapPrimitive(primitiveMap)
            .setNestedMessage(generateRandomNestedMessage())
            .addAllRepeatedMessage(Arrays.asList(generateRandomNestedMessage(), generateRandomNestedMessage()))
            .putAllMapMessage(messageMap)
            .setWrappedString(StringValue.of(UUID.randomUUID().toString()))
            .setWrappedInt(Int32Value.of(RANDOM.nextInt()))
            .setWrappedLong(Int64Value.of(RANDOM.nextLong()))
            .setWrappedUnsignedInt(UInt32Value.of(RANDOM.nextInt()))
            .setWrappedUnsignedLong(UInt64Value.of(RANDOM.nextLong()))
            .setWrappedDouble(DoubleValue.of(RANDOM.nextDouble()))
            .setWrappedFloat(FloatValue.of(RANDOM.nextFloat()))
            .setWrappedBoolean(BoolValue.of(RANDOM.nextBoolean()))
            .setWrappedBytes(BytesValue.of(ByteString.copyFrom(UUID.randomUUID().toString().getBytes())))
            .setEnum(SampleEnum.SECOND)
            .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));
      }
      return builder.build();
    }).collect(Collectors.toList());
  }

  private static Nested generateRandomNestedMessage() {
    return Nested.newBuilder()
        .setNestedInt(RANDOM.nextInt())
        .build();
  }

  private void sendMessages(String topic, List<Sample> messages, int numPartitions) {
    try (Producer<String, byte[]> producer = new KafkaProducer<>(getProducerProperties())) {
      for (int i = 0; i < messages.size(); i++) {
        // use consistent keys to get even spread over partitions for test expectations
        producer.send(new ProducerRecord<>(topic, Integer.toString(i % numPartitions), messages.get(i).toByteArray()));
      }
    }
  }

  private Properties getProducerProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", testUtils.brokerAddress());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    // Key serializer is required.
    props.put("key.serializer", StringSerializer.class.getName());
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all");
    return props;
  }
}
