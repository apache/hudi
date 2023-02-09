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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests against {@link ProtoKafkaSource}.
 */
public class TestProtoKafkaSource extends BaseTestKafkaSource {
  private static final Random RANDOM = new Random();

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
    props.setProperty(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_CLASS_NAME.key(), Sample.class.getName());
    return props;
  }

  @Override
  SourceFormatAdapter createSource(TypedProperties props) {
    this.schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    return new SourceFormatAdapter(protoKafkaSource);
  }

  @Test
  public void testProtoKafkaSourceWithFlattenWrappedPrimitives() {

    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testProtoKafkaSourceFlatten";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.setProperty(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.key(), "true");
    SchemaProvider schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(protoKafkaSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    Dataset<Row> fetch1AsRows = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
        schemaProvider.getSourceSchema().toString(), protoKafkaSource.getSparkSession());
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

  @Override
  void sendMessagesToKafka(String topic, int count, int numPartitions) {
    List<Sample> messages = createSampleMessages(count);
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
