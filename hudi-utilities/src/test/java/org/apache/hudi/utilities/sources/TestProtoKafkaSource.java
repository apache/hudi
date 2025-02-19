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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.config.ProtoClassBasedSchemaProviderConfig;
import org.apache.hudi.utilities.schema.ProtoClassBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.SourceFormatAdapter;
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
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_PROTO_VALUE_DESERIALIZER_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests against {@link ProtoKafkaSource}.
 */
public class TestProtoKafkaSource extends BaseTestKafkaSource {
  private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();
  private static final Random RANDOM = new Random();
  private static final String MOCK_REGISTRY_URL = "mock://127.0.0.1:8081";

  @Override
  protected TypedProperties createPropsForKafkaSource(String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.source.kafka.topic", topic);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("auto.offset.reset", resetStrategy);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty("hoodie.streamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(KafkaSourceConfig.MAX_EVENTS_FROM_KAFKA_SOURCE.defaultValue()));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key(), Sample.class.getName());
    return props;
  }

  @Override
  protected SourceFormatAdapter createSource(TypedProperties props) {
    this.schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), metrics, new DefaultStreamContext(schemaProvider, sourceProfile));
    return new SourceFormatAdapter(protoKafkaSource);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testProtoKafkaSourceWithConfluentProtoDeserialization(boolean persistSourceRdd) {
    final String topic = TEST_TOPIC_PREFIX + "testProtoKafkaSourceWithConfluentDeserializer_" + persistSourceRdd;
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.put(KAFKA_PROTO_VALUE_DESERIALIZER_CLASS.key(),
        "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
    props.put("schema.registry.url", MOCK_REGISTRY_URL);
    props.put("hoodie.streamer.schemaprovider.registry.url", MOCK_REGISTRY_URL);
    props.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.key(), "true");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD.key(), String.valueOf(persistSourceRdd));
    // class name is not required so we'll remove it
    props.remove(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key());
    SchemaProvider schemaProvider = new SchemaRegistryProvider(props, jsc());
    ProtoKafkaSource protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    List<Sample> messages = createSampleMessages(1000);
    sendMessagesToKafkaWithConfluentSerializer(topic, 2, messages);
    // Assert messages are read correctly
    JavaRDD<Message> messagesRead = protoKafkaSource.fetchNext(Option.empty(), 1000).getBatch().get();
    assertEquals(messages.stream().map(this::protoToJson).collect(Collectors.toSet()),
        new HashSet<>(messagesRead.map(message -> PRINTER.print(message)).collect()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testProtoKafkaSourceWithFlattenWrappedPrimitives(boolean persistSourceRdd) {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "test_proto_kafka_source_flatten_persist_source_rdd_" + persistSourceRdd;
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.key(), "true");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD.key(), Boolean.toString(persistSourceRdd));
    SchemaProvider schemaProvider = new ProtoClassBasedSchemaProvider(props, jsc());
    Source protoKafkaSource = new ProtoKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(protoKafkaSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    Dataset<Row> fetch1AsRows = kafkaSource.fetchNewDataInRowFormat(Option.empty(), 900).getBatch().get();
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
          .setPrimitiveBytes(ByteString.copyFrom(getUTF8Bytes(UUID.randomUUID().toString())));

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
            .setWrappedBytes(BytesValue.of(ByteString.copyFrom(getUTF8Bytes(UUID.randomUUID().toString()))))
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
  protected void sendMessagesToKafka(String topic, int count, int numPartitions) {
    List<Sample> messages = createSampleMessages(count);
    try (Producer<String, byte[]> producer = new KafkaProducer<>(getProducerProperties(false))) {
      for (int i = 0; i < messages.size(); i++) {
        // use consistent keys to get even spread over partitions for test expectations
        producer.send(new ProducerRecord<>(topic, Integer.toString(i % numPartitions), messages.get(i).toByteArray()));
      }
    }
  }

  private void sendMessagesToKafkaWithConfluentSerializer(String topic, int numPartitions, List<Sample> messages) {
    try (Producer<String, Message> producer = new KafkaProducer<>(getProducerProperties(true))) {
      for (int i = 0; i < messages.size(); i++) {
        // use consistent keys to get even spread over partitions for test expectations
        producer.send(new ProducerRecord<>(topic, Integer.toString(i % numPartitions), messages.get(i)));
      }
    }
  }

  private Properties getProducerProperties(boolean useConfluentProtobufSerializer) {
    Properties props = new Properties();
    props.put("bootstrap.servers", testUtils.brokerAddress());
    if (useConfluentProtobufSerializer) {
      props.put("value.serializer", KafkaProtobufSerializer.class.getName());
      props.put("value.deserializer", KafkaProtobufDeserializer.class.getName());
      props.put("schema.registry.url", MOCK_REGISTRY_URL);
      props.put("auto.register.schemas", "true");
    } else {
      props.put("value.serializer", ByteArraySerializer.class.getName());
      // Key serializer is required.
    }
    props.put("key.serializer", StringSerializer.class.getName());
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all");
    return props;
  }

  private String protoToJson(Message input) {
    try {
      return PRINTER.print(input);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert proto to json", e);
    }
  }
}
