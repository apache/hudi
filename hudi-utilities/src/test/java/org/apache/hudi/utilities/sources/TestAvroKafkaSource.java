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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.streamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_KEY_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class TestAvroKafkaSource extends SparkClientFunctionalTestHarness {
  protected static final String TEST_TOPIC_PREFIX = "hoodie_avro_test_";

  protected static HoodieTestDataGenerator dataGen;

  protected static String SCHEMA_PATH = "/tmp/schema_file.avsc";

  protected final HoodieIngestionMetrics metrics = mock(HoodieIngestionMetrics.class);

  protected SchemaProvider schemaProvider;

  protected KafkaTestUtils testUtils;

  @BeforeAll
  public static void initClass() {
    dataGen = new HoodieTestDataGenerator(0xDEED);
  }

  @BeforeEach
  public void setup() {
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterEach
  public void tearDown() {
    testUtils.teardown();
  }

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
    return props;
  }

  void sendMessagesToKafka(String topic, int count, int numPartitions) {
    List<GenericRecord> genericRecords = dataGen.generateGenericRecords(count);
    Properties config = getProducerProperties();
    try (Producer<String, byte[]> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < genericRecords.size(); i++) {
        // use consistent keys to get even spread over partitions for test expectations
        producer.send(new ProducerRecord<>(topic, i % numPartitions, "key", HoodieAvroUtils.avroToBytes(genericRecords.get(i))));
      }
    }
  }

  void sendMessagesToKafkaWithNullKafkaKey(String topic, int count, int numPartitions) {
    List<GenericRecord> genericRecords = dataGen.generateGenericRecords(count);
    Properties config = getProducerProperties();
    try (Producer<String, byte[]> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < genericRecords.size(); i++) {
        // null kafka key
        producer.send(new ProducerRecord<>(topic, i % numPartitions, null, HoodieAvroUtils.avroToBytes(genericRecords.get(i))));
      }
    }
  }

  void sendMessagesToKafkaWithNullKafkaValue(String topic, int count, int numPartitions) {
    Properties config = getProducerProperties();
    try (Producer<String, byte[]> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < count; i++) {
        // null kafka value
        producer.send(new ProducerRecord<>(topic, i % numPartitions, "key", null));
      }
    }
  }

  private Properties getProducerProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", testUtils.brokerAddress());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("value.deserializer", ByteArraySerializer.class.getName());
    // Key serializer is required.
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("auto.register.schemas", "false");
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all");
    return props;
  }

  @Test
  public void testAppendKafkaOffsets() throws IOException {
    UtilitiesTestBase.Helpers.saveStringsToDFS(
        new String[] {dataGen.generateGenericRecord().getSchema().toString()}, hoodieStorage(),
        SCHEMA_PATH);
    ConsumerRecord<Object, Object> recordConsumerRecord =
        new ConsumerRecord<Object, Object>("test", 0, 1L,
            "test", dataGen.generateGenericRecord());
    JavaRDD<ConsumerRecord<Object, Object>> rdd =
        jsc().parallelize(Arrays.asList(recordConsumerRecord));
    TypedProperties props = new TypedProperties();
    props.put("hoodie.streamer.source.kafka.topic", "test");
    props.put("hoodie.streamer.schemaprovider.source.schema.file", SCHEMA_PATH);
    SchemaProvider schemaProvider = UtilHelpers.wrapSchemaProviderWithPostProcessor(
        UtilHelpers.createSchemaProvider(FilebasedSchemaProvider.class.getName(), props, jsc()),
        props, jsc(), new ArrayList<>());

    AvroKafkaSource avroKafkaSource =
        new AvroKafkaSource(props, jsc(), spark(), schemaProvider, null);
    GenericRecord withoutKafkaOffsets =
        avroKafkaSource.maybeAppendKafkaOffsets(rdd).collect().get(0);

    props.put(HoodieStreamerConfig.KAFKA_APPEND_OFFSETS.key(), "true");
    schemaProvider = UtilHelpers.wrapSchemaProviderWithPostProcessor(
        UtilHelpers.createSchemaProvider(FilebasedSchemaProvider.class.getName(), props, jsc()),
        props, jsc(), new ArrayList<>());
    avroKafkaSource = new AvroKafkaSource(props, jsc(), spark(), schemaProvider, null);
    GenericRecord withKafkaOffsets = avroKafkaSource.maybeAppendKafkaOffsets(rdd).collect().get(0);
    assertEquals(4, withKafkaOffsets.getSchema().getFields().size()
        - withoutKafkaOffsets.getSchema().getFields().size());
    assertEquals("test", withKafkaOffsets.get("_hoodie_kafka_source_key").toString());

    // scenario with null kafka key
    ConsumerRecord<Object, Object> recordConsumerRecordNullKafkaKey =
        new ConsumerRecord<Object, Object>("test", 0, 1L,
            null, dataGen.generateGenericRecord());
    JavaRDD<ConsumerRecord<Object, Object>> rddNullKafkaKey =
        jsc().parallelize(Arrays.asList(recordConsumerRecordNullKafkaKey));
    avroKafkaSource = new AvroKafkaSource(props, jsc(), spark(), schemaProvider, null);
    GenericRecord withKafkaOffsetsAndNullKafkaKey =
        avroKafkaSource.maybeAppendKafkaOffsets(rddNullKafkaKey).collect().get(0);
    assertNull(withKafkaOffsetsAndNullKafkaKey.get("_hoodie_kafka_source_key"));
  }

  @Test
  public void testAppendKafkaOffsetsSourceFormatAdapter() throws IOException {
    UtilitiesTestBase.Helpers.saveStringsToDFS(
        new String[] {dataGen.generateGenericRecord().getSchema().toString()}, hoodieStorage(),
        SCHEMA_PATH);
    final String topic = TEST_TOPIC_PREFIX + "testKafkaOffsetAppend";
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");

    props.put("hoodie.streamer.schemaprovider.source.schema.file", SCHEMA_PATH);
    SchemaProvider schemaProvider = UtilHelpers.wrapSchemaProviderWithPostProcessor(
        UtilHelpers.createSchemaProvider(FilebasedSchemaProvider.class.getName(), props, jsc()),
        props, jsc(), new ArrayList<>());

    props.put("hoodie.streamer.source.kafka.value.deserializer.class", ByteArrayDeserializer.class.getName());
    int numPartitions = 2;
    int numMessages = 30;
    testUtils.createTopic(topic,numPartitions);
    sendMessagesToKafka(topic, numMessages, numPartitions);
    // send some null value records
    sendMessagesToKafkaWithNullKafkaValue(topic, numMessages, numPartitions);

    AvroKafkaSource avroKafkaSource = new AvroKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(avroKafkaSource);
    Dataset<Row> c = kafkaSource.fetchNewDataInRowFormat(Option.empty(),Long.MAX_VALUE)
        .getBatch().get();
    List<String> columns = Arrays.stream(c.columns()).collect(Collectors.toList());
    props.put(HoodieStreamerConfig.KAFKA_APPEND_OFFSETS.key(), "true");

    schemaProvider = UtilHelpers.wrapSchemaProviderWithPostProcessor(
        UtilHelpers.createSchemaProvider(FilebasedSchemaProvider.class.getName(), props, jsc()), props, jsc(), new ArrayList<>());
    avroKafkaSource = new AvroKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    kafkaSource = new SourceFormatAdapter(avroKafkaSource);
    Dataset<Row>  d = kafkaSource.fetchNewDataInRowFormat(Option.empty(),Long.MAX_VALUE).getBatch().get();
    assertEquals(numMessages, d.count());
    for (int i = 0; i < numPartitions; i++) {
      assertEquals(numMessages / numPartitions, d.filter("_hoodie_kafka_source_partition=" + i).collectAsList().size());
    }
    List<String> withKafkaOffsetColumns = Arrays.stream(d.columns()).collect(Collectors.toList());
    assertEquals(0, d.drop(KAFKA_SOURCE_OFFSET_COLUMN, KAFKA_SOURCE_PARTITION_COLUMN, KAFKA_SOURCE_TIMESTAMP_COLUMN, KAFKA_SOURCE_KEY_COLUMN,"city_to_state").except(c.drop("city_to_state")).count());
    assertEquals(4, withKafkaOffsetColumns.size() - columns.size());
    List<String> appendList = Arrays.asList(KAFKA_SOURCE_OFFSET_COLUMN, KAFKA_SOURCE_PARTITION_COLUMN, KAFKA_SOURCE_TIMESTAMP_COLUMN, KAFKA_SOURCE_KEY_COLUMN);
    assertEquals(appendList, withKafkaOffsetColumns.subList(withKafkaOffsetColumns.size() - 4, withKafkaOffsetColumns.size()));

    // scenario with null kafka key
    sendMessagesToKafkaWithNullKafkaKey(topic, numMessages, numPartitions);
    AvroKafkaSource avroKafkaSourceWithNullKafkaKey = new AvroKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSourceWithNullKafkaKey = new SourceFormatAdapter(avroKafkaSourceWithNullKafkaKey);
    Dataset<Row> nullKafkaKeyDataset = kafkaSourceWithNullKafkaKey.fetchNewDataInRowFormat(Option.empty(),Long.MAX_VALUE)
            .getBatch().get();
    assertEquals(numMessages, nullKafkaKeyDataset.toDF().filter("_hoodie_kafka_source_key is null").count());
  }
}
