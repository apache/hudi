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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaAvroTestDeserializer;
import org.apache.hudi.utilities.sources.helpers.KafkaAvroTestSerializer;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config;
import org.apache.hudi.utilities.sources.helpers.SchemaTestProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests {@link AvroKakfaSource}.
 */
public class TestAvroKafkaSource extends UtilitiesTestBase {

  private static String TEST_TOPIC_NAME = "hoodie_test";

  private FilebasedSchemaProvider schemaProvider;
  private KafkaTestUtils testUtils;
  private Producer<String, GenericRecord> kafkaProducer;
  private HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);
  @Mock
  private SchemaTestProvider schemaTestProvider;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    testUtils = new KafkaTestUtils();
    testUtils.setup();
    kafkaProducer = createProducer(testUtils.brokerAddress(), "dummy");
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    testUtils.teardown();
  }

  private TypedProperties createPropsForAvroKafkaSource(Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", TEST_TOPIC_NAME);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("hoodie.deltastreamer.source.kafka.auto.reset.offsets", resetStrategy);
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(Config.maxEventsFromKafkaSource));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  public static Producer<String, GenericRecord> createProducer(String brokers, String schemaRegistryUrl) {
    TypedProperties props = new TypedProperties();
    props.put("bootstrap.servers", brokers);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", KafkaAvroTestSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    return new KafkaProducer<String, GenericRecord>(props);
  }

  @Test
  public void testAvroKafkaSource() {
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForAvroKafkaSource(null, "earliest");
    props.put("value.deserializer", KafkaAvroTestDeserializer.class);
    props.put("hoodie.deltastreamer.source.kafka.value.deserializer.class", KafkaAvroTestDeserializer.class.getCanonicalName());
    props.put("hoodie.deltastreamer.schemaprovider.class", SchemaTestProvider.class);
    when(schemaTestProvider.getSourceSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);

    Source avroKafkaSource = new AvroKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(avroKafkaSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    List<GenericRecord> genericRecordList = Helpers.toGenericRecords(dataGenerator.generateInserts("000", 10));
    for (GenericRecord genericRecord : genericRecordList) {
      kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC_NAME, genericRecord));
    }
    kafkaProducer.flush();
    kafkaProducer.close();

    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 10);
    List<GenericRecord> actualGenRecs = fetch1.getBatch().get().collect();
    assertEquals(10, actualGenRecs.size());
    // to be fixed : equality checks for generic Records.
    // assertEquals(genericRecordList, actualGenRecs);
    assertListEquality(genericRecordList, actualGenRecs);

    // evolve schema and add more records to kafka
    KafkaAvroTestSerializer.schemaToReturn = HoodieTestDataGenerator.AVRO_EVOLVED_SCHEMA;
    KafkaAvroTestDeserializer.schemaToReturn = HoodieTestDataGenerator.AVRO_EVOLVED_SCHEMA;
    when(schemaTestProvider.getSourceSchema()).thenReturn(HoodieTestDataGenerator.AVRO_EVOLVED_SCHEMA);
    kafkaProducer = createProducer(testUtils.brokerAddress(), "dummy");

    genericRecordList = Helpers.toGenericRecords(dataGenerator.generateInsertsStream("001", 20, false,
        HoodieTestDataGenerator.TRIP_EVOLVED_EXAMPLE_SCHEMA).collect(Collectors.toList()), HoodieTestDataGenerator.AVRO_EVOLVED_SCHEMA);
    for (GenericRecord genericRecord : genericRecordList) {
      kafkaProducer.send(new ProducerRecord<>(TEST_TOPIC_NAME, genericRecord));
    }
    kafkaProducer.flush();

    InputBatch<JavaRDD<GenericRecord>> fetch2 = kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), 20);
    List<GenericRecord> actualGenRecs2 = fetch2.getBatch().get().collect();
    assertEquals(20, actualGenRecs2.size());
    assertEquals(genericRecordList, actualGenRecs2);
  }

  private void assertListEquality(List<GenericRecord> list1, List<GenericRecord> list2) {
    System.out.println("Expected :: ");
    for (GenericRecord gRec : list1) {
      System.out.println("exp rec:: " + gRec.toString());
    }
    System.out.println("Actual :: ");
    for (GenericRecord gRec : list2) {
      System.out.println("act rec:: " + gRec.toString());
    }
    assertEquals(list1.size(), list2.size());
    for (GenericRecord genericRecord : list1) {
      System.out.println("Checking " + genericRecord.toString() + " from exp ");
      assertTrue(list2.contains(genericRecord));
    }
    for (GenericRecord genericRecord : list2) {
      System.out.println("Checking " + genericRecord.toString() + " from actual ");
      assertTrue(list1.contains(genericRecord));
    }
  }

}
