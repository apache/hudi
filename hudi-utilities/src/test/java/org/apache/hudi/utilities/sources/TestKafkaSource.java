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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests against {@link AvroKafkaSource}.
 */
public class TestKafkaSource extends UtilitiesTestBase {

  private static String TEST_TOPIC_NAME = "hoodie_test";

  private FilebasedSchemaProvider schemaProvider;
  private KafkaTestUtils testUtils;
  private HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);

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
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    testUtils.teardown();
  }

  private TypedProperties createPropsForJsonSource(Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", TEST_TOPIC_NAME);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty(Config.KAFKA_AUTO_OFFSET_RESET, resetStrategy);
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(Config.maxEventsFromKafkaSource));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  @Test
  public void testJsonKafkaSource() {

    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(null, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    Dataset<Row> fetch1AsRows = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
        schemaProvider.getSourceSchema().toString(), jsonSource.getSparkSession());
    assertEquals(900, fetch1AsRows.count());

    // 2. Produce new data, extract new data
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
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
  public void testJsonKafkaSourceResetStrategy() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();

    TypedProperties earliestProps = createPropsForJsonSource(null, "earliest");
    Source earliestJsonSource = new JsonKafkaSource(earliestProps, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter earliestKafkaSource = new SourceFormatAdapter(earliestJsonSource);

    TypedProperties latestProps = createPropsForJsonSource(null, "latest");
    Source latestJsonSource = new JsonKafkaSource(latestProps, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter latestKafkaSource = new SourceFormatAdapter(latestJsonSource);

    // 1. Extract with a none data kafka checkpoint
    // => get a checkpoint string like "hoodie_test,0:0,1:0", latest checkpoint should be equals to earliest checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch0 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    InputBatch<JavaRDD<GenericRecord>> latFetch0 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch0.getBatch(), latFetch0.getBatch());
    assertEquals(earFetch0.getCheckpointForNextBatch(), latFetch0.getCheckpointForNextBatch());

    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

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
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(Long.MAX_VALUE, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);
    Config.maxEventsFromKafkaSource = 500;

    /*
    1. Extract without any checkpoint => get all the data, respecting default upper cap since both sourceLimit and
    maxEventsFromKafkaSourceProp are set to Long.MAX_VALUE
     */
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(1000, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on sourceLimit
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 1500);
    assertEquals(1000, fetch2.getBatch().get().count());

    //reset the value back since it is a static variable
    Config.maxEventsFromKafkaSource = Config.DEFAULT_MAX_EVENTS_FROM_KAFKA_SOURCE;
  }

  @Test
  public void testJsonKafkaSourceInsertRecordsLessSourceLimit() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(Long.MAX_VALUE, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);
    Config.maxEventsFromKafkaSource = 500;

    /*
     1. maxEventsFromKafkaSourceProp set to more than generated insert records
     and sourceLimit less than the generated insert records num.
     */
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 400)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 300);
    assertEquals(300, fetch1.getBatch().get().count());

    /*
     2. Produce new data, extract new data based on sourceLimit
     and sourceLimit less than the generated insert records num.
     */
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 600)));
    InputBatch<Dataset<Row>> fetch2 =
            kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 300);
    assertEquals(300, fetch2.getBatch().get().count());

    //reset the value back since it is a static variable
    Config.maxEventsFromKafkaSource = Config.DEFAULT_MAX_EVENTS_FROM_KAFKA_SOURCE;
  }

  @Test
  public void testJsonKafkaSourceWithConfigurableUpperCap() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(500L, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on upper cap
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
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
}
