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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;
import org.apache.hudi.utilities.sources.processor.maxwell.MaxwellJsonKafkaSourcePostProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config.JSON_KAFKA_PROCESSOR_CLASS_OPT;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJsonKafkaSourcePostProcessor extends TestJsonKafkaSource {

  @Test
  public void testNoPostProcessor() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testNoPostProcessor";
    testUtils.createTopic(topic, 2);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, null, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);

    assertEquals(900, fetch1.getBatch().get().count());
  }

  @Test
  public void testSampleJsonKafkaSourcePostProcessor() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testSampleJsonKafkaSourcePostProcessor";
    testUtils.createTopic(topic, 2);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, null, "earliest");

    // processor class name setup
    props.setProperty(JSON_KAFKA_PROCESSOR_CLASS_OPT.key(), SampleJsonKafkaSourcePostProcessor.class.getName());

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);

    assertNotEquals(900, fetch1.getBatch().get().count());
  }

  @Test
  public void testInvalidJsonKafkaSourcePostProcessor() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testInvalidJsonKafkaSourcePostProcessor";
    testUtils.createTopic(topic, 2);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, null, "earliest");

    // processor class name setup
    props.setProperty(JSON_KAFKA_PROCESSOR_CLASS_OPT.key(), "InvalidJsonKafkaSourcePostProcessor");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);
    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    Assertions.assertThrows(HoodieSourcePostProcessException.class,
        () -> kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900));
  }

  @Test
  public void testChainedJsonKafkaSourcePostProcessor() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testChainedJsonKafkaSourcePostProcessor";
    testUtils.createTopic(topic, 2);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(topic, null, "earliest");

    // processor class name setup
    props.setProperty(JSON_KAFKA_PROCESSOR_CLASS_OPT.key(), SampleJsonKafkaSourcePostProcessor.class.getName()
        + "," + DummyJsonKafkaSourcePostProcessor.class.getName());

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    testUtils.sendMessages(topic, jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);

    assertEquals(0, fetch1.getBatch().get().count());
  }

  @Test
  public void testMaxwellJsonKafkaSourcePostProcessor() throws IOException {
    // ------------------------------------------------------------------------
    //  Maxwell data
    // ------------------------------------------------------------------------

    // database hudi, table hudi_maxwell_01 (insert, update and delete)
    String hudiMaxwell01Insert = "{\"database\":\"hudi\",\"table\":\"hudi_maxwell_01\",\"type\":\"insert\","
        + "\"ts\":1647074402,\"xid\":6233,\"commit\":true,\"data\":{\"id\":\"6018220e39e74477b45c7cf42f66bdc0\","
        + "\"name\":\"mathieu\",\"age\":18,\"insert_time\":\"2022-03-12 08:40:02\","
        + "\"update_time\":\"2022-03-12 08:40:02\"}}";

    String hudiMaxwell01Update = "{\"database\":\"hudi\",\"table\":\"hudi_maxwell_01\",\"type\":\"update\","
        + "\"ts\":1647074482,\"xid\":6440,\"commit\":true,\"data\":{\"id\":\"6018220e39e74477b45c7cf42f66bdc0\","
        + "\"name\":\"mathieu\",\"age\":20,\"insert_time\":\"2022-03-12 04:40:02\",\"update_time\":\"2022-03-12 04:42:25\"},"
        + "\"old\":{\"age\":18,\"insert_time\":\"2022-03-12 08:40:02\",\"update_time\":\"2022-03-12 08:40:02\"}}";

    String hudiMaxwell01Delete = "{\"database\":\"hudi\",\"table\":\"hudi_maxwell_01\",\"type\":\"delete\","
        + "\"ts\":1647074555,\"xid\":6631,\"commit\":true,\"data\":{\"id\":\"6018220e39e74477b45c7cf42f66bdc0\","
        + "\"name\":\"mathieu\",\"age\":20,\"insert_time\":\"2022-03-12 04:40:02\",\"update_time\":\"2022-03-12 04:42:25\"}}";

    String hudiMaxwell01Ddl = "{\"type\":\"table-alter\",\"database\":\"hudi\",\"table\":\"hudi_maxwell_01\","
        + "\"old\":{\"database\":\"hudi\",\"charset\":\"utf8\",\"table\":\"hudi_maxwell_01\","
        + "\"primary-key\":[\"id\"],\"columns\":[{\"type\":\"varchar\",\"name\":\"id\",\"charset\":\"utf8\"},"
        + "{\"type\":\"varchar\",\"name\":\"name\",\"charset\":\"utf8\"},{\"type\":\"int\",\"name\":\"age\","
        + "\"signed\":true},{\"type\":\"timestamp\",\"name\":\"insert_time\",\"column-length\":0},"
        + "{\"type\":\"timestamp\",\"name\":\"update_time\",\"column-length\":0}]},\"def\":{\"database\":\"hudi\","
        + "\"charset\":\"utf8\",\"table\":\"hudi_maxwell_01\",\"primary-key\":[\"id\"],"
        + "\"columns\":[{\"type\":\"varchar\",\"name\":\"id\",\"charset\":\"utf8\"},{\"type\":\"varchar\","
        + "\"name\":\"name\",\"charset\":\"utf8\"},{\"type\":\"int\",\"name\":\"age\",\"signed\":true},"
        + "{\"type\":\"timestamp\",\"name\":\"insert_time\",\"column-length\":0},{\"type\":\"timestamp\","
        + "\"name\":\"update_time\",\"column-length\":0}]},\"ts\":1647072305000,\"sql\":\"/* ApplicationName=DBeaver "
        + "21.0.4 - Main */ ALTER TABLE hudi.hudi_maxwell_01 MODIFY COLUMN age int(3) NULL\"}";

    // database hudi, table hudi_maxwell_010, insert
    String hudiMaxwell010Insert = "{\"database\":\"hudi\",\"table\":\"hudi_maxwell_010\",\"type\":\"insert\","
        + "\"ts\":1647073982,\"xid\":5164,\"commit\":true,\"data\":{\"id\":\"f3eaf4cdf7534e47a88cdf93d19b2ee6\","
        + "\"name\":\"wangxianghu\",\"age\":18,\"insert_time\":\"2022-03-12 08:33:02\","
        + "\"update_time\":\"2022-03-12 08:33:02\"}}";

    // database hudi_02, table hudi_maxwell_02, insert
    String hudi02Maxwell02Insert = "{\"database\":\"hudi_02\",\"table\":\"hudi_maxwell_02\",\"type\":\"insert\","
        + "\"ts\":1647073916,\"xid\":4990,\"commit\":true,\"data\":{\"id\":\"9bb17f316ee8488cb107621ddf0f3cb0\","
        + "\"name\":\"andy\",\"age\":17,\"insert_time\":\"2022-03-12 08:31:56\","
        + "\"update_time\":\"2022-03-12 08:31:56\"}}";

    // database hudi_02, table hudi_maxwell_01, insert
    String hudi02Maxwell01Insert = "{\"database\":\"hudi_02\",\"table\":\"hudi_maxwell_01\",\"type\":\"insert\","
        + "\"ts\":1647073916,\"xid\":4990,\"commit\":true,\"data\":{\"id\":\"9bb17f316ee8488cb107621ddf0f3cb0\","
        + "\"name\":\"andy\",\"age\":17,\"insert_time\":\"2022-03-12 08:31:56\","
        + "\"update_time\":\"2022-03-12 08:31:56\"}}";

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    ObjectMapper mapper = new ObjectMapper();
    TypedProperties props = new TypedProperties();
    props.setProperty(MaxwellJsonKafkaSourcePostProcessor.Config.DATABASE_NAME_REGEX_PROP.key(), "hudi(_)?[0-9]{0,2}");
    props.setProperty(MaxwellJsonKafkaSourcePostProcessor.Config.TABLE_NAME_REGEX_PROP.key(), "hudi_maxwell(_)?[0-9]{0,2}");

    // test insert and update
    JavaRDD<String> inputInsertAndUpdate = jsc().parallelize(Arrays.asList(hudiMaxwell01Insert, hudiMaxwell01Update));
    MaxwellJsonKafkaSourcePostProcessor processor = new MaxwellJsonKafkaSourcePostProcessor(props);
    processor.process(inputInsertAndUpdate).map(mapper::readTree).foreach(record -> {
      // database name should be null
      JsonNode database = record.get("database");
      // insert and update records should be tagged as no delete
      boolean isDelete = record.get(HoodieRecord.HOODIE_IS_DELETED_FIELD).booleanValue();

      assertFalse(isDelete);
      assertNull(database);
    });

    // test delete
    props.setProperty(MaxwellJsonKafkaSourcePostProcessor.Config.PRECOMBINE_FIELD_TYPE_PROP.key(), "DATE_STRING");
    props.setProperty(MaxwellJsonKafkaSourcePostProcessor.Config.PRECOMBINE_FIELD_FORMAT_PROP.key(), "yyyy-MM-dd HH:mm:ss");
    props.setProperty(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "update_time");

    JavaRDD<String> inputDelete = jsc().parallelize(Collections.singletonList(hudiMaxwell01Delete));

    long ts = mapper.readTree(hudiMaxwell01Delete).get("ts").longValue();
    String formatTs = DateTimeUtils.formatUnixTimestamp(ts, "yyyy-MM-dd HH:mm:ss");

    new MaxwellJsonKafkaSourcePostProcessor(props)
        .process(inputDelete).map(mapper::readTree).foreach(record -> {

          // delete records should be tagged as delete
          boolean isDelete = record.get(HoodieRecord.HOODIE_IS_DELETED_FIELD).booleanValue();
          // update_time should equals ts
          String updateTime = record.get("update_time").textValue();

          assertEquals(formatTs, updateTime);
          assertTrue(isDelete);
        });

    // test preCombine field is not time
    props.setProperty(MaxwellJsonKafkaSourcePostProcessor.Config.PRECOMBINE_FIELD_TYPE_PROP.key(), "NON_TIMESTAMP");
    props.setProperty(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "id");

    JavaRDD<String> inputDelete2 = jsc().parallelize(Collections.singletonList(hudiMaxwell01Delete));

    String updateTimeInUpdate = mapper.readTree(hudiMaxwell01Update).get("data").get("update_time").textValue();
    new MaxwellJsonKafkaSourcePostProcessor(props)
        .process(inputDelete2).map(mapper::readTree).foreach(record -> {

          // updateTimeInUpdate should updateTimeInDelete
          String updateTimeInDelete = record.get("update_time").textValue();
          assertEquals(updateTimeInUpdate, updateTimeInDelete);
        });

    // test database, table regex
    JavaRDD<String> dirtyData = jsc().parallelize(Arrays.asList(hudiMaxwell01Insert, hudiMaxwell010Insert, hudi02Maxwell02Insert));
    long validDataNum = processor.process(dirtyData).count();
    // hudiMaxwell010Insert is dirty data
    assertEquals(2, validDataNum);

    // test ddl
    JavaRDD<String> ddlData = jsc().parallelize(Collections.singletonList(hudiMaxwell01Ddl));
    // ddl data will be ignored, ths count should be 0
    long ddlDataNum = processor.process(ddlData).count();
    assertEquals(0, ddlDataNum);

    // test table regex without database regex
    props.remove(MaxwellJsonKafkaSourcePostProcessor.Config.DATABASE_NAME_REGEX_PROP.key());
    props.setProperty(MaxwellJsonKafkaSourcePostProcessor.Config.TABLE_NAME_REGEX_PROP.key(), "hudi_maxwell(_)?[0-9]{0,2}");

    JavaRDD<String> dataWithoutDatabaseRegex = jsc().parallelize(Arrays.asList(hudiMaxwell01Insert, hudi02Maxwell01Insert));
    long countWithoutDatabaseRegex = processor.process(dataWithoutDatabaseRegex).count();
    assertEquals(2, countWithoutDatabaseRegex);
  }

  /**
   * JsonKafkaSourcePostProcessor that return a sub RDD of the incoming data which get the data from incoming data using
   * {org.apache.spark.api.java.JavaRDD#sample(boolean, double, long)} method.
   */
  public static class SampleJsonKafkaSourcePostProcessor extends JsonKafkaSourcePostProcessor {

    public SampleJsonKafkaSourcePostProcessor(TypedProperties props) {
      super(props);
    }

    @Override
    public JavaRDD<String> process(JavaRDD<String> inputJsonRecords) {
      return inputJsonRecords.sample(false, 0.5);
    }
  }

  public static class DummyJsonKafkaSourcePostProcessor extends JsonKafkaSourcePostProcessor {
    public DummyJsonKafkaSourcePostProcessor(TypedProperties props) {
      super(props);
    }

    @Override
    public JavaRDD<String> process(JavaRDD<String> inputJsonRecords) {
      // return empty RDD
      return inputJsonRecords.map(x -> "").filter(x -> !Objects.equals(x, ""));
    }
  }

}
