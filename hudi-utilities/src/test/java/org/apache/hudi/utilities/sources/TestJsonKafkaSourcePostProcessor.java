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
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config.JSON_KAFKA_PROCESSOR_CLASS_OPT;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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

}
