/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import static com.uber.hoodie.utilities.sources.KafkaSource.CheckpointUtils;
import static org.junit.Assert.assertEquals;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.UtilitiesTestBase;
import com.uber.hoodie.utilities.schema.FilebasedSchemaProvider;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import kafka.common.TopicAndPartition;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset;
import org.apache.spark.streaming.kafka.KafkaTestUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests against {@link KafkaSource}
 */
public class TestKafkaSource extends UtilitiesTestBase {

  private static String TEST_TOPIC_NAME = "hoodie_test";

  private FilebasedSchemaProvider schemaProvider;
  private KafkaTestUtils testUtils;

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
    testUtils.teardown();
  }


  @Test
  public void testJsonKafkaSource() throws IOException {

    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", TEST_TOPIC_NAME);
    props.setProperty("metadata.broker.list", testUtils.brokerAddress());
    props.setProperty("auto.offset.reset", "smallest");
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Source kafkaSource = new JsonKafkaSource(props, jsc, schemaProvider);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Optional.empty(), kafkaSource.fetchNewData(Optional.empty(), Long.MAX_VALUE).getKey());
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch1 = kafkaSource.fetchNewData(Optional.empty(), 900);
    assertEquals(900, fetch1.getKey().get().count());

    // 2. Produce new data, extract new data
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch2 = kafkaSource.fetchNewData(
        Optional.of(fetch1.getValue()), Long.MAX_VALUE);
    assertEquals(1100, fetch2.getKey().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch3 = kafkaSource.fetchNewData(
        Optional.of(fetch1.getValue()), Long.MAX_VALUE);
    assertEquals(fetch2.getKey().get().count(), fetch3.getKey().get().count());
    assertEquals(fetch2.getValue(), fetch3.getValue());

    // 4. Extract with latest checkpoint => no new data returned
    Pair<Optional<JavaRDD<GenericRecord>>, String> fetch4 = kafkaSource.fetchNewData(
        Optional.of(fetch2.getValue()), Long.MAX_VALUE);
    assertEquals(Optional.empty(), fetch4.getKey());
  }


  private static HashMap<TopicAndPartition, LeaderOffset> makeOffsetMap(int[] partitions, long[] offsets) {
    HashMap<TopicAndPartition, LeaderOffset> map = new HashMap<>();
    for (int i = 0; i < partitions.length; i++) {
      map.put(new TopicAndPartition(TEST_TOPIC_NAME, partitions[i]), new LeaderOffset("", -1, offsets[i]));
    }
    return map;
  }

  @Test
  public void testComputeOffsetRanges() {
    // test totalNewMessages()
    long totalMsgs = CheckpointUtils.totalNewMessages(new OffsetRange[]{
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 0, 100),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 100, 200)
    });
    assertEquals(200, totalMsgs);

    // should consume all the full data
    OffsetRange[] ranges = CheckpointUtils.computeOffsetRanges(
        makeOffsetMap(new int[]{0, 1}, new long[]{200000, 250000}),
        makeOffsetMap(new int[]{0, 1}, new long[]{300000, 350000}),
        1000000L
    );
    assertEquals(200000, CheckpointUtils.totalNewMessages(ranges));

    // should only consume upto limit
    ranges = CheckpointUtils.computeOffsetRanges(
        makeOffsetMap(new int[]{0, 1}, new long[]{200000, 250000}),
        makeOffsetMap(new int[]{0, 1}, new long[]{300000, 350000}),
        10000
    );
    assertEquals(10000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(200000, ranges[0].fromOffset());
    assertEquals(205000, ranges[0].untilOffset());
    assertEquals(250000, ranges[1].fromOffset());
    assertEquals(255000, ranges[1].untilOffset());

    // should also consume from new partitions.
    ranges = CheckpointUtils.computeOffsetRanges(
        makeOffsetMap(new int[]{0, 1}, new long[]{200000, 250000}),
        makeOffsetMap(new int[]{0, 1, 2}, new long[]{300000, 350000, 100000}),
        1000000L
    );
    assertEquals(300000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(3, ranges.length);

    // for skewed offsets, does not starve any partition & can catch up
    ranges = CheckpointUtils.computeOffsetRanges(
        makeOffsetMap(new int[]{0, 1}, new long[]{200000, 250000}),
        makeOffsetMap(new int[]{0, 1, 2}, new long[]{200010, 350000, 10000}),
        100000
    );
    assertEquals(100000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(10, ranges[0].count());
    assertEquals(89990, ranges[1].count());
    assertEquals(10000, ranges[2].count());

    ranges = CheckpointUtils.computeOffsetRanges(
        makeOffsetMap(new int[]{0, 1}, new long[]{200000, 250000}),
        makeOffsetMap(new int[]{0, 1, 2}, new long[]{200010, 350000, 10000}),
        1000000
    );
    assertEquals(110010, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(10, ranges[0].count());
    assertEquals(100000, ranges[1].count());
    assertEquals(10000, ranges[2].count());
  }
}
