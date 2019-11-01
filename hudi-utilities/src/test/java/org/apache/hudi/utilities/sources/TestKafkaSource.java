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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import kafka.common.TopicAndPartition;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset;
import org.apache.spark.streaming.kafka.KafkaTestUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests against {@link AvroKafkaSource}
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
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS("delta-streamer-config/source.avsc"), jsc);
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

    Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider);
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
    long totalMsgs = CheckpointUtils.totalNewMessages(new OffsetRange[] {OffsetRange.apply(TEST_TOPIC_NAME, 0, 0, 100),
        OffsetRange.apply(TEST_TOPIC_NAME, 0, 100, 200)});
    assertEquals(200, totalMsgs);

    // should consume all the full data
    OffsetRange[] ranges =
        CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
            makeOffsetMap(new int[] {0, 1}, new long[] {300000, 350000}), 1000000L);
    assertEquals(200000, CheckpointUtils.totalNewMessages(ranges));

    // should only consume upto limit
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1}, new long[] {300000, 350000}), 10000);
    assertEquals(10000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(200000, ranges[0].fromOffset());
    assertEquals(205000, ranges[0].untilOffset());
    assertEquals(250000, ranges[1].fromOffset());
    assertEquals(255000, ranges[1].untilOffset());

    // should also consume from new partitions.
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {300000, 350000, 100000}), 1000000L);
    assertEquals(300000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(3, ranges.length);

    // for skewed offsets, does not starve any partition & can catch up
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {200010, 350000, 10000}), 100000);
    assertEquals(100000, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(10, ranges[0].count());
    assertEquals(89990, ranges[1].count());
    assertEquals(10000, ranges[2].count());

    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1}, new long[] {200000, 250000}),
        makeOffsetMap(new int[] {0, 1, 2}, new long[] {200010, 350000, 10000}), 1000000);
    assertEquals(110010, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(10, ranges[0].count());
    assertEquals(100000, ranges[1].count());
    assertEquals(10000, ranges[2].count());

    // not all partitions consume same entries.
    ranges = CheckpointUtils.computeOffsetRanges(makeOffsetMap(new int[] {0, 1, 2, 3, 4}, new long[] {0, 0, 0, 0, 0}),
        makeOffsetMap(new int[] {0, 1, 2, 3, 4}, new long[] {100, 1000, 1000, 1000, 1000}), 1001);
    assertEquals(1001, CheckpointUtils.totalNewMessages(ranges));
    assertEquals(100, ranges[0].count());
    assertEquals(226, ranges[1].count());
    assertEquals(226, ranges[2].count());
    assertEquals(226, ranges[3].count());
    assertEquals(223, ranges[4].count());
  }
}
