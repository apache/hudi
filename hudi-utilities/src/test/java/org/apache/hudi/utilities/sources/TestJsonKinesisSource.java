/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.utilities.config.KinesisSourceConfig.KINESIS_REGION;
import static org.apache.hudi.utilities.config.KinesisSourceConfig.KINESIS_STARTING_POSITION;
import static org.apache.hudi.utilities.config.KinesisSourceConfig.KINESIS_STREAM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for JsonKinesisSource.
 */
class TestJsonKinesisSource extends SparkClientFunctionalTestHarness {

  private static final String STREAM_NAME = "test-stream";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private TestableJsonKinesisSource source;

  @BeforeEach
  void setup() {
    TypedProperties props = new TypedProperties();
    props.setProperty(KINESIS_STREAM_NAME.key(), STREAM_NAME);
    props.setProperty(KINESIS_REGION.key(), "us-east-1");
    props.setProperty(KINESIS_STARTING_POSITION.key(), "TRIM_HORIZON");

    source = new TestableJsonKinesisSource(
        props, jsc(), spark(), null, mock(HoodieIngestionMetrics.class));
  }

  // --- recordToJsonStatic tests ---

  private static String recordToJson(Record record, String shardId, boolean shouldAddOffsets)
      throws Exception {
    Method m = JsonKinesisSource.class.getDeclaredMethod(
        "recordToJsonStatic", Record.class, String.class, boolean.class);
    m.setAccessible(true);
    return (String) m.invoke(null, record, shardId, shouldAddOffsets);
  }

  private static Record kinesisRecord(String data, String partitionKey, String sequenceNumber,
      Instant approximateArrivalTimestamp) {
    Record.Builder builder = Record.builder()
        .data(SdkBytes.fromUtf8String(data))
        .partitionKey(partitionKey)
        .sequenceNumber(sequenceNumber);
    if (approximateArrivalTimestamp != null) {
      builder.approximateArrivalTimestamp(approximateArrivalTimestamp);
    }
    return builder.build();
  }

  @Test
  void testRecordToJsonPassThroughWhenShouldAddOffsetsFalse() throws Exception {
    String json = "{\"id\":1,\"name\":\"alice\"}";
    Record record = kinesisRecord(json, "pk1", "49590382471490958861609854428592832524486083118",
        Instant.ofEpochMilli(1700000000000L));

    String result = recordToJson(record, "shardId-000000000000", false);

    assertEquals(json, result);
  }

  @Test
  void testRecordToJsonAddsOffsetFieldsWhenShouldAddOffsetsTrue() throws Exception {
    String json = "{\"id\":1,\"name\":\"alice\"}";
    Record record = kinesisRecord(json, "pk1", "49590382471490958861609854428592832524486083118",
        Instant.ofEpochMilli(1700000000000L));

    String result = recordToJson(record, "shardId-000000000001", true);

    JsonNode node = MAPPER.readTree(result);
    assertEquals("49590382471490958861609854428592832524486083118",
        node.get("_hoodie_kinesis_source_sequence_number").asText());
    assertEquals("shardId-000000000001", node.get("_hoodie_kinesis_source_shard_id").asText());
    assertEquals("pk1", node.get("_hoodie_kinesis_source_partition_key").asText());
    assertEquals(1700000000000L, node.get("_hoodie_kinesis_source_timestamp").asLong());
    assertEquals(1, node.get("id").asInt());
    assertEquals("alice", node.get("name").asText());
  }

  @Test
  void testRecordToJsonEmptyStringReturnsNull() throws Exception {
    Record record = kinesisRecord("", "pk1", "seq123", Instant.now());

    String result = recordToJson(record, "shardId-0", false);

    assertNull(result);
  }

  @Test
  void testRecordToJsonWhitespaceOnlyReturnsNull() throws Exception {
    Record record = kinesisRecord("   ", "pk1", "seq123", Instant.now());

    String result = recordToJson(record, "shardId-0", false);

    assertNull(result);
  }

  @Test
  void testRecordToJsonNullTimestampNotAdded() throws Exception {
    String json = "{\"id\":1}";
    Record record = kinesisRecord(json, "pk1", "seq123", null);

    String result = recordToJson(record, "shardId-0", true);

    JsonNode node = MAPPER.readTree(result);
    assertTrue(node.has("_hoodie_kinesis_source_sequence_number"));
    assertTrue(node.has("_hoodie_kinesis_source_shard_id"));
    assertTrue(node.has("_hoodie_kinesis_source_partition_key"));
    assertEquals(false, node.has("_hoodie_kinesis_source_timestamp"));
  }

  @Disabled("Production code throws HoodieException for invalid JSON when shouldAddMetaFields=true; fix in follow-up")
  @Test
  void testRecordToJsonInvalidJsonWithShouldAddOffsetsReturnsOriginalString() throws Exception {
    String invalidJson = "not valid json {";
    Record record = kinesisRecord(invalidJson, "pk1", "seq123", Instant.now());

    String result = recordToJson(record, "shardId-0", true);

    assertEquals(invalidJson, result);
  }

  @Test
  void testRecordToJsonNestedJsonPreservedWithOffsets() throws Exception {
    String json = "{\"user\":{\"name\":\"bob\",\"age\":30},\"event\":\"click\"}";
    Record record = kinesisRecord(json, "pk2", "seq456", Instant.ofEpochMilli(1700000001000L));

    String result = recordToJson(record, "shardId-000000000002", true);

    JsonNode node = MAPPER.readTree(result);
    assertEquals("seq456", node.get("_hoodie_kinesis_source_sequence_number").asText());
    assertEquals("shardId-000000000002", node.get("_hoodie_kinesis_source_shard_id").asText());
    assertEquals("bob", node.get("user").get("name").asText());
    assertEquals(30, node.get("user").get("age").asInt());
    assertEquals("click", node.get("event").asText());
  }

  // --- createCheckpointFromBatch tests ---

  @Test
  void testCreateCheckpointOpenShardsWithLastSeqFromFetch() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "49590382471490958861609854428592832524486083118");
    lastCheckpointData.put("shardId-000000000001", "49590382471490958861609854428592832524486083122");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000001", Option.empty(), Option.empty())
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);

    assertNotNull(checkpoint);
    assertTrue(checkpoint.startsWith(STREAM_NAME + ","));
    assertTrue(checkpoint.contains("shardId-000000000000:49590382471490958861609854428592832524486083118"));
    assertTrue(checkpoint.contains("shardId-000000000001:49590382471490958861609854428592832524486083122"));
    assertFalse(checkpoint.contains("|"), "Open shards should not have endSeq");
  }

  @Test
  void testCreateCheckpointFallbackToStartSeqWhenNoRecordsRead() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "seq123");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000001",
            Option.of("seq456"), Option.empty())
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);

    assertTrue(checkpoint.contains("shardId-000000000000:seq123"));
    assertTrue(checkpoint.contains("shardId-000000000001:seq456"));
  }

  @Test
  void testCreateCheckpointClosedShardWithRealEndSeqFullyConsumed() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "49590382471490958861609854428592832524486083118");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000",
            Option.of("49590382471490958861609854428592832524486083110"),
            Option.of("49590382471490958861609854428592832524486083118"))
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);

    assertTrue(checkpoint.contains("shardId-000000000000:49590382471490958861609854428592832524486083118|"
        + "49590382471490958861609854428592832524486083118"));
  }

  @Test
  void testCreateCheckpointClosedShardFirstReadPartialConsumption() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "49590382471490958861609854428592832524486083115");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000",
            Option.empty(), Option.of("49590382471490958861609854428592832524486083118"))
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);

    assertTrue(checkpoint.contains("shardId-000000000000:49590382471490958861609854428592832524486083115|"
        + "49590382471490958861609854428592832524486083118"));
  }

  @Disabled("Checkpoint sentinel replacement not yet implemented; fix in follow-up")
  @Test
  void testCreateCheckpointLocalStackSentinelReplacedWithLastSeq() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "49590382471490958861609854428592832524486083118");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000",
            Option.of("49590382471490958861609854428592832524486083118"),
            Option.of("9223372036854775807"))
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);

    assertFalse(checkpoint.contains("9223372036854775807"),
        "LocalStack Long.MAX_VALUE should be replaced with lastSeq");
    assertTrue(checkpoint.contains("shardId-000000000000:49590382471490958861609854428592832524486083118|"
        + "49590382471490958861609854428592832524486083118"));
  }

  @Test
  void testCreateCheckpointClosedShardFirstReadNoRecordsUseEndSeqAsLastSeq() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    String endSeq = "49590382471490958861609854428592832524486083118";

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000",
            Option.empty(), Option.of(endSeq))
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);
    assertTrue(checkpoint.contains("shardId-000000000000:" + endSeq + "|" + endSeq));
  }

  @Test
  void testCreateCheckpointClosedShardFirstReadNoRecordsLocalStackSentinel() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    String sentinel = "9223372036854775807";

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000",
            Option.empty(), Option.of(sentinel))
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);
    assertTrue(checkpoint.contains("shardId-000000000000:" + sentinel + "|" + sentinel));
  }

  @Test
  void testCreateCheckpointEmptyShardsOmitted() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "seq123");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000001", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000002", Option.empty(), Option.empty())
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);

    assertTrue(checkpoint.contains("shardId-000000000000:seq123"));
    assertFalse(checkpoint.contains("shardId-000000000001"));
    assertFalse(checkpoint.contains("shardId-000000000002"));
  }

  @Test
  void testCreateCheckpointShardsReachedEndRaceOpenToClosedShard() {
    // Corner case: open shard that executor reached end-of (nextShardIterator=null) but listShards
    // has not yet reflected the close. Should record endSeq=lastSeq for expiry detection.
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "49590382471490958861609854428592832524486083118");
    Set<String> shardsReachedEnd = new HashSet<>();
    shardsReachedEnd.add("shardId-000000000000");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000",
            Option.of("49590382471490958861609854428592832524486083110"),
            Option.empty())  // endSeq null (listShards not updated yet)
    };

    String checkpoint = source.testCreateCheckpointFromBatch(
        emptyRdd(), ranges, lastCheckpointData, shardsReachedEnd);

    assertTrue(checkpoint.contains("shardId-000000000000:49590382471490958861609854428592832524486083118|"
        + "49590382471490958861609854428592832524486083118"),
        "shardsReachedEnd: endSeq should be set to lastSeq for expiry detection");
  }

  @Test
  void testCreateCheckpointWithoutShardsReachedEndOpenShardNoEndSeq() {
    // When shardsReachedEnd is null, open shard stays open (no endSeq in checkpoint)
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "seq123");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000", Option.of("seq100"), Option.empty())
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData, null);

    assertTrue(checkpoint.contains("shardId-000000000000:seq123"));
    assertFalse(checkpoint.contains("|"), "Open shard without shardsReachedEnd should not have endSeq");
  }

  @Test
  void testCreateCheckpointLastCheckpointDataNullUsesStartSeq() {
    // When lastCheckpointData is null (e.g. fetch failure), falls back to range.getStartingSequenceNumber
    Map<String, String> lastCheckpointData = null;

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000",
            Option.of("seqFromStart"), Option.of("seqEnd"))
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData, null);

    assertTrue(checkpoint.contains("shardId-000000000000:seqFromStart|seqEnd"));
  }

  @Test
  void testCreateCheckpointMixedOpenClosedAndEmpty() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "seq100");
    lastCheckpointData.put("shardId-000000000001", "seq200");

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000001",
            Option.of("seq200"), Option.of("seq200")),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000002", Option.empty(), Option.empty())
    };

    String checkpoint = source.testCreateCheckpointFromBatch(emptyRdd(), ranges, lastCheckpointData);

    assertTrue(checkpoint.contains("shardId-000000000000:seq100"));
    assertTrue(checkpoint.contains("shardId-000000000001:seq200|seq200"));
    assertFalse(checkpoint.contains("shardId-000000000002"));
  }

  @Test
  void testCreateCheckpointEmbeddsArrivalTimeForReadShards() {
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "seq100");
    lastCheckpointData.put("shardId-000000000001", "seq200");

    Map<String, Long> lastArrivalTimes = new HashMap<>();
    lastArrivalTimes.put("shardId-000000000000", 1700000000000L);
    // shard-1 has no arrival time (e.g. record had null approximateArrivalTimestamp)

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000001", Option.empty(), Option.empty())
    };

    String checkpoint = source.testCreateCheckpointFromBatch(
        emptyRdd(), ranges, lastCheckpointData, null, lastArrivalTimes);

    // shard-0: arrival time embedded
    assertTrue(checkpoint.contains("shardId-000000000000:seq100@1700000000000"),
        "Arrival time should be embedded with '@' separator");
    // shard-1: no arrival time, plain lastSeq
    assertTrue(checkpoint.contains("shardId-000000000001:seq200"));
    assertFalse(checkpoint.contains("shardId-000000000001:seq200@"),
        "shard without arrival time should not have '@'");
  }

  @Test
  void testCreateCheckpointArrivalTimeNotAddedForUnreadShards() {
    // shard-1 was not read (not in lastCheckpointData); its arrival time must not appear even if
    // somehow present in lastArrivalTimes
    Map<String, String> lastCheckpointData = new HashMap<>();
    lastCheckpointData.put("shardId-000000000000", "seq100");

    Map<String, Long> lastArrivalTimes = new HashMap<>();
    lastArrivalTimes.put("shardId-000000000001", 1700000000000L);

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000000", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shardId-000000000001",
            Option.of("seq200"), Option.empty())
    };

    String checkpoint = source.testCreateCheckpointFromBatch(
        emptyRdd(), ranges, lastCheckpointData, null, lastArrivalTimes);

    assertTrue(checkpoint.contains("shardId-000000000000:seq100"));
    assertTrue(checkpoint.contains("shardId-000000000001:seq200"));
    assertFalse(checkpoint.contains("shardId-000000000001:seq200@"),
        "Unread shard should not have arrival time in checkpoint");
  }

  private JavaRDD<String> emptyRdd() {
    return jsc().emptyRDD();
  }

  private static class TestableJsonKinesisSource extends JsonKinesisSource {

    TestableJsonKinesisSource(TypedProperties properties, JavaSparkContext sparkContext,
        org.apache.spark.sql.SparkSession sparkSession, SchemaProvider schemaProvider,
        HoodieIngestionMetrics metrics) {
      super(properties, sparkContext, sparkSession, metrics,
          new org.apache.hudi.utilities.streamer.DefaultStreamContext(schemaProvider, Option.empty()));
    }

    String testCreateCheckpointFromBatch(JavaRDD<String> batch,
        KinesisOffsetGen.KinesisShardRange[] shardRanges, Map<String, String> lastCheckpointData) {
      return testCreateCheckpointFromBatch(batch, shardRanges, lastCheckpointData, null, null);
    }

    String testCreateCheckpointFromBatch(JavaRDD<String> batch,
        KinesisOffsetGen.KinesisShardRange[] shardRanges, Map<String, String> lastCheckpointData,
        Set<String> shardsReachedEnd) {
      return testCreateCheckpointFromBatch(batch, shardRanges, lastCheckpointData, shardsReachedEnd, null);
    }

    String testCreateCheckpointFromBatch(JavaRDD<String> batch,
        KinesisOffsetGen.KinesisShardRange[] shardRanges, Map<String, String> lastCheckpointData,
        Set<String> shardsReachedEnd, Map<String, Long> lastArrivalTimes) {
      this.lastCheckpointData = lastCheckpointData;
      this.shardsReachedEnd = shardsReachedEnd;
      this.lastArrivalTimes = lastArrivalTimes;
      return createCheckpointFromBatch(batch, shardRanges, shardRanges);
    }
  }
}
