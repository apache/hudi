/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.KinesisSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hudi.utilities.config.KinesisSourceConfig.KINESIS_REGION;
import static org.apache.hudi.utilities.config.KinesisSourceConfig.KINESIS_STARTING_POSITION;
import static org.apache.hudi.utilities.config.KinesisSourceConfig.KINESIS_STREAM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for KinesisSource.readFromCheckpoint shard filtering (hasUnreadRecords).
 */
class TestKinesisSourceFiltering extends SparkClientFunctionalTestHarness {

  private static final String STREAM_NAME = "test-stream";

  private TestableKinesisSource source;

  @BeforeEach
  void setup() {
    TypedProperties props = new TypedProperties();
    props.setProperty(KINESIS_STREAM_NAME.key(), STREAM_NAME);
    props.setProperty(KINESIS_REGION.key(), "us-east-1");
    props.setProperty(KINESIS_STARTING_POSITION.key(), "TRIM_HORIZON");

    source = new TestableKinesisSource(
        props, jsc(), spark(), null, mock(HoodieIngestionMetrics.class));
  }

  @Test
  void testAllShardsFilteredReturnsEmptyBatchWithPreservedCheckpoint() {
    KinesisOffsetGen mockOffsetGen = mock(KinesisOffsetGen.class);
    when(mockOffsetGen.getStreamName()).thenReturn(STREAM_NAME);

    // Two closed shards, both fully consumed (lastSeq >= endSeq)
    KinesisOffsetGen.KinesisShardRange[] allFiltered = {
        KinesisOffsetGen.KinesisShardRange.of("shard-0", Option.of("200"), Option.of("200")),
        KinesisOffsetGen.KinesisShardRange.of("shard-1", Option.of("300"), Option.of("300"))
    };
    when(mockOffsetGen.getNextShardRanges(any(), anyLong())).thenReturn(allFiltered);
    when(mockOffsetGen.getStartingPositionStrategy()).thenReturn(
        KinesisSourceConfig.KinesisStartingPositionStrategy.EARLIEST);

    source.setOffsetGen(mockOffsetGen);

    String previousCheckpoint = STREAM_NAME + ",shard-0:200,shard-1:300";
    Option<Checkpoint> lastCheckpoint = Option.of(new StreamerCheckpointV1(previousCheckpoint));

    org.apache.hudi.utilities.sources.InputBatch<JavaRDD<String>> batch =
        source.fetchNext(lastCheckpoint, 1000L);

    assertFalse(batch.getBatch().isPresent(), "All shards filtered: should return empty batch");
    assertEquals(previousCheckpoint, batch.getCheckpointForNextBatch().getCheckpointKey(),
        "Checkpoint should be preserved when empty batch");
    assertTrue(source.getToBatchShardIds().isEmpty(),
        "toBatch should not be called when all shards filtered");
  }

  @Test
  void testSomeShardsFilteredOnlyNonFilteredPassedToToBatch() {
    KinesisOffsetGen mockOffsetGen = mock(KinesisOffsetGen.class);
    when(mockOffsetGen.getStreamName()).thenReturn(STREAM_NAME);

    // shard-0: fully consumed (filtered). shard-1: has unread (lastSeq < endSeq). shard-2: open (not filtered)
    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shard-0", Option.of("200"), Option.of("200")),
        KinesisOffsetGen.KinesisShardRange.of("shard-1", Option.of("100"), Option.of("300")),
        KinesisOffsetGen.KinesisShardRange.of("shard-2", Option.empty(), Option.empty())
    };
    when(mockOffsetGen.getNextShardRanges(any(), anyLong())).thenReturn(ranges);
    when(mockOffsetGen.getStartingPositionStrategy()).thenReturn(
        KinesisSourceConfig.KinesisStartingPositionStrategy.EARLIEST);

    source.setOffsetGen(mockOffsetGen);

    Option<Checkpoint> lastCheckpoint = Option.of(
        new StreamerCheckpointV1(STREAM_NAME + ",shard-0:200,shard-1:100"));

    org.apache.hudi.utilities.sources.InputBatch<JavaRDD<String>> batch =
        source.fetchNext(lastCheckpoint, 1000L);

    List<String> passedShards = source.getToBatchShardIds();
    assertEquals(2, passedShards.size(), "Only shard-1 and shard-2 should be passed");
    assertTrue(passedShards.contains("shard-1"));
    assertTrue(passedShards.contains("shard-2"));
    assertFalse(passedShards.contains("shard-0"));

    // Checkpoint must include filtered shard-0 so next run doesn't re-read from TRIM_HORIZON
    String checkpoint = batch.getCheckpointForNextBatch().getCheckpointKey();
    assertTrue(checkpoint.contains("shard-0"), "Filtered shard-0 must be in checkpoint");
    assertTrue(checkpoint.contains("shard-1"));
  }

  @Test
  void testUseLatestFiltersClosedShardWithNoCheckpoint() {
    KinesisOffsetGen mockOffsetGen = mock(KinesisOffsetGen.class);
    when(mockOffsetGen.getStreamName()).thenReturn(STREAM_NAME);

    // Closed shard with no checkpoint; useLatest=true -> filtered (at tip, no records)
    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shard-0", Option.empty(), Option.of("300"))
    };
    when(mockOffsetGen.getNextShardRanges(any(), anyLong())).thenReturn(ranges);
    when(mockOffsetGen.getStartingPositionStrategy()).thenReturn(
        KinesisSourceConfig.KinesisStartingPositionStrategy.LATEST);

    source.setOffsetGen(mockOffsetGen);

    Option<Checkpoint> lastCheckpoint = Option.of(new StreamerCheckpointV1(STREAM_NAME));

    org.apache.hudi.utilities.sources.InputBatch<JavaRDD<String>> batch =
        source.fetchNext(lastCheckpoint, 1000L);

    assertFalse(batch.getBatch().isPresent(),
        "Closed shard + LATEST + no checkpoint: filtered, empty batch");
    assertTrue(source.getToBatchShardIds().isEmpty());
  }

  @Test
  void testUseTrimHorizonKeepsClosedShardWithNoCheckpoint() {
    KinesisOffsetGen mockOffsetGen = mock(KinesisOffsetGen.class);
    when(mockOffsetGen.getStreamName()).thenReturn(STREAM_NAME);

    // Closed shard with no checkpoint; useLatest=false (TRIM_HORIZON) -> not filtered
    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shard-0", Option.empty(), Option.of("300"))
    };
    when(mockOffsetGen.getNextShardRanges(any(), anyLong())).thenReturn(ranges);
    when(mockOffsetGen.getStartingPositionStrategy()).thenReturn(
        KinesisSourceConfig.KinesisStartingPositionStrategy.EARLIEST);

    source.setOffsetGen(mockOffsetGen);

    Option<Checkpoint> lastCheckpoint = Option.of(new StreamerCheckpointV1(STREAM_NAME));

    org.apache.hudi.utilities.sources.InputBatch<JavaRDD<String>> batch =
        source.fetchNext(lastCheckpoint, 1000L);

    assertTrue(batch.getBatch().isPresent(),
        "Closed shard + TRIM_HORIZON + no checkpoint: not filtered, has batch");
    assertEquals(Arrays.asList("shard-0"), source.getToBatchShardIds());
  }

  @Test
  void testNoCheckpointAllShardsPassWhenOpen() {
    KinesisOffsetGen mockOffsetGen = mock(KinesisOffsetGen.class);
    when(mockOffsetGen.getStreamName()).thenReturn(STREAM_NAME);

    KinesisOffsetGen.KinesisShardRange[] ranges = {
        KinesisOffsetGen.KinesisShardRange.of("shard-0", Option.empty(), Option.empty()),
        KinesisOffsetGen.KinesisShardRange.of("shard-1", Option.empty(), Option.empty())
    };
    when(mockOffsetGen.getNextShardRanges(any(), anyLong())).thenReturn(ranges);
    when(mockOffsetGen.getStartingPositionStrategy()).thenReturn(
        KinesisSourceConfig.KinesisStartingPositionStrategy.LATEST);

    source.setOffsetGen(mockOffsetGen);

    org.apache.hudi.utilities.sources.InputBatch<JavaRDD<String>> batch =
        source.fetchNext(Option.empty(), 1000L);

    assertTrue(batch.getBatch().isPresent());
    assertEquals(Arrays.asList("shard-0", "shard-1"), source.getToBatchShardIds(),
        "Open shards always pass filter");
  }

  /**
   * Testable KinesisSource that records which shard ranges are passed to toBatch.
   * Does not call real Kinesis.
   */
  private static class TestableKinesisSource extends KinesisSource<JavaRDD<String>> {

    private final AtomicReference<List<String>> toBatchShardIds = new AtomicReference<>(new ArrayList<>());

    TestableKinesisSource(TypedProperties properties, JavaSparkContext sparkContext,
        org.apache.spark.sql.SparkSession sparkSession, SchemaProvider schemaProvider,
        HoodieIngestionMetrics metrics) {
      super(properties, sparkContext, sparkSession, SourceType.JSON, metrics,
          new DefaultStreamContext(schemaProvider, Option.empty()));
    }

    void setOffsetGen(KinesisOffsetGen gen) {
      this.offsetGen = gen;
    }

    List<String> getToBatchShardIds() {
      return new ArrayList<>(toBatchShardIds.get());
    }

    @Override
    protected JavaRDD<String> toBatch(KinesisOffsetGen.KinesisShardRange[] shardRanges, long sourceLimit) {
      List<String> ids = new ArrayList<>();
      for (KinesisOffsetGen.KinesisShardRange r : shardRanges) {
        ids.add(r.getShardId());
      }
      toBatchShardIds.set(ids);
      return sparkContext.emptyRDD();
    }

    @Override
    protected String createCheckpointFromBatch(JavaRDD<String> batch,
        KinesisOffsetGen.KinesisShardRange[] shardRangesWithUnreadRecords,
        KinesisOffsetGen.KinesisShardRange[] allOpenClosedShardRanges) {
      lastCheckpointData = new java.util.HashMap<>();
      for (KinesisOffsetGen.KinesisShardRange r : shardRangesWithUnreadRecords) {
        lastCheckpointData.put(r.getShardId(), r.getStartingSequenceNumber().orElse(""));
      }
      // Include filtered shards from allShardRanges so checkpoint is complete
      java.util.Map<String, String> full = new java.util.HashMap<>();
      for (KinesisOffsetGen.KinesisShardRange r : allOpenClosedShardRanges) {
        String lastSeq = lastCheckpointData.containsKey(r.getShardId())
            ? lastCheckpointData.get(r.getShardId())
            : r.getStartingSequenceNumber().orElse("");
        String endSeq = r.getEndingSequenceNumber().orElse(null);
        if (lastSeq != null && !lastSeq.isEmpty()) {
          full.put(r.getShardId(),
              endSeq != null ? lastSeq + "|" + endSeq : lastSeq);
        }
      }
      return KinesisOffsetGen.CheckpointUtils.offsetsToStr(offsetGen.getStreamName(), full);
    }

    @Override
    protected long getRecordCount(JavaRDD<String> batch) {
      return 0;
    }
  }
}
