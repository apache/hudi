/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer.validator;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SparkValidationContext}.
 */
public class TestSparkValidationContext {

  private static HoodieWriteStat buildStat(long inserts, long updates) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setNumInserts(inserts);
    stat.setNumUpdateWrites(updates);
    stat.setPartitionPath("partition1");
    return stat;
  }

  @Test
  public void testBasicProperties() {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata("key1", "value1");
    List<HoodieWriteStat> writeStats = Collections.singletonList(buildStat(100, 50));

    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(metadata),
        Option.of(writeStats),
        Option.empty());

    assertEquals("20260320120000000", ctx.getInstantTime());
    assertTrue(ctx.getCommitMetadata().isPresent());
    assertTrue(ctx.getWriteStats().isPresent());
    assertEquals(1, ctx.getWriteStats().get().size());
  }

  @Test
  public void testRecordCounting() {
    List<HoodieWriteStat> writeStats = Arrays.asList(
        buildStat(100, 50),   // partition1: 100 inserts, 50 updates
        buildStat(200, 30));  // partition2: 200 inserts, 30 updates

    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(writeStats),
        Option.empty());

    assertEquals(300, ctx.getTotalInsertRecordsWritten());
    assertEquals(80, ctx.getTotalUpdateRecordsWritten());
    assertEquals(380, ctx.getTotalRecordsWritten());
  }

  @Test
  public void testFirstCommitDetection() {
    // No previous commit metadata -> first commit
    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.empty());

    assertTrue(ctx.isFirstCommit());
  }

  @Test
  public void testNotFirstCommitWhenPreviousExists() {
    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();

    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.of(prevMeta));

    assertFalse(ctx.isFirstCommit());
  }

  @Test
  public void testExtraMetadataAccess() {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:1000");
    metadata.addMetadata("custom.key", "custom_value");

    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(metadata),
        Option.of(Collections.emptyList()),
        Option.empty());

    assertEquals("events,0:1000",
        ctx.getExtraMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1).get());
    assertEquals("custom_value", ctx.getExtraMetadata("custom.key").get());
    assertFalse(ctx.getExtraMetadata("nonexistent.key").isPresent());
  }

  @Test
  public void testPreviousCommitMetadataAccess() {
    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1, "events,0:500");

    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.of(prevMeta));

    assertTrue(ctx.getPreviousCommitMetadata().isPresent());
    assertEquals("events,0:500",
        ctx.getPreviousCommitMetadata().get().getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1));
  }

  @Test
  public void testEmptyWriteStats() {
    SparkValidationContext ctx = new SparkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.empty(),
        Option.empty());

    assertEquals(0, ctx.getTotalRecordsWritten());
    assertEquals(0, ctx.getTotalInsertRecordsWritten());
    assertEquals(0, ctx.getTotalUpdateRecordsWritten());
  }
}
