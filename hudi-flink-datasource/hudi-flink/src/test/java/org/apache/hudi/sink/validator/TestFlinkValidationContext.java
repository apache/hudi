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

package org.apache.hudi.sink.validator;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlinkValidationContext}.
 */
public class TestFlinkValidationContext {

  @Test
  public void testBasicProperties() {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata("key1", "value1");

    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setNumInserts(100);
    stat.setNumUpdateWrites(50);
    List<HoodieWriteStat> stats = Collections.singletonList(stat);

    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(metadata),
        Option.of(stats),
        Option.empty());

    assertEquals("20260320120000000", ctx.getInstantTime());
    assertTrue(ctx.getCommitMetadata().isPresent());
    assertTrue(ctx.getWriteStats().isPresent());
    assertEquals(1, ctx.getWriteStats().get().size());
  }

  @Test
  public void testTotalRecordsWritten() {
    HoodieWriteStat stat1 = new HoodieWriteStat();
    stat1.setNumInserts(100);
    stat1.setNumUpdateWrites(50);

    HoodieWriteStat stat2 = new HoodieWriteStat();
    stat2.setNumInserts(200);
    stat2.setNumUpdateWrites(30);

    List<HoodieWriteStat> stats = Arrays.asList(stat1, stat2);

    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(stats),
        Option.empty());

    assertEquals(380, ctx.getTotalRecordsWritten());
    assertEquals(300, ctx.getTotalInsertRecordsWritten());
    assertEquals(80, ctx.getTotalUpdateRecordsWritten());
  }

  @Test
  public void testIsFirstCommit() {
    FlinkValidationContext ctxFirst = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.empty());

    assertTrue(ctxFirst.isFirstCommit());

    FlinkValidationContext ctxNotFirst = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.of(new HoodieCommitMetadata()));

    assertFalse(ctxNotFirst.isFirstCommit());
  }

  @Test
  public void testExtraMetadata() {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata("checkpoint", "topic,0:100");
    metadata.addMetadata("other_key", "other_value");

    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(metadata),
        Option.of(Collections.emptyList()),
        Option.empty());

    assertEquals(2, ctx.getExtraMetadata().size());
    assertTrue(ctx.getExtraMetadata("checkpoint").isPresent());
    assertEquals("topic,0:100", ctx.getExtraMetadata("checkpoint").get());
    assertFalse(ctx.getExtraMetadata("nonexistent").isPresent());
  }

  @Test
  public void testPreviousCommitMetadata() {
    HoodieCommitMetadata prevMeta = new HoodieCommitMetadata();
    prevMeta.addMetadata("checkpoint", "topic,0:50");

    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.of(prevMeta));

    assertTrue(ctx.getPreviousCommitMetadata().isPresent());
    assertEquals("topic,0:50", ctx.getPreviousCommitMetadata().get().getMetadata("checkpoint"));
  }

  @Test
  public void testGetActiveTimelineThrows() {
    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.empty());

    assertThrows(UnsupportedOperationException.class, ctx::getActiveTimeline);
  }

  @Test
  public void testGetPreviousCommitInstantThrows() {
    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(Collections.emptyList()),
        Option.of(new HoodieCommitMetadata()));

    assertThrows(UnsupportedOperationException.class, ctx::getPreviousCommitInstant);
  }

  @Test
  public void testMultipleWriteStatsAggregation() {
    HoodieWriteStat stat1 = new HoodieWriteStat();
    stat1.setNumInserts(10);
    stat1.setNumUpdateWrites(5);

    HoodieWriteStat stat2 = new HoodieWriteStat();
    stat2.setNumInserts(20);
    stat2.setNumUpdateWrites(15);

    HoodieWriteStat stat3 = new HoodieWriteStat();
    stat3.setNumInserts(30);
    stat3.setNumUpdateWrites(25);

    List<HoodieWriteStat> stats = Arrays.asList(stat1, stat2, stat3);

    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.of(stats),
        Option.empty());

    assertEquals(60, ctx.getTotalInsertRecordsWritten());
    assertEquals(45, ctx.getTotalUpdateRecordsWritten());
    assertEquals(105, ctx.getTotalRecordsWritten());
  }

  @Test
  public void testEmptyWriteStats() {
    FlinkValidationContext ctx = new FlinkValidationContext(
        "20260320120000000",
        Option.of(new HoodieCommitMetadata()),
        Option.empty(),
        Option.empty());

    assertEquals(0, ctx.getTotalRecordsWritten());
    assertEquals(0, ctx.getTotalInsertRecordsWritten());
    assertEquals(0, ctx.getTotalUpdateRecordsWritten());
  }
}
