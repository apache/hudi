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

package org.apache.hudi.utilities.health;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CompactionCadenceCheck} against real timelines built with {@link HoodieTestTable}.
 */
public class TestCompactionCadenceCheck {

  @TempDir
  Path tempDir;

  private String basePath;
  private final CompactionCadenceCheck check = new CompactionCadenceCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  private HoodieTableMetaClient initTable(HoodieTableType tableType) throws Exception {
    return HoodieTestUtils.init(basePath, tableType);
  }

  private TypedProperties propsWithTrigger(int maxDeltaCommits) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(), "NUM_COMMITS");
    props.setProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(),
        String.valueOf(maxDeltaCommits));
    return props;
  }

  private HealthCheckResult runCheck(HoodieTableMetaClient metaClient, TypedProperties props,
                                     boolean applyAllDefaults) throws Exception {
    // Reload so the check sees every instant written after the meta client was created.
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    return check.check(new HealthCheckContext(reloaded, props, applyAllDefaults));
  }

  @Test
  public void copyOnWriteTableIsSkippedBecauseItHasNoCompaction() throws Exception {
    // given a Copy-on-Write table
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(metaClient).addCommit("001").addCommit("002");

    // when the compaction check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(5), false);

    // then it is skipped rather than reported either way
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains("Copy-on-Write"));
  }

  @Test
  public void missingTriggerConfigSkipsRatherThanAssumingDefaults() throws Exception {
    // given a MOR table with delta commits piled up well past the stock trigger
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = 1; i <= 40; i++) {
      table.addDeltaCommit(String.format("%03d", i));
    }

    // when the check runs without the writer's trigger configuration
    HealthCheckResult result = runCheck(metaClient, new TypedProperties(), false);

    // then it declines to judge, naming what it needs, instead of calling the table unhealthy
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key()));
  }

  @Test
  public void applyAllDefaultsEvaluatesWithoutSuppliedConfig() throws Exception {
    // given the same table and still no supplied configuration
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = 1; i <= 40; i++) {
      table.addDeltaCommit(String.format("%03d", i));
    }

    // when the operator explicitly opts into Hudi defaults
    HealthCheckResult result = runCheck(metaClient, new TypedProperties(), true);

    // then the check runs and, with 40 delta commits against a default trigger of 5, reports badly behind
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
  }

  @Test
  public void deltaCommitsWithinSlackAreHealthy() throws Exception {
    // given a MOR table whose delta commits sit inside the trigger threshold plus slack
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    table.addDeltaCommit("001").addDeltaCommit("002").addDeltaCommit("003");

    // when the check runs with a trigger of 5 (threshold 10 after the 2.0 slack factor)
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(5), false);

    // then the table is healthy -- scheduling jitter is not an alert
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
  }

  @Test
  public void deltaCommitsPastSlackAreUnhealthyAndSayNoCompactionIsScheduled() throws Exception {
    // given a MOR table with delta commits far past the trigger and nothing scheduled
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = 1; i <= 25; i++) {
      table.addDeltaCommit(String.format("%03d", i));
    }

    // when the check runs with a trigger of 5 (threshold 10)
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(5), false);

    // then it reports unhealthy and points at compaction not being scheduled at all
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("No compaction is scheduled")),
        "expected a finding about compaction not being scheduled, got: " + result.getFindings());
  }

  @Test
  public void pendingCompactionIsCalledOutAsSchedulingWithoutExecution() throws Exception {
    // given a MOR table behind on compaction, with a plan requested but never completed
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = 1; i <= 25; i++) {
      table.addDeltaCommit(String.format("%03d", i));
    }
    table.addRequestedCompaction("026");

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(5), false);

    // then the diagnosis distinguishes "not scheduled" from "scheduled but not executed"
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("not executed")),
        "expected a finding about compaction not executing, got: " + result.getFindings());
  }

  @Test
  public void deltaCommitsAfterACompletedCompactionResetTheCount() throws Exception {
    // given a MOR table that fell behind, then compacted, then took a few more delta commits.
    // Completion times are set explicitly and kept in step with requested times: the underlying
    // timeline lookup selects delta commits by *completion* time, so a fixture that leaves them
    // unset does not model a real timeline.
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = 1; i <= 25; i++) {
      String instant = String.format("%03d", i);
      table.addDeltaCommit(instant, Option.of(instant), new HoodieCommitMetadata());
    }
    table.addCompaction("026", Option.of("026"), new HoodieCommitMetadata());
    table.addDeltaCommit("027", Option.of("027"), new HoodieCommitMetadata());
    table.addDeltaCommit("028", Option.of("028"), new HoodieCommitMetadata());

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(5), false);

    // then only the two delta commits after that compaction count, so the table reads healthy
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
    assertEquals("026", result.getEffectiveConfigs().get("observed.last.compaction.instant"));
    assertEquals("2", result.getEffectiveConfigs().get("observed.delta.commits.since.last.compaction"));
  }

  @Test
  public void effectiveConfigIsEchoedSoAVerdictCanBeAudited() throws Exception {
    // given any MOR table
    HoodieTableMetaClient metaClient = initTable(HoodieTableType.MERGE_ON_READ);
    HoodieTestTable.of(metaClient).addDeltaCommit("001");

    // when the check runs with a known trigger
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(7), false);

    // then the values it reasoned with are reported alongside the verdict
    assertEquals("7", result.getEffectiveConfigs()
        .get(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key()));
    assertEquals("NUM_COMMITS", result.getEffectiveConfigs()
        .get(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key()));
    assertEquals("14", result.getEffectiveConfigs().get("effective.unhealthy.threshold.delta.commits"));
  }
}
