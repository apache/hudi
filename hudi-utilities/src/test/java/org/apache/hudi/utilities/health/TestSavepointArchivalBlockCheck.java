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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieArchivalConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link SavepointArchivalBlockCheck}.
 *
 * <p>Archival stops at the earliest savepoint, so the behaviour under test is unconditional: a
 * savepoint sitting behind the archival retention pins the timeline, and the only remedy offered is
 * releasing it.
 */
public class TestSavepointArchivalBlockCheck {

  private static final DateTimeFormatter INSTANT_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneId.systemDefault());

  @TempDir
  Path tempDir;

  private String basePath;
  private final SavepointArchivalBlockCheck check = new SavepointArchivalBlockCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  /** An instant time the given number of days in the past, parseable by the staleness check. */
  private static String instantDaysAgo(int daysAgo) {
    return INSTANT_FORMAT.format(Instant.now().minus(daysAgo, ChronoUnit.DAYS));
  }

  private TypedProperties propsWithRetention(int maxCommitsToKeep) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), String.valueOf(maxCommitsToKeep));
    return props;
  }

  private HealthCheckResult runCheck(HoodieTableMetaClient metaClient, TypedProperties props) throws Exception {
    return check.check(new HealthCheckContext(HoodieTableMetaClient.reload(metaClient), props, false));
  }

  @Test
  public void tableWithNoSavepointsIsHealthy() throws Exception {
    // given a table that has never been savepointed
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(metaClient).addCommit("001").addCommit("002");

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(10));

    // then nothing is blocking archival
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
    assertEquals("0", result.getEffectiveConfigs().get("observed.savepoint.count"));
  }

  @Test
  public void missingRetentionConfigSkipsRatherThanGuessing() throws Exception {
    // given a table with a savepoint
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    String savepointed = instantDaysAgo(30);
    table.addCommit(savepointed);
    table.addSavepoint(savepointed, table.getSavepointMetadata(savepointed, Collections.emptyMap()));

    // when the check runs without the archival retention configured
    HealthCheckResult result = runCheck(metaClient, new TypedProperties());

    // then it declines to judge and names what it needs
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key()));
  }

  @Test
  public void recentSavepointInsideRetentionIsHealthy() throws Exception {
    // given a small table whose only savepoint is well inside the retention window
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    String savepointed = instantDaysAgo(1);
    table.addCommit(savepointed);
    table.addSavepoint(savepointed, table.getSavepointMetadata(savepointed, Collections.emptyMap()));

    // when the check runs with a retention larger than the timeline
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(10));

    // then archival is not yet blocked by it
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
  }

  @Test
  public void savepointBehindRetentionBlocksArchival() throws Exception {
    // given a savepoint on the oldest commit, with many commits written after it
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    String savepointed = instantDaysAgo(60);
    table.addCommit(savepointed);
    table.addSavepoint(savepointed, table.getSavepointMetadata(savepointed, Collections.emptyMap()));
    for (int i = 1; i <= 20; i++) {
      table.addCommit(instantDaysAgo(60 - i));
    }

    // when the check runs with a retention of 5 commits
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(5));

    // then the savepoint is reported as blocking archival, naming the instant
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertTrue(result.getSummary().contains(savepointed));
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("Archival stops at the earliest savepoint")),
        "expected the archival-stops finding, got: " + result.getFindings());
  }

  @Test
  public void remedyIsAlwaysToReleaseTheSavepoint() throws Exception {
    // given a savepoint blocking archival
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    String savepointed = instantDaysAgo(60);
    table.addCommit(savepointed);
    table.addSavepoint(savepointed, table.getSavepointMetadata(savepointed, Collections.emptyMap()));
    for (int i = 1; i <= 20; i++) {
      table.addCommit(instantDaysAgo(60 - i));
    }

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(5));

    // then the advice is to delete the savepoint, and never to loosen archival past savepoints
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("savepoint delete")),
        "expected advice to delete the savepoint, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().noneMatch(f -> f.contains("beyond.savepoint")),
        "must not suggest archiving beyond savepoints: " + result.getFindings());
    assertTrue(result.getEffectiveConfigs().keySet().stream().noneMatch(k -> k.contains("beyond.savepoint")),
        "must not report a config this tool does not rely on: " + result.getEffectiveConfigs());
  }

  @Test
  public void staleSavepointIsFlaggedEvenWhenItIsNotYetBlocking() throws Exception {
    // given a long-forgotten savepoint on a table too short for it to block archival yet
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    String savepointed = instantDaysAgo(90);
    table.addCommit(savepointed);
    table.addSavepoint(savepointed, table.getSavepointMetadata(savepointed, Collections.emptyMap()));

    // when the check runs with a retention larger than the timeline
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(50));

    // then archival is not blocked, but the forgotten savepoint is still surfaced
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("days old")),
        "expected a staleness finding, got: " + result.getFindings());
  }
}
