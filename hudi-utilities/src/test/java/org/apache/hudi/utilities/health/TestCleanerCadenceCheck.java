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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieCleanConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CleanerCadenceCheck}.
 */
public class TestCleanerCadenceCheck {

  @TempDir
  Path tempDir;

  private String basePath;
  private final CleanerCadenceCheck check = new CleanerCadenceCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  /** A serializable but empty plan -- the Avro schema rejects the no-arg constructor's nulls. */
  private static HoodieCleanerPlan emptyCleanerPlan() {
    return new HoodieCleanerPlan(new HoodieActionInstant("", "", ""), "", "", new HashMap<>(),
        CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
  }

  private TypedProperties propsWithTrigger(int triggerCommits) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key(), String.valueOf(triggerCommits));
    return props;
  }

  private HealthCheckResult runCheck(HoodieTableMetaClient metaClient, TypedProperties props,
                                     boolean applyAllDefaults) throws Exception {
    return check.check(new HealthCheckContext(HoodieTableMetaClient.reload(metaClient), props, applyAllDefaults));
  }

  private HoodieTestTable tableWithCommits(HoodieTableMetaClient metaClient, int from, int to) throws Exception {
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = from; i <= to; i++) {
      table.addCommit(String.format("%05d", i));
    }
    return table;
  }

  @Test
  public void missingTriggerConfigSkipsRatherThanGuessing() throws Exception {
    // given a table with many commits and no clean
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    tableWithCommits(metaClient, 1, 30);

    // when the check runs without the cleaner trigger configured
    HealthCheckResult result = runCheck(metaClient, new TypedProperties(), false);

    // then it declines to judge and names what it needs
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key()));
  }

  @Test
  public void applyAllDefaultsEvaluatesWithoutSuppliedConfig() throws Exception {
    // given the same never-cleaned table
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    tableWithCommits(metaClient, 1, 30);

    // when the operator explicitly opts into Hudi defaults (trigger of 1)
    HealthCheckResult result = runCheck(metaClient, new TypedProperties(), true);

    // then the check runs and reports the cleaner as badly behind
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
  }

  @Test
  public void fewCommitsWithoutACleanAreStillHealthy() throws Exception {
    // given a young table with three commits and no clean yet
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    tableWithCommits(metaClient, 1, 3);

    // when the check runs with a trigger of 1 (threshold floors at 5)
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(1), false);

    // then ordinary async lag is not an alert
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
    assertEquals("none", result.getEffectiveConfigs().get("observed.last.clean.instant"));
  }

  @Test
  public void thresholdFloorsAtMinimumEvenForATriggerOfOne() throws Exception {
    // given any table
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    tableWithCommits(metaClient, 1, 1);

    // when the check runs with the lowest possible trigger
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(1), false);

    // then the threshold is the floor, not 2
    assertEquals(String.valueOf(CleanerCadenceCheck.MIN_UNHEALTHY_COMMITS),
        result.getEffectiveConfigs().get("effective.unhealthy.threshold.commits"));
  }

  @Test
  public void neverCleanedTableWithManyCommitsIsUnhealthy() throws Exception {
    // given a table that has taken many commits and never been cleaned
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    tableWithCommits(metaClient, 1, 30);

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(1), false);

    // then it is unhealthy and says nothing is scheduled
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertTrue(result.getSummary().contains("never been cleaned"), result.getSummary());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("No clean is scheduled")),
        "expected a not-scheduled finding, got: " + result.getFindings());
  }

  @Test
  public void commitsAfterACompletedCleanResetTheCount() throws Exception {
    // given a table that fell behind, cleaned, then took two more commits
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = tableWithCommits(metaClient, 1, 30);
    table.addClean("00031");
    table.addCommit("00032").addCommit("00033");

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(1), false);

    // then only the commits after that clean count
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
    assertEquals("00031", result.getEffectiveConfigs().get("observed.last.clean.instant"));
    assertEquals("2", result.getEffectiveConfigs().get("observed.commits.since.last.clean"));
  }

  @Test
  public void staleCleanWithManyCommitsAfterItIsUnhealthy() throws Exception {
    // given a table whose last clean is long behind the head of the timeline
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = tableWithCommits(metaClient, 1, 2);
    table.addClean("00003");
    for (int i = 4; i <= 40; i++) {
      table.addCommit(String.format("%05d", i));
    }

    // when the check runs with a trigger of 3 (threshold 6)
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(3), false);

    // then the cleaner is reported as behind, naming the last clean
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertTrue(result.getSummary().contains("00003"), result.getSummary());
    assertEquals("37", result.getEffectiveConfigs().get("observed.commits.since.last.clean"));
  }

  @Test
  public void inflightCleanIsCalledOutAsSchedulingWithoutCompletion() throws Exception {
    // given a lagging table with a clean requested but never finished
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = tableWithCommits(metaClient, 1, 30);
    table.addInflightClean("00031", emptyCleanerPlan());

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(1), false);

    // then the diagnosis distinguishes "not scheduled" from "scheduled but stuck"
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("has not completed")),
        "expected a stuck-clean finding, got: " + result.getFindings());
  }

  @Test
  public void optionalPolicyConfigIsEchoedOnlyWhenSupplied() throws Exception {
    // given a table and a props set that includes the retention policy
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    tableWithCommits(metaClient, 1, 1);
    TypedProperties props = propsWithTrigger(1);
    props.setProperty(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "20");

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, props, false);

    // then the supplied value is echoed and the unsupplied policy is not invented
    assertEquals("20", result.getEffectiveConfigs().get(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key()));
    assertTrue(!result.getEffectiveConfigs().containsKey(HoodieCleanConfig.CLEANER_POLICY.key()));
  }
}
