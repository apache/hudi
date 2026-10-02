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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ArchivalCadenceCheck}.
 */
public class TestArchivalCadenceCheck {

  @TempDir
  Path tempDir;

  private String basePath;
  private final ArchivalCadenceCheck check = new ArchivalCadenceCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  private TypedProperties propsWithRetention(int maxCommitsToKeep) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), String.valueOf(maxCommitsToKeep));
    return props;
  }

  private HealthCheckResult runCheck(HoodieTableMetaClient metaClient, TypedProperties props) throws Exception {
    return check.check(new HealthCheckContext(HoodieTableMetaClient.reload(metaClient), props, false));
  }

  private HoodieTableMetaClient tableWithCommits(int numCommits) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable table = HoodieTestTable.of(metaClient);
    for (int i = 1; i <= numCommits; i++) {
      table.addCommit(String.format("%05d", i));
    }
    return metaClient;
  }

  @Test
  public void missingRetentionConfigSkipsRatherThanGuessing() throws Exception {
    // given a table with a long active timeline
    HoodieTableMetaClient metaClient = tableWithCommits(50);

    // when the check runs without the archival retention configured
    HealthCheckResult result = runCheck(metaClient, new TypedProperties());

    // then it declines to judge instead of calling a possibly fine table unhealthy
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key()));
  }

  @Test
  public void timelineWithinRetentionIsHealthy() throws Exception {
    // given a timeline comfortably inside the configured retention
    HoodieTableMetaClient metaClient = tableWithCommits(5);

    // when the check runs with a retention of 30
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(30));

    // then archival is keeping up
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
  }

  @Test
  public void timelineSlightlyOverRetentionIsStillHealthy() throws Exception {
    // given 10 write instants against a retention of 10 -- archival runs periodically, not per commit
    HoodieTableMetaClient metaClient = tableWithCommits(10);

    // when the check runs (threshold is 11 after the 1.1 slack factor)
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(10));

    // then the normal gap between archival runs is not an alert
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
  }

  @Test
  public void timelineWellPastRetentionIsUnhealthy() throws Exception {
    // given far more write instants than the configured retention allows
    HoodieTableMetaClient metaClient = tableWithCommits(60);

    // when the check runs with a retention of 10
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(10));

    // then archival is reported as not keeping up
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("not keeping up with ingestion")),
        "expected a retention finding, got: " + result.getFindings());
  }

  @Test
  public void unhealthyResultPointsAtTheSavepointCheckFirst() throws Exception {
    // given a table whose archival has stalled
    HoodieTableMetaClient metaClient = tableWithCommits(60);

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(10));

    // then it directs the operator to the most common cause rather than guessing at one itself
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains(SavepointArchivalBlockCheck.NAME)),
        "expected a pointer to the savepoint check, got: " + result.getFindings());
  }

  @Test
  public void effectiveConfigIsEchoedSoAVerdictCanBeAudited() throws Exception {
    // given a short timeline
    HoodieTableMetaClient metaClient = tableWithCommits(3);

    // when the check runs with a known retention
    HealthCheckResult result = runCheck(metaClient, propsWithRetention(20));

    // then both the configured value and the derived threshold are reported
    assertEquals("20", result.getEffectiveConfigs().get(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key()));
    assertEquals("22", result.getEffectiveConfigs().get("effective.unhealthy.threshold.write.instants"));
    assertEquals("3", result.getEffectiveConfigs().get("observed.write.instants"));
  }
}
