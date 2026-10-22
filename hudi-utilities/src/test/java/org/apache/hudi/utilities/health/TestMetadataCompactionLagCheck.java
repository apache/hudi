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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link MetadataCompactionLagCheck}.
 *
 * <p>The check reads only the metadata table's timeline, so the fixture initializes an empty
 * metadata table under {@code .hoodie/metadata}, registers its FILES partition on the data table,
 * and drives its timeline with {@link HoodieTestTable}. Completion times are always explicit: delta
 * commits are counted by completion time relative to the last compaction, so a fixture that leaves
 * them implicit can silently count nothing.
 */
public class TestMetadataCompactionLagCheck {

  private static final String COMPACTION_INSTANT = "20260101100000000";

  @TempDir
  Path tempDir;

  private String basePath;
  private final MetadataCompactionLagCheck check = new MetadataCompactionLagCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  private TypedProperties propsWithTrigger(int maxDeltaCommits) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");
    props.setProperty(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key(), String.valueOf(maxDeltaCommits));
    return props;
  }

  private HealthCheckResult runCheck(HoodieTableMetaClient metaClient, TypedProperties props) throws Exception {
    return check.check(new HealthCheckContext(HoodieTableMetaClient.reload(metaClient), props, false));
  }

  private HoodieTableMetaClient tableWithoutMetadataTable() throws Exception {
    return HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
  }

  /** A data table with an initialized, empty metadata table registered in its hoodie.properties. */
  private HoodieTableMetaClient tableWithMetadataTable() throws Exception {
    HoodieTableMetaClient dataMetaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    Properties registration = new Properties();
    registration.setProperty(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), MetadataPartitionType.FILES.getPartitionPath());
    HoodieTableConfig.update(dataMetaClient.getStorage(), dataMetaClient.getMetaPath(), registration);
    HoodieTestUtils.init(HoodieTableMetadata.getMetadataTableBasePath(basePath), HoodieTableType.MERGE_ON_READ);
    return dataMetaClient;
  }

  private HoodieTestTable metadataTableOf(String dataBasePath) throws Exception {
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
        .setConf(HoodieTestUtils.getDefaultStorageConf())
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(dataBasePath))
        .build();
    return HoodieTestTable.of(metadataMetaClient);
  }

  /** The instant {@code minutes} minutes after {@link #COMPACTION_INSTANT}, formatted as an instant time. */
  private static String minutesAfterCompaction(int minutes) throws Exception {
    Date compactedAt = TimelineUtils.parseDateFromInstantTime(COMPACTION_INSTANT);
    return TimelineUtils.formatDate(new Date(compactedAt.getTime() + TimeUnit.MINUTES.toMillis(minutes)));
  }

  private void addCompactionThenDeltaCommits(HoodieTestTable metadataTable, int deltaCommits, int minutesApart) throws Exception {
    metadataTable.addCompaction(COMPACTION_INSTANT, Option.of(COMPACTION_INSTANT), new HoodieCommitMetadata());
    addDeltaCommits(metadataTable, deltaCommits, minutesApart);
  }

  private void addDeltaCommits(HoodieTestTable metadataTable, int deltaCommits, int minutesApart) throws Exception {
    for (int i = 1; i <= deltaCommits; i++) {
      String instant = minutesAfterCompaction(i * minutesApart);
      metadataTable.addDeltaCommit(instant, Option.of(instant), new HoodieCommitMetadata());
    }
  }

  @Test
  public void tableWithoutMetadataTableIsSkipped() throws Exception {
    // given a table that has never had a metadata table
    HoodieTableMetaClient metaClient = tableWithoutMetadataTable();

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(10));

    // then there is no metadata table to compact, and the check says so
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains("no metadata table"), result.getSummary());
  }

  @Test
  public void metadataTableDisabledForWriterIsSkipped() throws Exception {
    // given a table with a metadata table
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    TypedProperties props = propsWithTrigger(10);
    props.setProperty(HoodieMetadataConfig.ENABLE.key(), "false");

    // when the writer is configured with the metadata table off
    HealthCheckResult result = runCheck(metaClient, props);

    // then the check does not apply
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.ENABLE.key()), result.getSummary());
  }

  @Test
  public void missingTriggerPropertySkipsNamingIt() throws Exception {
    // given a lagging metadata table
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    addCompactionThenDeltaCommits(metadataTableOf(basePath), 30, 1);
    TypedProperties onlyEnable = new TypedProperties();
    onlyEnable.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");

    // when the check runs without the compaction trigger configured
    HealthCheckResult result = runCheck(metaClient, onlyEnable);

    // then it declines to judge rather than guessing a trigger the table may not be written with
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key()), result.getSummary());
  }

  @Test
  public void metadataTableWithNoDeltaCommitsIsHealthy() throws Exception {
    // given a freshly initialized metadata table with an empty timeline
    HoodieTableMetaClient metaClient = tableWithMetadataTable();

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(10));

    // then nothing has accumulated
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
  }

  @Test
  public void deltaCommitsWithinSlackAreHealthy() throws Exception {
    // given 8 delta commits since compaction against a trigger of 5 -- past the trigger, inside the 2x slack
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    addCompactionThenDeltaCommits(metadataTableOf(basePath), 8, 1);

    // when the check runs (threshold is 10)
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(5));

    // then the gap between trigger and execution is not an alert
    assertEquals(HealthStatus.HEALTHY, result.getStatus());
    assertEquals("8", result.getEffectiveConfigs().get("observed.mdt.delta.commits.since.compaction"));
    assertEquals("10", result.getEffectiveConfigs().get("effective.unhealthy.threshold.delta.commits"));
  }

  @Test
  public void deltaCommitsPastSlackAreUnhealthyWithDurationEchoed() throws Exception {
    // given a trigger of 2 and 6 delta commits landing every 10 minutes after the last compaction
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    addCompactionThenDeltaCommits(metadataTableOf(basePath), 6, 10);

    // when the check runs (threshold is 4)
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(2));

    // then compaction is reported as lagging, with how long it has been lagging
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertEquals("6", result.getEffectiveConfigs().get("observed.mdt.delta.commits.since.compaction"));
    assertEquals(COMPACTION_INSTANT, result.getEffectiveConfigs().get("observed.last.mdt.compaction.instant"));
    assertEquals("60", result.getEffectiveConfigs().get("observed.lag.duration.minutes"));
    assertTrue(result.getSummary().contains("60 minute"), result.getSummary());
  }

  @Test
  public void pendingDataTableInstantIsNamedAsTheFirstThingToCheck() throws Exception {
    // given a lagging metadata table and a data-table delta commit stuck inflight
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    addCompactionThenDeltaCommits(metadataTableOf(basePath), 6, 10);
    String pendingInstant = minutesAfterCompaction(30);
    HoodieTestTable.of(metaClient).addInflightDeltaCommit(pendingInstant);

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(2));

    // then the pending instant is named, since it is what pins the compaction instant
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertEquals(pendingInstant, result.getEffectiveConfigs().get("observed.data.table.earliest.pending.instant"));
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains(pendingInstant) && f.contains("pending")),
        "expected the pending data-table instant to be named, got: " + result.getFindings());
  }

  @Test
  public void unhealthyResultNeverRecommendsRebuildingTheMetadataTable() throws Exception {
    // given a lagging metadata table
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    addCompactionThenDeltaCommits(metadataTableOf(basePath), 6, 10);

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(2));

    // then the operator is told compaction lag recovers on its own
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("Do not delete or rebuild")),
        "expected a warning against rebuilding, got: " + result.getFindings());
  }

  @Test
  public void metadataTableThatHasNeverCompactedCountsFromItsFirstDeltaCommit() throws Exception {
    // given 6 delta commits and no compaction at all
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    addDeltaCommits(metadataTableOf(basePath), 6, 10);

    // when the check runs with a trigger of 2
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(2));

    // then every delta commit counts, and the report does not pretend a compaction happened
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertEquals("6", result.getEffectiveConfigs().get("observed.mdt.delta.commits.since.compaction"));
    assertEquals("none", result.getEffectiveConfigs().get("observed.last.mdt.compaction.instant"));
    assertFalse(result.getSummary().contains("compaction at"), result.getSummary());
  }

  @Test
  public void bootstrapPlaceholderInstantDoesNotDistortTheLagDuration() throws Exception {
    // given a never-compacted metadata table whose first delta commit is the zero bootstrap placeholder
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    HoodieTestTable metadataTable = metadataTableOf(basePath);
    String bootstrapInstant = HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP + "000";
    metadataTable.addDeltaCommit(bootstrapInstant, Option.of(bootstrapInstant), new HoodieCommitMetadata());
    addDeltaCommits(metadataTable, 6, 10);

    // when the check runs with a trigger of 2
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(2));

    // then the duration is measured from the first real delta commit, not from the placeholder
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus());
    assertEquals("7", result.getEffectiveConfigs().get("observed.mdt.delta.commits.since.compaction"));
    assertEquals("50", result.getEffectiveConfigs().get("observed.lag.duration.minutes"));
    assertFalse(result.getSummary().contains(bootstrapInstant), result.getSummary());
  }

  @Test
  public void effectiveConfigIsEchoedSoAVerdictCanBeAudited() throws Exception {
    // given a healthy metadata table
    HoodieTableMetaClient metaClient = tableWithMetadataTable();
    addCompactionThenDeltaCommits(metadataTableOf(basePath), 3, 1);

    // when the check runs with a known trigger
    HealthCheckResult result = runCheck(metaClient, propsWithTrigger(10));

    // then the supplied value, the derived threshold, and the measurements are all reported
    assertEquals("true", result.getEffectiveConfigs().get(HoodieMetadataConfig.ENABLE.key()));
    assertEquals("10", result.getEffectiveConfigs().get(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key()));
    assertEquals("20", result.getEffectiveConfigs().get("effective.unhealthy.threshold.delta.commits"));
    assertEquals("3", result.getEffectiveConfigs().get("observed.mdt.delta.commits.since.compaction"));
    assertEquals(COMPACTION_INSTANT, result.getEffectiveConfigs().get("observed.last.mdt.compaction.instant"));
    assertEquals("3", result.getEffectiveConfigs().get("observed.lag.duration.minutes"));
  }
}
