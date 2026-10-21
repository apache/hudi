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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the paths of {@link RecordIndexSizingCheck} that need no metadata table on disk: the check
 * declining to run, and the count comparison on synthetic footprints, where the healthy/undersized
 * boundary and the floor rule can be placed exactly. Verdicts on a real record index are covered by
 * {@link TestRecordIndexSizingCheckWithSpark}, which has to write one.
 */
public class TestRecordIndexSizingCheck {

  @TempDir
  Path tempDir;

  private String basePath;
  private final RecordIndexSizingCheck check = new RecordIndexSizingCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  private HoodieTableMetaClient tableWithRecordIndexListed() throws Exception {
    // Only the table configuration says the partition exists; there is no metadata table behind it.
    // Enough for the check's fail-fast paths, which must not touch the metadata table.
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(metaClient).addCommit("001");
    metaClient.getTableConfig().setMetadataPartitionState(
        metaClient, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), true);
    return HoodieTableMetaClient.reload(metaClient);
  }

  private TypedProperties globalSizingProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "2");
    props.setProperty(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "2");
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), "1073741824");
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key(), "2.0");
    return props;
  }

  @Test
  public void tableWithoutRecordIndexIsSkippedAndSaysSo() throws Exception {
    // given a table whose metadata table has no record index partition
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(metaClient).addCommit("001");

    // when the check runs with every sizing property supplied
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, globalSizingProps(), false));

    // then it does not apply, and the reason names the partition it looked for
    assertFalse(check.appliesTo(new HealthCheckContext(metaClient, globalSizingProps(), false)));
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()),
        "expected the skip reason to name the record index partition, got: " + result.getSummary());
  }

  @Test
  public void missingSizingConfigSkipsNamingTheFirstMissingProperty() throws Exception {
    // given a table with a record index and no writer configuration supplied
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, new TypedProperties(), false));

    // then it declines to judge and names the global minimum file-group count first
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(
        HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()), result.getSummary());
  }

  @Test
  public void eachSizingPropertyIsRequiredInTurn() throws Exception {
    // given a record index and a properties set missing only the growth factor
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();
    TypedProperties props = globalSizingProps();
    props.remove(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key());

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, props, false));

    // then that is the property it asks for
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key()),
        result.getSummary());
  }

  @Test
  public void partitionedRecordIndexRequiresThePartitionedSizingKeys() throws Exception {
    // given a writer that enables the partitioned record index but only supplies the global sizing keys
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();
    TypedProperties props = globalSizingProps();
    props.setProperty(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, props, false));

    // then it asks for the partitioned key, since that is what the writer sizes with
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(
        HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()), result.getSummary());
  }

  @Test
  public void maxFileGroupSizeBelowOneRecordIsRejectedRatherThanDividedByZero() throws Exception {
    // given a maximum file-group size smaller than a single index record
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();
    TypedProperties props = globalSizingProps();
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), "10");

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, props, false));

    // then it skips with the offending value echoed, before ever opening the metadata table
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key()),
        result.getSummary());
    assertEquals("10", result.getEffectiveConfigs()
        .get(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key()));
  }

  // ---- count comparison on synthetic footprints -------------------------------------------------

  private static final long ONE_GIB = 1024L * 1024 * 1024;
  /** Records that fit one 1 GiB file group at the writer's 48-byte average record. */
  private static final long RECORDS_PER_FILE_GROUP = ONE_GIB / 48;

  private static RecordIndexSizingCheck.SizingConfig sizing(int minFileGroups, int maxFileGroups) {
    return new RecordIndexSizingCheck.SizingConfig(minFileGroups, maxFileGroups, 2.0f, ONE_GIB);
  }

  /**
   * Base-file bytes that make the writer's estimate land on {@code idealFileGroups}: the estimate
   * multiplies the record count by the 2.0 growth factor and divides by the records per file group,
   * so half that many records' worth of bytes per ideal file group. Aimed at the middle of the band
   * (ideal + 0.5) rather than its lower edge, because the writer does that multiplication in
   * {@code float} and a count above 2^24 records rounds by a few either way.
   */
  private static long bytesForIdeal(int idealFileGroups) {
    return (long) ((idealFileGroups + 0.5) * RECORDS_PER_FILE_GROUP / 2) * 48;
  }

  private static RecordIndexSizingCheck.RecordIndexFootprint footprint(int fileGroups, long baseFileBytes) {
    return RecordIndexSizingCheck.RecordIndexFootprint.synthetic(fileGroups, fileGroups, baseFileBytes, baseFileBytes);
  }

  private static RecordIndexSizingCheck.CountComparison compare(int fileGroups, long baseFileBytes,
                                                                RecordIndexSizingCheck.SizingConfig sizing) {
    return RecordIndexSizingCheck.CountComparison.of(Option.empty(), footprint(fileGroups, baseFileBytes), sizing);
  }

  @Test
  public void fileGroupsAtExactlyHalfTheIdealAreHealthy() {
    // given an index whose bytes call for four file groups and which has two
    RecordIndexSizingCheck.CountComparison comparison = compare(2, bytesForIdeal(4), sizing(1, 1000));

    // then the ideal is size-derived, the ratio sits exactly on the 0.5 boundary, and it is not undersized
    assertEquals(4, comparison.idealFileGroupCount);
    assertFalse(comparison.idealIsFloor);
    assertEquals(2, comparison.minHealthyFileGroupCount());
    assertEquals(0.5, comparison.ratio(), 1e-9);
    assertFalse(comparison.isUndersized());
  }

  @Test
  public void fileGroupsJustUnderHalfTheIdealAreUndersized() {
    // given the same sizing with one file group fewer, and a five-ideal index with two
    RecordIndexSizingCheck.CountComparison oneOfFour = compare(1, bytesForIdeal(4), sizing(1, 1000));
    RecordIndexSizingCheck.CountComparison twoOfFive = compare(2, bytesForIdeal(5), sizing(1, 1000));

    // then both fall below ceil(0.5 * ideal) and are undersized
    assertEquals(4, oneOfFour.idealFileGroupCount);
    assertTrue(oneOfFour.isUndersized());
    assertEquals(5, twoOfFive.idealFileGroupCount);
    assertEquals(3, twoOfFive.minHealthyFileGroupCount());
    assertTrue(twoOfFive.isUndersized());
  }

  @Test
  public void floorBoundIdealIsNeverUndersized() {
    // given a fixed count (min == max) and a minimum far above what a tiny index needs
    RecordIndexSizingCheck.CountComparison fixedCount = compare(1, bytesForIdeal(4), sizing(4, 4));
    RecordIndexSizingCheck.CountComparison tinyIndex = compare(1, 1024 * 1024, sizing(10, 1000));

    // then both ideals come from the floor and neither comparison reports a shortfall, however low the ratio
    assertTrue(fixedCount.idealIsFloor);
    assertEquals(0.25, fixedCount.ratio(), 1e-9);
    assertFalse(fixedCount.isUndersized());
    assertTrue(tinyIndex.idealIsFloor);
    assertEquals(10, tinyIndex.idealFileGroupCount);
    assertEquals(0.1, tinyIndex.ratio(), 1e-9);
    assertFalse(tinyIndex.isUndersized());
  }

  @Test
  public void worstDataPartitionIsTheOneWithARealShortfallNotTheLowestRatio() {
    // given a partitioned index after the minimum was raised from 1 to 10: an old one-file-group
    // partition is floor-bound at ratio 0.1, and a large partition has five file groups against a
    // size-derived ideal of twenty, ratio 0.25
    RecordIndexSizingCheck.SizingConfig sizing = sizing(10, 1000);
    Map<String, RecordIndexSizingCheck.RecordIndexFootprint> byDataPartition = new TreeMap<>();
    byDataPartition.put("2024/01/01", footprint(1, 1024 * 1024));
    byDataPartition.put("2024/01/02", footprint(5, bytesForIdeal(20)));
    RecordIndexSizingCheck.RecordIndexFootprint index = footprint(6, bytesForIdeal(20) + 1024 * 1024)
        .withLayout(true, byDataPartition);
    HealthCheckResult.Builder result = HealthCheckResult.builder(RecordIndexSizingCheck.NAME);

    // when the worst data partition is picked
    RecordIndexSizingCheck.CountComparison worst =
        RecordIndexSizingCheck.compareFileGroupCount(index, true, sizing, result).get();

    // then the real shortfall wins the pick over the lower ratio, and the verdict is undersized
    assertEquals("2024/01/02", worst.dataPartition.get());
    assertEquals(20, worst.idealFileGroupCount);
    assertFalse(worst.idealIsFloor);
    assertTrue(worst.isUndersized());
    Map<String, String> configs = result.build().getEffectiveConfigs();
    assertEquals("2024/01/02", configs.get("observed.worst.data.partition"));
    assertEquals("1", configs.get("observed.undersized.data.partition.count"));
    assertEquals("size estimate", configs.get("effective.ideal.source"));
  }

  @Test
  public void amongHealthyDataPartitionsTheLowestSizeDerivedRatioIsReported() {
    // given no shortfall anywhere: one floor-bound partition at ratio 0.1 and two size-derived
    // partitions at ratios 1.0 and 0.75
    RecordIndexSizingCheck.SizingConfig sizing = sizing(10, 1000);
    Map<String, RecordIndexSizingCheck.RecordIndexFootprint> byDataPartition = new TreeMap<>();
    byDataPartition.put("a", footprint(1, 1024 * 1024));
    byDataPartition.put("b", footprint(20, bytesForIdeal(20)));
    byDataPartition.put("c", footprint(15, bytesForIdeal(20)));
    RecordIndexSizingCheck.RecordIndexFootprint index = footprint(36, 2 * bytesForIdeal(20)).withLayout(true, byDataPartition);
    HealthCheckResult.Builder result = HealthCheckResult.builder(RecordIndexSizingCheck.NAME);

    // when the worst data partition is picked
    RecordIndexSizingCheck.CountComparison worst =
        RecordIndexSizingCheck.compareFileGroupCount(index, true, sizing, result).get();

    // then the floor-bound partition does not win on ratio; the size-derived 0.75 does, and nothing is undersized
    assertEquals("c", worst.dataPartition.get());
    assertFalse(worst.isUndersized());
    assertEquals("0", result.build().getEffectiveConfigs().get("observed.undersized.data.partition.count"));
  }

  @Test
  public void recordEstimateUsesBaseFileBytesAndFallsBackToLogsBeforeTheFirstCompaction() {
    // given a compacted index whose log files triple its footprint, and an uncompacted one with logs only
    RecordIndexSizingCheck.RecordIndexFootprint compacted =
        RecordIndexSizingCheck.RecordIndexFootprint.synthetic(2, 2, ONE_GIB, 3 * ONE_GIB);
    RecordIndexSizingCheck.RecordIndexFootprint logsOnly =
        RecordIndexSizingCheck.RecordIndexFootprint.synthetic(2, 0, 0, ONE_GIB);

    // then the estimate ignores the log bytes when base files exist, and uses them when nothing else is there
    assertEquals(ONE_GIB, compacted.estimateBytes());
    assertEquals("base files", compacted.estimateBasis());
    assertEquals(ONE_GIB / 48, RecordIndexSizingCheck.CountComparison.of(Option.empty(), compacted, sizing(1, 1000)).estimatedRecordCount);
    assertEquals(ONE_GIB, logsOnly.estimateBytes());
    assertEquals("log files (the index has no base file yet)", logsOnly.estimateBasis());
  }
}
