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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link RecordIndexSizingCheck} verdicts against a record index actually written through
 * the Spark write client. The global index is initialized with a fixed two file groups and the
 * partitioned one with a fixed one file group per data partition, so the check's verdict is driven
 * entirely by the sizing configuration handed to it.
 */
public class TestRecordIndexSizingCheckWithSpark extends SparkClientFunctionalTestHarness {

  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(0L);
  private static final int GLOBAL_FILE_GROUPS = 2;
  private static final int FILE_GROUPS_PER_DATA_PARTITION = 1;
  private static final int DATA_PARTITIONS = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS.length;
  private static final String ONE_GIB = String.valueOf(1024L * 1024 * 1024);
  /**
   * A maximum file-group size small enough that the fixture index estimates several file groups
   * rather than clamping to the configured minimum. With ONE_GIB the estimate is zero for any
   * fixture-sized index, so the ideal comes from the floor and says nothing about the table --
   * which is the bug these tests exist to catch, not a configuration to assert against.
   */
  private static final String SIXTEEN_KIB = String.valueOf(16 * 1024);
  /** Smaller still, so the estimate clears the 0.5 healthy ratio rather than sitting on it. */
  private static final String FOUR_KIB = String.valueOf(4 * 1024);
  /**
   * Small enough that one data partition's base-file bytes alone call for several file groups. The
   * estimate is taken from base files, not base plus logs, so the per-partition fixture -- a few
   * dozen records each -- needs a limit this low to show a real shortfall rather than the floor.
   */
  private static final String ONE_KIB = String.valueOf(1024);

  private final RecordIndexSizingCheck check = new RecordIndexSizingCheck();
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void initTable() throws Exception {
    metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
  }

  @AfterAll
  public static void closeDataGenerator() {
    DATA_GENERATOR.close();
  }

  private void writeGlobalRecordIndex() throws Exception {
    HoodieWriteConfig writeConfig = writeConfigBuilder()
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableGlobalRecordLevelIndex(true)
            .withRecordIndexFileGroupCount(GLOBAL_FILE_GROUPS, GLOBAL_FILE_GROUPS)
            .build())
        .build();
    insertBatches(writeConfig, 1);
  }

  private void writePartitionedRecordIndex() throws Exception {
    // A partitioned record index is sized from the data partitions that exist, so its
    // initialization is deferred past the first commit and two batches are written.
    HoodieWriteConfig writeConfig = writeConfigBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.RECORD_LEVEL_INDEX).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordLevelIndex(true)
            .withPartitionedRecordIndexFileGroupCount(FILE_GROUPS_PER_DATA_PARTITION, FILE_GROUPS_PER_DATA_PARTITION)
            .withDeferRliInitializationForFreshTable(true)
            .build())
        .build();
    insertBatches(writeConfig, 2);
  }

  private HoodieWriteConfig.Builder writeConfigBuilder() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .forTable("record_index_sizing");
  }

  private void insertBatches(HoodieWriteConfig writeConfig, int batches) throws Exception {
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context(), writeConfig)) {
      for (int i = 0; i < batches; i++) {
        String instant = writeClient.startCommit();
        List<HoodieRecord> records = DATA_GENERATOR.generateInserts(instant, 100);
        List<WriteStatus> statuses = writeClient.upsert(jsc().parallelize(records, 1), instant).collect();
        assertNoWriteErrors(statuses);
        writeClient.commit(instant, jsc().parallelize(statuses));
      }
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX),
        "fixture should have initialized a record index");
  }

  private static TypedProperties globalSizingProps(int minFileGroups, int maxFileGroups, String maxFileGroupSizeBytes) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(),
        String.valueOf(minFileGroups));
    props.setProperty(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(),
        String.valueOf(maxFileGroups));
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), maxFileGroupSizeBytes);
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key(), "2.0");
    return props;
  }

  private static TypedProperties partitionedSizingProps(int minFileGroups, int maxFileGroups, String maxFileGroupSizeBytes) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    props.setProperty(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(),
        String.valueOf(minFileGroups));
    props.setProperty(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(),
        String.valueOf(maxFileGroups));
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), maxFileGroupSizeBytes);
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key(), "2.0");
    return props;
  }

  private HealthCheckResult runCheck(TypedProperties props, boolean applyAllDefaults) throws Exception {
    return check.check(new HealthCheckContext(metaClient, props, applyAllDefaults));
  }

  private static void assertCountComparisonAbsent(Map<String, String> configs) {
    assertFalse(configs.containsKey("effective.ideal.file.group.count"), configs.toString());
    assertFalse(configs.containsKey("effective.min.healthy.file.group.count"), configs.toString());
    assertFalse(configs.containsKey("observed.estimated.record.count"), configs.toString());
    assertTrue(configs.get("effective.count.comparison").startsWith("skipped:"), configs.toString());
  }

  @Test
  public void recordIndexMatchingItsConfiguredSizingIsHealthy() throws Exception {
    // given a global index sized so the estimate derives from the index's own bytes rather than
    // clamping to the floor, and lands at or below what the index already has
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(1, 64, SIXTEEN_KIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the ideal is size-derived and the index is not called undersized
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    assertEquals("2", result.getEffectiveConfigs().get("observed.file.group.count"));
    assertEquals("whole index (global record index)", result.getEffectiveConfigs().get("effective.count.comparison"));
    int ideal = Integer.parseInt(result.getEffectiveConfigs().get("effective.ideal.file.group.count"));
    assertTrue(ideal > 1, "fixture should exercise the estimate, not the floor; ideal was " + ideal);
  }

  @Test
  public void fewerFileGroupsThanTheWriterNowSizesForIsUnhealthyAndSaysToRebootstrap() throws Exception {
    // given a maximum file-group size small enough that the index's own bytes call for more file
    // groups than it was built with -- a real shortfall, not one manufactured by the floor
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(1, 64, FOUR_KIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then it reports the index undersized, with the counts and the only remedy spelled out
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus(), result.getSummary());
    int ideal = Integer.parseInt(result.getEffectiveConfigs().get("effective.ideal.file.group.count"));
    assertTrue(ideal > 1, "the ideal must come from the estimate, not the floor; was " + ideal);
    assertEquals(String.valueOf((int) Math.ceil(0.5 * ideal)),
        result.getEffectiveConfigs().get("effective.min.healthy.file.group.count"));
    assertEquals("2", result.getEffectiveConfigs().get("observed.file.group.count"));
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("Record index was initialized with 2 file group(s)")),
        "expected a finding stating the observed count, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("metadata delete-record-index")),
        "expected the hudi-cli drop command, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("--mode dropindex --index-types RECORD_INDEX")),
        "expected the HoodieIndexer drop command, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("--mode scheduleAndExecute --index-types RECORD_INDEX")),
        "expected the HoodieIndexer rebuild command, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("cannot be changed in place")),
        "expected the finding to say the count is fixed at initialization, got: " + result.getFindings());
  }

  @Test
  public void fixedFileGroupCountAboveWhatExistsIsReportedAsTheFloorNotAsAShortfall() throws Exception {
    // given a writer that pins the count at four (min == max) while the index has two; the ideal is
    // the configured floor, not a size estimate, so the 0.5 ratio means nothing about index health
    // (the exact 0.5 boundary itself is covered on synthetic footprints in TestRecordIndexSizingCheck)
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(4, 4, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then it is healthy, says the ideal came from the floor, and tells the operator why that is not a shortfall
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    assertEquals("4", result.getEffectiveConfigs().get("effective.ideal.file.group.count"));
    assertEquals("configured minimum (floor)", result.getEffectiveConfigs().get("effective.ideal.source"));
    assertTrue(result.getSummary().contains("fewer than the configured minimum of 4"), result.getSummary());
  }

  @Test
  public void fileGroupPastTheConfiguredMaximumSizeIsUnhealthy() throws Exception {
    // given a maximum file-group size far below what even a hundred-record index occupies
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(GLOBAL_FILE_GROUPS, GLOBAL_FILE_GROUPS, "512");

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the largest file group is called out, and the remedy is the same re-bootstrap
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus(), result.getSummary());
    long largest = Long.parseLong(result.getEffectiveConfigs().get("observed.largest.file.group.bytes"));
    assertTrue(largest > 768, "fixture should have produced a file group past 1.5 x 512 bytes, was " + largest);
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("past the configured maximum of 512 bytes")),
        "expected a finding about the oversized file group, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("metadata delete-record-index")),
        "expected the re-bootstrap remedy, got: " + result.getFindings());
  }

  @Test
  public void applyAllDefaultsEvaluatesWithoutSuppliedConfig() throws Exception {
    // given no writer configuration at all, but defaults explicitly allowed
    writeGlobalRecordIndex();

    // when the check runs
    HealthCheckResult result = runCheck(new TypedProperties(), true);

    // then it runs rather than skipping, and reports the stock sizing it reasoned with
    assertNotEquals(HealthStatus.SKIPPED, result.getStatus(), result.getSummary());
    assertEquals(String.valueOf(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.defaultValue()),
        result.getEffectiveConfigs().get(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()));
    // A fixture-sized index against the stock 1 GiB maximum file-group size estimates zero file
    // groups, so the ideal is the configured minimum and carries nothing about the table. That is
    // not a shortfall to act on: it must not read as UNHEALTHY and must not recommend a rebuild.
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("comes from the configured minimum")),
        "expected the floor to be named as the reason, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().noneMatch(f -> f.contains("metadata delete-record-index")),
        "a floor-bound ideal must not recommend a re-bootstrap, got: " + result.getFindings());
  }

  @Test
  public void legacySizingKeysAreAcceptedAsAlternatives() throws Exception {
    // given a writer that still spells the sizing keys the pre-rename way
    writeGlobalRecordIndex();
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.metadata.record.index.min.filegroup.count", "2");
    props.setProperty("hoodie.metadata.record.index.max.filegroup.count", "2");
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), ONE_GIB);
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key(), "2.0");

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the old keys satisfy the requirement and their values are what the check reasons with
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    assertEquals("2", result.getEffectiveConfigs()
        .get(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()));
  }

  @Test
  public void effectiveConfigIsEchoedSoAVerdictCanBeAudited() throws Exception {
    // given a known sizing configuration
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(GLOBAL_FILE_GROUPS, GLOBAL_FILE_GROUPS, ONE_GIB);

    // when the check runs
    Map<String, String> configs = runCheck(props, false).getEffectiveConfigs();

    // then every supplied value, derived threshold and measurement is reported alongside the verdict
    assertEquals("global", configs.get("observed.index.layout"));
    assertEquals("2", configs.get(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()));
    assertEquals("2", configs.get(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key()));
    assertEquals(ONE_GIB, configs.get(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key()));
    assertEquals("2.0", configs.get(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key()));
    assertEquals("48", configs.get("effective.average.record.size.bytes"));
    assertEquals("2", configs.get("effective.ideal.file.group.count"));
    // min == max pins the count, so this fixture's ideal is the floor; the estimate branch is exercised elsewhere
    assertEquals("configured minimum (floor)", configs.get("effective.ideal.source"));
    assertEquals("1", configs.get("effective.min.healthy.file.group.count"));
    assertEquals(String.valueOf((long) (1.5 * 1024L * 1024 * 1024)), configs.get("effective.max.healthy.file.group.bytes"));
    assertTrue(configs.containsKey("observed.base.file.size.bytes"), configs.toString());
    assertTrue(configs.get("effective.estimate.basis").startsWith("base files")
        || configs.get("effective.estimate.basis").startsWith("log files"), configs.toString());
    assertEquals("2", configs.get("observed.file.group.count"));
    assertTrue(Long.parseLong(configs.get("observed.total.size.bytes")) > 0);
    assertTrue(Long.parseLong(configs.get("observed.largest.file.group.bytes")) > 0);
    assertTrue(Long.parseLong(configs.get("observed.estimated.record.count")) > 0);
    assertTrue(Integer.parseInt(configs.get("observed.base.file.count"))
        + Integer.parseInt(configs.get("observed.log.file.count")) > 0);
    assertTrue(!configs.get("observed.largest.file.group.id").isEmpty());
  }

  @Test
  public void partitionedConfigOnAGlobalIndexAsksForTheGlobalSizingKeys() throws Exception {
    // given a global index, but a writer configuration that says the index is partitioned, with a
    // maximum file-group size the index is already past
    writeGlobalRecordIndex();
    TypedProperties props = partitionedSizingProps(10, 10, "512");

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the layout comes from storage, so the check asks for the sizing keys the index on disk
    // actually needs rather than silently judging it against the partitioned ones it was handed
    assertEquals(HealthStatus.SKIPPED, result.getStatus(), result.getSummary());
    assertTrue(result.getSummary().contains(
            HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()),
        "expected the global sizing key to be named, got: " + result.getSummary());
    // and it judges nothing until it has them -- naming the missing key beats guessing with the
    // wrong one, per the checker's fail-fast contract
    assertTrue(result.getFindings().isEmpty(),
        "a skipped check must not carry findings, got: " + result.getFindings());
  }

  @Test
  public void partitionedConfigOnAGlobalIndexNeverComparesPerPartitionAgainstTheTotal() throws Exception {
    // given the same mismatch but a generous maximum file-group size
    writeGlobalRecordIndex();
    TypedProperties props = partitionedSizingProps(10, 10, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then no false shortfall: a per-partition count is never compared against the whole-index
    // total, because the global sizing keys govern an index that is global on storage
    assertEquals(HealthStatus.SKIPPED, result.getStatus(), result.getSummary());
    assertTrue(result.getSummary().contains(
            HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()),
        "expected the global sizing key to be named, got: " + result.getSummary());
  }

  @Test
  public void partitionedIndexIsComparedPerDataPartition() throws Exception {
    // given a partitioned index with one file group in each of the generator's three data partitions,
    // and a writer that still sizes it at one file group per data partition
    writePartitionedRecordIndex();
    TypedProperties props = partitionedSizingProps(FILE_GROUPS_PER_DATA_PARTITION, FILE_GROUPS_PER_DATA_PARTITION, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the total is reported, the comparison is per data partition, and the index is healthy
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    Map<String, String> configs = result.getEffectiveConfigs();
    assertEquals("partitioned", configs.get("observed.index.layout"));
    assertEquals(String.valueOf(DATA_PARTITIONS), configs.get("observed.data.partition.count"));
    assertEquals(String.valueOf(DATA_PARTITIONS * FILE_GROUPS_PER_DATA_PARTITION), configs.get("observed.file.group.count"));
    assertEquals("1", configs.get("observed.worst.data.partition.file.group.count"));
    assertEquals("1", configs.get("effective.ideal.file.group.count"));
    // min == max per data partition, so every partition is floor-bound here; the pick among
    // size-derived partitions is covered on synthetic footprints in TestRecordIndexSizingCheck
    assertEquals("configured minimum (floor)", configs.get("effective.ideal.source"));
    assertEquals("0", configs.get("observed.undersized.data.partition.count"));
    assertTrue(configs.get("effective.count.comparison").startsWith("per data partition"), configs.toString());
    assertTrue(configs.containsKey("observed.worst.data.partition"), configs.toString());
  }

  @Test
  public void partitionedIndexUndersizedInADataPartitionIsUnhealthyAndNamesThePartition() throws Exception {
    // given a partitioned index with one file group per data partition, and a maximum file-group
    // size small enough that each partition's own base-file bytes call for more than the one it has
    writePartitionedRecordIndex();
    TypedProperties props = partitionedSizingProps(1, 64, ONE_KIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the shortfall is judged per data partition -- three total file groups are not weighed
    // against four -- and the worst partition is named in the finding with the remedy
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus(), result.getSummary());
    Map<String, String> configs = result.getEffectiveConfigs();
    int partitionedIdeal = Integer.parseInt(configs.get("effective.ideal.file.group.count"));
    assertTrue(partitionedIdeal > 1, "the ideal must come from the estimate, not the floor; was " + partitionedIdeal);
    assertEquals(String.valueOf((int) Math.ceil(0.5 * partitionedIdeal)),
        configs.get("effective.min.healthy.file.group.count"));
    assertEquals("1", configs.get("observed.worst.data.partition.file.group.count"));
    assertEquals("size estimate", configs.get("effective.ideal.source"));
    assertTrue(Integer.parseInt(configs.get("observed.undersized.data.partition.count")) >= 1, configs.toString());
    String worstPartition = configs.get("observed.worst.data.partition");
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains(
        String.format("Data partition '%s' of the record index was initialized with 1 file group(s)", worstPartition))),
        "expected the finding to name the worst data partition, got: " + result.getFindings());
    // HoodieIndexer cannot rebuild a partitioned index (apache/hudi#20343), so the remedy must say
    // so rather than name a command that would silently produce a global index instead
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("issues/20343")),
        "expected the partitioned-rebuild caveat, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().noneMatch(f -> f.contains("metadata delete-record-index")),
        "the global re-bootstrap command must not be offered for a partitioned index, got: " + result.getFindings());
  }
}
