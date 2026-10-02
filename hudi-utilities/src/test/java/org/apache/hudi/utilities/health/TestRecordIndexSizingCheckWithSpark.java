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
    // given a global index whose writer still sizes it at the two file groups it was initialized with
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(GLOBAL_FILE_GROUPS, GLOBAL_FILE_GROUPS, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then observed and ideal agree and the index is healthy
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    assertEquals("2", result.getEffectiveConfigs().get("observed.file.group.count"));
    assertEquals("2", result.getEffectiveConfigs().get("effective.ideal.file.group.count"));
    assertEquals("whole index (global record index)", result.getEffectiveConfigs().get("effective.count.comparison"));
    assertTrue(result.getFindings().isEmpty(), "healthy verdict should carry no findings: " + result.getFindings());
  }

  @Test
  public void fewerFileGroupsThanTheWriterNowSizesForIsUnhealthyAndSaysToRebootstrap() throws Exception {
    // given the writer's sizing has since been raised to a fixed ten file groups, five times what exists
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(10, 10, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then it reports the index undersized, with the counts and the only remedy spelled out
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus(), result.getSummary());
    assertEquals("10", result.getEffectiveConfigs().get("effective.ideal.file.group.count"));
    assertEquals("5", result.getEffectiveConfigs().get("effective.min.healthy.file.group.count"));
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
  public void fileGroupsAtHalfTheIdealAreStillHealthy() throws Exception {
    // given a writer sizing that calls for four file groups, exactly twice what exists
    writeGlobalRecordIndex();
    TypedProperties props = globalSizingProps(4, 4, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the ratio of 0.5 sits on the boundary and is not an alert
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    assertEquals("4", result.getEffectiveConfigs().get("effective.ideal.file.group.count"));
    assertEquals("2", result.getEffectiveConfigs().get("effective.min.healthy.file.group.count"));
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

    // then it runs, and against the stock minimum of ten file groups the two-file-group index is undersized
    assertNotEquals(HealthStatus.SKIPPED, result.getStatus(), result.getSummary());
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus(), result.getSummary());
    assertEquals(String.valueOf(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.defaultValue()),
        result.getEffectiveConfigs().get(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()));
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
    assertEquals("false", configs.get(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key()));
    assertEquals("2", configs.get(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()));
    assertEquals("2", configs.get(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key()));
    assertEquals(ONE_GIB, configs.get(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key()));
    assertEquals("2.0", configs.get(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key()));
    assertEquals("48", configs.get("effective.average.record.size.bytes"));
    assertEquals("2", configs.get("effective.ideal.file.group.count"));
    assertEquals("1", configs.get("effective.min.healthy.file.group.count"));
    assertEquals(String.valueOf((long) (1.5 * 1024L * 1024 * 1024)), configs.get("effective.max.healthy.file.group.bytes"));
    assertEquals("2", configs.get("observed.file.group.count"));
    assertTrue(Long.parseLong(configs.get("observed.total.size.bytes")) > 0);
    assertTrue(Long.parseLong(configs.get("observed.largest.file.group.bytes")) > 0);
    assertTrue(Long.parseLong(configs.get("observed.estimated.record.count")) > 0);
    assertTrue(Integer.parseInt(configs.get("observed.base.file.count"))
        + Integer.parseInt(configs.get("observed.log.file.count")) > 0);
    assertTrue(!configs.get("observed.largest.file.group.id").isEmpty());
  }

  @Test
  public void partitionedConfigOnAGlobalIndexSkipsTheCountComparisonAndStillChecksFileGroupSize() throws Exception {
    // given a global index, but a writer configuration that says the index is partitioned and sizes it
    // at ten file groups per data partition, with a maximum file-group size the index is already past
    writeGlobalRecordIndex();
    TypedProperties props = partitionedSizingProps(10, 10, "512");

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the count comparison is skipped and says why, the count keys are absent, and the size rule still fires
    assertCountComparisonAbsent(result.getEffectiveConfigs());
    assertTrue(result.getEffectiveConfigs().get("effective.count.comparison").contains("index on storage is global"),
        result.getEffectiveConfigs().toString());
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus(), result.getSummary());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("past the configured maximum of 512 bytes")),
        "expected the oversized finding, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().noneMatch(f -> f.contains("was initialized with")),
        "expected no undersized finding, got: " + result.getFindings());
  }

  @Test
  public void partitionedConfigOnAGlobalIndexDoesNotReportAFalseShortfall() throws Exception {
    // given the same mismatch but a generous maximum file-group size
    writeGlobalRecordIndex();
    TypedProperties props = partitionedSizingProps(10, 10, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then ten-per-partition is never compared against the two-file-group total, and the index reads healthy
    assertCountComparisonAbsent(result.getEffectiveConfigs());
    assertEquals(HealthStatus.HEALTHY, result.getStatus(), result.getSummary());
    assertTrue(result.getSummary().contains("not compared"), result.getSummary());
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
    assertEquals("true", configs.get(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key()));
    assertEquals(String.valueOf(DATA_PARTITIONS), configs.get("observed.data.partition.count"));
    assertEquals(String.valueOf(DATA_PARTITIONS * FILE_GROUPS_PER_DATA_PARTITION), configs.get("observed.file.group.count"));
    assertEquals("1", configs.get("observed.worst.data.partition.file.group.count"));
    assertEquals("1", configs.get("effective.ideal.file.group.count"));
    assertTrue(configs.get("effective.count.comparison").startsWith("per data partition"), configs.toString());
    assertTrue(configs.containsKey("observed.worst.data.partition"), configs.toString());
  }

  @Test
  public void partitionedIndexUndersizedInADataPartitionIsUnhealthyAndNamesThePartition() throws Exception {
    // given a partitioned index with one file group per data partition, and a writer that now sizes
    // every data partition at a fixed four
    writePartitionedRecordIndex();
    TypedProperties props = partitionedSizingProps(4, 4, ONE_GIB);

    // when the check runs
    HealthCheckResult result = runCheck(props, false);

    // then the shortfall is judged per data partition -- three total file groups are not weighed
    // against four -- and the worst partition is named in the finding with the remedy
    assertEquals(HealthStatus.UNHEALTHY, result.getStatus(), result.getSummary());
    Map<String, String> configs = result.getEffectiveConfigs();
    assertEquals("4", configs.get("effective.ideal.file.group.count"));
    assertEquals("2", configs.get("effective.min.healthy.file.group.count"));
    assertEquals("1", configs.get("observed.worst.data.partition.file.group.count"));
    String worstPartition = configs.get("observed.worst.data.partition");
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains(
        String.format("Data partition '%s' of the record index was initialized with 1 file group(s)", worstPartition))),
        "expected the finding to name the worst data partition, got: " + result.getFindings());
    assertTrue(result.getFindings().stream().anyMatch(f -> f.contains("metadata delete-record-index")),
        "expected the re-bootstrap remedy, got: " + result.getFindings());
  }
}
