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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for upgrade/downgrade operations using pre-created fixture tables
 * from different Hudi releases.
 */
public class TestUpgradeDowngrade extends SparkClientFunctionalTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeDowngrade.class);
  private static final String FIXTURES_BASE_PATH = "/upgrade-downgrade-fixtures/mor-tables/";
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private HoodieTableMetaClient metaClient;

  @ParameterizedTest
  @MethodSource("upgradeVersions")
  public void testUpgradeOnly(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing upgrade for version {}", originalVersion);
    
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    assertEquals(originalVersion, originalMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at expected version");
    
    Option<HoodieTableVersion> targetVersionOpt = getNextVersion(originalVersion);
    if (!targetVersionOpt.isPresent()) {
      LOG.info("Skipping upgrade test for version {} (no higher version available)", originalVersion);
      return;
    }
    HoodieTableVersion targetVersion = targetVersionOpt.get();

    HoodieWriteConfig config = createWriteConfig(originalMetaClient, true);
    
    int initialPendingCommits = originalMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    int initialCompletedCommits = originalMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    
    // Read original data before upgrade for validation
    Dataset<Row> originalData = readTableData(originalMetaClient, "before upgrade");
    
    LOG.info("Upgrading from {} to {}", originalVersion, targetVersion);
    new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    HoodieTableMetaClient upgradedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    
    assertTableVersionOnDataAndMetadataTable(upgradedMetaClient, targetVersion);
    validateVersionSpecificProperties(upgradedMetaClient, originalVersion, targetVersion);
    validateDataConsistency(originalData, upgradedMetaClient, "after upgrade");
    
    int finalPendingCommits = upgradedMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    assertTrue(finalPendingCommits <= initialPendingCommits,
        "Pending commits should be cleaned up or reduced after upgrade");
    
    int finalCompletedCommits = upgradedMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    assertTrue(finalCompletedCommits >= initialCompletedCommits,
        "Completed commits should be preserved or increased after upgrade");

    LOG.info("Successfully completed upgrade test for version {} -> {}", originalVersion, targetVersion);
  }

  @ParameterizedTest
  @MethodSource("downgradeVersions")
  public void testDowngradeOnly(HoodieTableVersion targetVersion) throws Exception {
    LOG.info("Testing downgrade for version {}", targetVersion);
    
    HoodieTableMetaClient targetMetaClient = loadFixtureTable(targetVersion);
    assertEquals(targetVersion, targetMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at expected version");
    
    Option<HoodieTableVersion> sourceVersionOpt = getPreviousVersion(targetVersion);
    if (!sourceVersionOpt.isPresent()) {
      LOG.info("Skipping downgrade test for version {} (no lower version available)", targetVersion);
      return;
    }
    HoodieTableVersion sourceVersion = sourceVersionOpt.get();
    
    HoodieWriteConfig config = createWriteConfig(targetMetaClient, true);
    
    // Count initial timeline state
    int initialPendingCommits = targetMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    int initialCompletedCommits = targetMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    
    // Read original data before downgrade for validation
    Dataset<Row> originalData = readTableData(targetMetaClient, "before downgrade");
    
    LOG.info("Downgrading from {} to {}", targetVersion, sourceVersion);
    new UpgradeDowngrade(targetMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(sourceVersion, null);
    
    // Create fresh meta client to read updated table configuration after downgrade
    HoodieTableMetaClient downgradedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(targetMetaClient.getBasePath())
        .build();
    
    assertTableVersionOnDataAndMetadataTable(downgradedMetaClient, sourceVersion);
    validateVersionSpecificProperties(downgradedMetaClient, targetVersion, sourceVersion);
    validateDataConsistency(originalData, downgradedMetaClient, "after downgrade");
    
    // Verify rollback behavior - pending commits should be cleaned up or reduced
    int finalPendingCommits = downgradedMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    assertTrue(finalPendingCommits <= initialPendingCommits,
        "Pending commits should be cleaned up or reduced after downgrade");
    
    // Verify we still have completed commits
    int finalCompletedCommits = downgradedMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    assertTrue(finalCompletedCommits >= initialCompletedCommits,
        "Completed commits should be preserved or increased after downgrade");

    LOG.info("Successfully completed downgrade test for version {} -> {}", targetVersion, sourceVersion);
  }

  @ParameterizedTest
  @MethodSource("tableVersions")
  public void testAutoUpgradeDisabled(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing auto-upgrade disabled for version {}", originalVersion);
    
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    
    Option<HoodieTableVersion> targetVersionOpt = getNextVersion(originalVersion);
    if (!targetVersionOpt.isPresent()) {
      LOG.info("Skipping auto-upgrade test for version {} (no higher version available)", originalVersion);
      return;
    }
    HoodieTableVersion targetVersion = targetVersionOpt.get();
    
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, false);
    
    // Attempt upgrade with auto-upgrade disabled
    new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to validate that version remained unchanged 
    HoodieTableMetaClient unchangedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    assertEquals(originalVersion, unchangedMetaClient.getTableConfig().getTableVersion(),
        "Table version should remain unchanged when auto-upgrade is disabled");
    validateSpecificPropertiesForVersion(unchangedMetaClient.getTableConfig(), originalVersion);
    validateVersionSpecificProperties(unchangedMetaClient, originalVersion, originalVersion);
    performDataValidationOnTable(unchangedMetaClient, "after auto-upgrade disabled test");
    
    LOG.info("Auto-upgrade disabled test passed for version {}", originalVersion);
  }

  /**
   * Load a fixture table from resources and copy it to a temporary location for testing.
   */
  private HoodieTableMetaClient loadFixtureTable(HoodieTableVersion version) throws IOException {
    String fixtureName = getFixtureName(version);
    String resourcePath = FIXTURES_BASE_PATH + fixtureName;
    
    LOG.info("Loading fixture from resource path: {}", resourcePath);
    HoodieTestUtils.extractZipToDirectory(resourcePath, tempDir, getClass());
    
    String tableName = fixtureName.replace(".zip", "");
    String tablePath = tempDir.resolve(tableName).toString();
    
    metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(tablePath)
        .build();
    
    LOG.info("Loaded fixture table {} at version {}", fixtureName, metaClient.getTableConfig().getTableVersion());
    return metaClient;
  }

  /**
   * Create write config for test operations (upgrade/downgrade/validation).
   */
  private HoodieWriteConfig createWriteConfig(HoodieTableMetaClient metaClient, boolean autoUpgrade) {
    Properties props = new Properties();
    props.putAll(metaClient.getTableConfig().getProps());
    
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath().toString())
        .withAutoUpgradeVersion(autoUpgrade)
        .withProps(props);
    
    // Add timeline layout version only if available (needed for upgrade operations)
    if (metaClient.getTableConfig().getTimelineLayoutVersion().isPresent()) {
      builder.withTimelineLayoutVersion(metaClient.getTableConfig().getTimelineLayoutVersion().get().getVersion());
    }
    
    // For validation operations, keep timeline server disabled for simplicity
    if (!autoUpgrade) {
      builder.withEmbeddedTimelineServerEnabled(false);
    }
    
    return builder.build();
  }

  /**
   * Get the next version up from the current version.
   */
  private Option<HoodieTableVersion> getNextVersion(HoodieTableVersion current) {
    switch (current) {
      case FOUR:
        return Option.of(HoodieTableVersion.FIVE);
      case FIVE:
        return Option.of(HoodieTableVersion.SIX);
      case SIX:
        // even though there is a table version 7, this is not an official release and serves as a bridge
        // so the next version should be 8
        return Option.of(HoodieTableVersion.EIGHT);
      case EIGHT:
        return Option.of(HoodieTableVersion.NINE);
      case NINE:
        return Option.empty(); // No higher version available
      default:
        return Option.empty();
    }
  }

  /**
   * Get the previous version down from the current version.
   */
  private Option<HoodieTableVersion> getPreviousVersion(HoodieTableVersion current) {
    switch (current) {
      case NINE:
        return Option.of(HoodieTableVersion.EIGHT);
      case EIGHT:
        return Option.of(HoodieTableVersion.SIX);
      case SIX:
        return Option.of(HoodieTableVersion.FIVE);
      case FIVE:
        return Option.of(HoodieTableVersion.FOUR);
      case FOUR:
        return Option.empty(); // for now we are focusing on V4-V9
      default:
        return Option.empty();
    }
  }

  /**
   * Get fixture zip file name for a given table version.
   */
  private String getFixtureName(HoodieTableVersion version) {
    switch (version) {
      case FOUR:
        return "hudi-v4-table.zip";
      case FIVE:
        return "hudi-v5-table.zip";
      case SIX:
        return "hudi-v6-table.zip";
      case EIGHT:
        return "hudi-v8-table.zip";
      case NINE:
        return "hudi-v9-table.zip";
      default:
        throw new IllegalArgumentException("Unsupported fixture version: " + version);
    }
  }

  /**
   * Provide test parameters for all table versions.
   */
  private static Stream<Arguments> tableVersions() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.FOUR),   // Hudi 0.11.1
        Arguments.of(HoodieTableVersion.FIVE),   // Hudi 0.12.2
        Arguments.of(HoodieTableVersion.SIX),    // Hudi 0.14
        Arguments.of(HoodieTableVersion.EIGHT),  // Hudi 1.0.2
        Arguments.of(HoodieTableVersion.NINE)    // Hudi 1.1
    );
  }

  /**
   * Provide test parameters for upgrade versions (V4-V8).
   */
  private static Stream<Arguments> upgradeVersions() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.FOUR),   // V4 -> V5
        Arguments.of(HoodieTableVersion.FIVE),   // V5 -> V6  
        Arguments.of(HoodieTableVersion.SIX),    // V6 -> V8
        Arguments.of(HoodieTableVersion.EIGHT)   // V8 -> V9
    );
  }

  /**
   * Provide test parameters for downgrade versions (V9 down to V5).
   */
  private static Stream<Arguments> downgradeVersions() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.NINE),   // V9 -> V8
        Arguments.of(HoodieTableVersion.EIGHT),  // V8 -> V6
        Arguments.of(HoodieTableVersion.SIX),    // V6 -> V5
        Arguments.of(HoodieTableVersion.FIVE)    // V5 -> V4
    );
  }

  /**
   * Assert table version on both data table and metadata table (if exists).
   */
  private void assertTableVersionOnDataAndMetadataTable(
      HoodieTableMetaClient metaClient, HoodieTableVersion expectedVersion) throws IOException {
    assertTableVersion(metaClient, expectedVersion);
    if (expectedVersion.greaterThanOrEquals(HoodieTableVersion.FOUR)) {
      StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
      if (metaClient.getStorage().exists(metadataTablePath)) {
        LOG.info("Verifying metadata table version at: {}", metadataTablePath);
        HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
            .setConf(metaClient.getStorageConf().newInstance()).setBasePath(metadataTablePath).build();
        assertTableVersion(mdtMetaClient, expectedVersion);
      } else {
        LOG.info("Metadata table does not exist at: {}", metadataTablePath);
      }
    }
  }

  private void assertTableVersion(
      HoodieTableMetaClient metaClient, HoodieTableVersion expectedVersion) {
    assertEquals(expectedVersion,
        metaClient.getTableConfig().getTableVersion());
  }

  /**
   * Validate version-specific properties after upgrade/downgrade operations.
   */
  private void validateVersionSpecificProperties(
      HoodieTableMetaClient metaClient, HoodieTableVersion fromVersion, HoodieTableVersion toVersion) throws IOException {
    LOG.info("Validating version-specific properties: {} -> {}", fromVersion, toVersion);
    
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    
    // Validate properties for target version
    switch (toVersion) {
      case FOUR:
        validateVersion4Properties(tableConfig);
        break;
      case FIVE:
        validateVersion5Properties(tableConfig);
        break;
      case SIX:
        validateVersion6Properties(tableConfig);
        break;
      case EIGHT:
        validateVersion8Properties(tableConfig);
        break;
      case NINE:
        validateVersion9Properties(metaClient, tableConfig);
        break;
      default:
        LOG.warn("No specific property validation for version {}", toVersion);
    }
    
  }

  /**
   * Validate specific properties for a given table version.
   */
  private void validateSpecificPropertiesForVersion(HoodieTableConfig tableConfig, HoodieTableVersion version) throws IOException {
    switch (version) {
      case FOUR:
        validateVersion4Properties(tableConfig);
        break;
      case FIVE:
        validateVersion5Properties(tableConfig);
        break;
      case SIX:
        validateVersion6Properties(tableConfig);
        break;
      case EIGHT:
        validateVersion8Properties(tableConfig);
        break;
      case NINE:
        // Note: validateVersion9Properties requires metaClient, so we'll skip this for now
        // or we could enhance this method to accept metaClient as well
        LOG.warn("Skipping version 9 specific validation as it requires metaClient");
        break;
      default:
        LOG.warn("No specific property validation for version {}", version);
    }
  }

  /**
   * Validate basic properties for version 4.
   */
  private void validateVersion4Properties(HoodieTableConfig tableConfig) {
    // TABLE_CHECKSUM should exist and be valid
    assertTrue(tableConfig.contains(HoodieTableConfig.TABLE_CHECKSUM),
        "TABLE_CHECKSUM should be set for V4");
    String actualChecksum = tableConfig.getString(HoodieTableConfig.TABLE_CHECKSUM);
    assertNotNull(actualChecksum, "TABLE_CHECKSUM should not be null");
    
    // Validate that the checksum is valid by comparing with computed checksum
    String expectedChecksum = String.valueOf(HoodieTableConfig.generateChecksum(tableConfig.getProps()));
    assertEquals(expectedChecksum, actualChecksum, 
        "TABLE_CHECKSUM should match computed checksum");
    
    // TABLE_METADATA_PARTITIONS should be properly set if present
    // Note: This is optional based on whether metadata table was enabled during upgrade
    if (tableConfig.contains(HoodieTableConfig.TABLE_METADATA_PARTITIONS)) {
      String metadataPartitions = tableConfig.getString(HoodieTableConfig.TABLE_METADATA_PARTITIONS);
      assertTrue(metadataPartitions.contains("files"), 
          "TABLE_METADATA_PARTITIONS should contain 'files' partition when set");
    }
  }

  /**
   * Validate properties for version 5 (default partition path changes).
   */
  private void validateVersion5Properties(HoodieTableConfig tableConfig) {
    // Version 5 mainly focuses on default partition path validation, to file JIRA
  }

  /**
   * Validate properties for version 6 (auxiliary folder cleanup).
   */
  private void validateVersion6Properties(HoodieTableConfig tableConfig) {
    // Version 6 focuses on auxiliary folder cleanup, to file JIRA
  }

  /**
   * Validate properties for version 8 (major upgrade with record merge mode, new configuration properties).
   */
  private void validateVersion8Properties(HoodieTableConfig tableConfig) {
    // Timeline layout should be upgraded to V2 for V8+
    Option<TimelineLayoutVersion> layoutVersion = tableConfig.getTimelineLayoutVersion();
    assertTrue(layoutVersion.isPresent(), "Timeline layout version should be present for V8+");
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_2, layoutVersion.get(),
        "Timeline layout should be V2 for V8+");

    // Timeline path should be set
    assertTrue(tableConfig.contains(HoodieTableConfig.TIMELINE_PATH),
        "Timeline path should be set for V8");
    assertEquals(HoodieTableConfig.TIMELINE_PATH.defaultValue(),
        tableConfig.getString(HoodieTableConfig.TIMELINE_PATH),
        "Timeline path should have default value");
    
    // Record merge mode should be set
    assertTrue(tableConfig.contains(HoodieTableConfig.RECORD_MERGE_MODE),
        "Record merge mode should be set for V8");
    RecordMergeMode mergeMode = tableConfig.getRecordMergeMode();
    assertNotNull(mergeMode, "Merge mode should not be null");
    
    // Record merge strategy ID should be set
    assertTrue(tableConfig.contains(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID),
        "Record merge strategy ID should be set for V8");
    
    // Payload class should be set
    assertTrue(tableConfig.contains(HoodieTableConfig.PAYLOAD_CLASS_NAME),
        "Payload class should be set for V8");
    
    // Initial version should be set
    assertTrue(tableConfig.contains(HoodieTableConfig.INITIAL_VERSION),
        "Initial version should be set for V8");
    
    // Key generator type should be set if key generator class is present
    if (tableConfig.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME)) {
      assertTrue(tableConfig.contains(HoodieTableConfig.KEY_GENERATOR_TYPE),
          "Key generator type should be set when key generator class is present");
    }
  }

  /**
   * Validate properties for version 9 (index version population).
   */
  private void validateVersion9Properties(HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig) {
    // Check if index metadata exists and has proper version information
    Option<HoodieIndexMetadata> indexMetadata = metaClient.getIndexMetadata();
    if (indexMetadata.isPresent()) {
      indexMetadata.get().getIndexDefinitions().forEach((indexName, indexDef) -> {
        assertNotNull(indexDef.getVersion(), 
            "Index " + indexName + " should have version information in V9");
      });
    }
  }

  /**
   * Read table data for validation purposes.
   */
  private Dataset<Row> readTableData(HoodieTableMetaClient metaClient, String stage) {
    LOG.info("Reading table data {}", stage);
    
    try {
      String basePath = metaClient.getBasePath().toString();
      Dataset<Row> tableData = sqlContext().read()
          .format("hudi")
          .load(basePath);

      assertNotNull(tableData, "Table read should not return null " + stage);
      long rowCount = tableData.count();
      assertTrue(rowCount >= 0, "Row count should be non-negative " + stage);
      
      LOG.info("Successfully read table data {} ({} rows)", stage, rowCount);
      return tableData;
    } catch (Exception e) {
      LOG.error("Failed to read table data {} from: {} (version: {})", 
          stage, metaClient.getBasePath(), metaClient.getTableConfig().getTableVersion(), e);
      throw new RuntimeException("Failed to read table data " + stage, e);
    }
  }
  
  /**
   * Validate data consistency between original data and data after upgrade/downgrade.
   * This ensures that upgrade/downgrade operations preserve data integrity.
   */
  private void validateDataConsistency(Dataset<Row> originalData, HoodieTableMetaClient metaClient, String stage) {
    LOG.info("Validating data consistency {}", stage);
    
    try {
      Dataset<Row> currentData = readTableData(metaClient, stage);
      
      // Exclude Hudi metadata columns
      Set<String> hoodieMetadataColumns = new HashSet<>(Arrays.asList(
          "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", 
          "_hoodie_partition_path", "_hoodie_file_name"));
      
      Set<String> columnsToValidate = Arrays.stream(originalData.columns())
          .filter(col -> !hoodieMetadataColumns.contains(col))
          .collect(Collectors.toSet());
      
      if (columnsToValidate.isEmpty()) {
        LOG.info("Skipping data consistency validation {} (no business columns to validate)", stage);
        return;
      }
      
      LOG.info("Validating data columns: {}", columnsToValidate);
      boolean dataConsistent = areDataframesEqual(originalData, currentData, columnsToValidate);
      assertTrue(dataConsistent, " data should be consistent between original and " + stage + " states");
      
      LOG.info("Data consistency validation passed {}", stage);
    } catch (Exception e) {
      throw new RuntimeException("Data consistency validation failed " + stage, e);
    }
  }
  
  /**
   * Simple data validation for testAutoUpgradeDisabled.
   */
  private void performDataValidationOnTable(HoodieTableMetaClient metaClient, String stage) {
    LOG.info("Performing simple data validation on table {}", stage);
    
    try {
      Dataset<Row> tableData = readTableData(metaClient, stage);
      LOG.info("Simple data validation passed {} (table accessible, {} rows)", stage, tableData.count());
    } catch (Exception e) {
      throw new RuntimeException("Simple data validation failed " + stage, e);
    }
  }

}
