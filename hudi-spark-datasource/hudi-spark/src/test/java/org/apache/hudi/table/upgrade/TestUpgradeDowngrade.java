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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
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
import java.util.List;
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
  @MethodSource("upgradeDowngradeVersionPairs")
  public void testUpgradeOrDowngrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion) throws Exception {
    boolean isUpgrade = fromVersion.versionCode() < toVersion.versionCode();
    String operation = isUpgrade ? "upgrade" : "downgrade";
    LOG.info("Testing {} from version {} to {}", operation, fromVersion, toVersion);
    
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(fromVersion);
    assertEquals(fromVersion, originalMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at expected version");
    
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, true);
    
    int initialPendingCommits = originalMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    int initialCompletedCommits = originalMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    
    Dataset<Row> originalData = readTableData(originalMetaClient, "before " + operation);
    
    LOG.info("{} from {} to {}", isUpgrade ? "Upgrading" : "Downgrading", fromVersion, toVersion);
    new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(toVersion, null);
    
    HoodieTableMetaClient resultMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    
    assertTableVersionOnDataAndMetadataTable(resultMetaClient, toVersion);
    validateVersionSpecificProperties(resultMetaClient, fromVersion, toVersion);
    validateDataConsistency(originalData, resultMetaClient, "after " + operation);
    
    // Validate pending commits based on whether this transition performs rollback operations
    int finalPendingCommits = resultMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    if (isRollbackTransition(fromVersion, toVersion)) {
      // Handlers that call rollbackFailedWritesAndCompact() clear all pending commits
      assertEquals(0, finalPendingCommits,
          "Pending commits should be cleared to 0 after " + operation + " (rollback transition)");
    } else {
      // Other handlers may clean up some pending commits but don't necessarily clear all
      assertTrue(finalPendingCommits <= initialPendingCommits,
          "Pending commits should be cleaned up or reduced after " + operation);
    }
    
    int finalCompletedCommits = resultMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    assertTrue(finalCompletedCommits >= initialCompletedCommits,
        "Completed commits should be preserved or increased after " + operation);

    LOG.info("Successfully completed {} test for version {} -> {}", operation, fromVersion, toVersion);
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
    validateSpecificPropertiesForVersion(unchangedMetaClient, unchangedMetaClient.getTableConfig(), originalVersion);
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
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath().toString())
        .withAutoUpgradeVersion(autoUpgrade)
        .withProps(props);

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

  private static Stream<Arguments> tableVersions() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.FOUR),   // Hudi 0.11.1
        Arguments.of(HoodieTableVersion.FIVE),   // Hudi 0.12.2
        Arguments.of(HoodieTableVersion.SIX),    // Hudi 0.14
        Arguments.of(HoodieTableVersion.EIGHT),  // Hudi 1.0.2
        Arguments.of(HoodieTableVersion.NINE)    // Hudi 1.1
    );
  }

  private static Stream<Arguments> upgradeDowngradeVersionPairs() {
    return Stream.of(
        // Upgrade test cases
        Arguments.of(HoodieTableVersion.FOUR, HoodieTableVersion.FIVE),   // V4 -> V5
        Arguments.of(HoodieTableVersion.FIVE, HoodieTableVersion.SIX),    // V5 -> V6  
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.EIGHT),   // V6 -> V8
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.NINE),  // V8 -> V9
        
        // Downgrade test cases
        Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.EIGHT),  // V9 -> V8
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SIX),   // V8 -> V6
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.FIVE),    // V6 -> V5
        Arguments.of(HoodieTableVersion.FIVE, HoodieTableVersion.FOUR)    // V5 -> V4
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
        validateVersion4Properties(metaClient, tableConfig);
        break;
      case FIVE:
        validateVersion5Properties(metaClient, tableConfig);
        break;
      case SIX:
        validateVersion6Properties(metaClient);
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
  private void validateSpecificPropertiesForVersion(HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig, HoodieTableVersion version) throws IOException {
    switch (version) {
      case FOUR:
        validateVersion4Properties(metaClient, tableConfig);
        break;
      case FIVE:
        validateVersion5Properties(metaClient, tableConfig);
        break;
      case SIX:
        validateVersion6Properties(metaClient);
        break;
      case EIGHT:
        validateVersion8Properties(tableConfig);
        break;
      case NINE:
        validateVersion9Properties(metaClient, tableConfig);
        break;
      default:
        LOG.warn("No specific property validation for version {}", version);
    }
  }

  /**
   * Validate basic properties for version 4.
   */
  private void validateVersion4Properties(HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig) throws IOException {
    // TABLE_CHECKSUM should exist and be valid
    assertTrue(tableConfig.contains(HoodieTableConfig.TABLE_CHECKSUM),
        "TABLE_CHECKSUM should be set for V4");
    String actualChecksum = tableConfig.getString(HoodieTableConfig.TABLE_CHECKSUM);
    assertNotNull(actualChecksum, "TABLE_CHECKSUM should not be null");
    
    // Validate that the checksum is valid by comparing with computed checksum
    String expectedChecksum = String.valueOf(HoodieTableConfig.generateChecksum(tableConfig.getProps()));
    assertEquals(expectedChecksum, actualChecksum, 
        "TABLE_CHECKSUM should match computed checksum");

    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_1, tableConfig.getTimelineLayoutVersion().get());
    
    // TABLE_METADATA_PARTITIONS should be properly set if present
    // Note: This is optional based on whether metadata table was enabled during upgrade
    // After downgrade operations, metadata table may be deleted, so we check if it exists first
    if (tableConfig.contains(HoodieTableConfig.TABLE_METADATA_PARTITIONS)) {
      if (isMetadataTablePresent(metaClient)) {
        // Metadata table exists - enforce strict validation
        String metadataPartitions = tableConfig.getString(HoodieTableConfig.TABLE_METADATA_PARTITIONS);
        assertTrue(metadataPartitions.contains("files"), 
            "TABLE_METADATA_PARTITIONS should contain 'files' partition when metadata table exists");
      } else {
        // Metadata table doesn't exist (likely after downgrade) - validation not applicable
        LOG.info("Skipping TABLE_METADATA_PARTITIONS 'files' validation - metadata table does not exist (likely after downgrade operation)");
      }
    }
  }
  
  /**
   * Check if metadata table is present for the given table.
   */
  private boolean isMetadataTablePresent(HoodieTableMetaClient metaClient) throws IOException {
    StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
    return metaClient.getStorage().exists(metadataTablePath);
  }
  
  /**
   * Determine if a version transition performs rollback operations that clear all pending commits.
   * These handlers call rollbackFailedWritesAndCompact() which clears pending commits to 0.
   */
  private boolean isRollbackTransition(HoodieTableVersion fromVersion, HoodieTableVersion toVersion) {
    // Upgrade handlers that perform rollbacks
    if (fromVersion == HoodieTableVersion.SEVEN && toVersion == HoodieTableVersion.EIGHT) {
      return true; // SevenToEightUpgradeHandler
    }
    if (fromVersion == HoodieTableVersion.EIGHT && toVersion == HoodieTableVersion.NINE) {
      return true; // EightToNineUpgradeHandler
    }
    
    // Downgrade handlers that perform rollbacks
    if (fromVersion == HoodieTableVersion.SIX && toVersion == HoodieTableVersion.FIVE) {
      return true; // SixToFiveDowngradeHandler
    }
    if (fromVersion == HoodieTableVersion.EIGHT && toVersion == HoodieTableVersion.SEVEN) {
      return true; // EightToSevenDowngradeHandler  
    }
    if (fromVersion == HoodieTableVersion.NINE && toVersion == HoodieTableVersion.EIGHT) {
      return true; // NineToEightDowngradeHandler
    }
    
    return false; // All other transitions don't perform rollbacks
  }

  /**
   * Validate properties for version 5 (default partition path migration).
   */
  private void validateVersion5Properties(HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig) throws IOException {

    validateVersion4Properties(metaClient, tableConfig);
    // Version 5 upgrade validates that no deprecated default partition paths exist
    // The upgrade handler checks for DEPRECATED_DEFAULT_PARTITION_PATH ("default") 
    // and requires migration to DEFAULT_PARTITION_PATH ("__HIVE_DEFAULT_PARTITION__")
    
    // If table is partitioned, validate partition path migration
    if (tableConfig.isTablePartitioned()) {
      LOG.info("Validating V5 partition path migration for partitioned table");
      
      // Check hive-style partitioning configuration
      boolean hiveStylePartitioningEnable = Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable());
      LOG.info("Hive-style partitioning enabled: {}", hiveStylePartitioningEnable);
      
      // Validate partition field configuration exists
      assertTrue(tableConfig.getPartitionFields().isPresent(),
          "Partition fields should be present for partitioned table in V5");
    } else {
      LOG.info("Non-partitioned table - skipping partition path validation for V5");
    }
  }

  /**
   * Validate properties for version 6 (auxiliary folder cleanup).
   * Based on FiveToSixUpgradeHandler - validates that compaction REQUESTED files were properly cleaned up.
   */
  private void validateVersion6Properties(HoodieTableMetaClient metaClient) throws IOException {
    // Version 6 upgrade deletes compaction requested files from .aux folder (HUDI-6040)
    // Validate that no REQUESTED compaction files remain in auxiliary folder
    validateVersion5Properties(metaClient, metaClient.getTableConfig());

    StoragePath auxPath = new StoragePath(metaClient.getMetaAuxiliaryPath());
    
    if (!metaClient.getStorage().exists(auxPath)) {
      // Auxiliary folder doesn't exist - this is valid, nothing to clean up
      LOG.info("V6 validation passed: Auxiliary folder does not exist");
      return;
    }
    
    // Auxiliary folder exists - validate that REQUESTED compaction files were cleaned up
    LOG.info("V6 validation: Checking auxiliary folder cleanup at: {}", auxPath);
    
    // Get pending compaction timeline with REQUESTED state (same as upgrade handler)
    HoodieTimeline compactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    
    InstantFileNameGenerator factory = metaClient.getInstantFileNameGenerator();
    
    // Validate that none of the REQUESTED compaction files exist in auxiliary folder
    compactionTimeline.getInstantsAsStream().forEach(instant -> {
      StoragePath compactionFile = new StoragePath(metaClient.getMetaAuxiliaryPath(), factory.getFileName(instant));
      try {
        if (metaClient.getStorage().exists(compactionFile)) {
          throw new AssertionError("V6 validation failed: REQUESTED compaction file should have been cleaned up but still exists: " + compactionFile);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to check existence of compaction file: " + compactionFile, e);
      }
    });
    
    LOG.info("V6 validation passed: {} REQUESTED compaction instants verified to be cleaned up from auxiliary folder", 
        compactionTimeline.countInstants());
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
    validateVersion8Properties(tableConfig);

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
      
      // Force execution to ensure data is read immediately (not lazily)
      // This prevents the "before upgrade" data from actually being read after upgrade operations
      List<Row> rows = tableData.collectAsList();
      long rowCount = rows.size();
      assertTrue(rowCount >= 0, "Row count should be non-negative " + stage);
      
      // Convert collected rows back to Dataset for use in validation
      Dataset<Row> materializedData = sqlContext().createDataFrame(rows, tableData.schema());
      
      LOG.info("Successfully read and materialized table data {} ({} rows)", stage, rowCount);
      return materializedData;
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

  private void performDataValidationOnTable(HoodieTableMetaClient metaClient, String stage) {
    LOG.info("Performing data validation on table {}", stage);
    
    try {
      Dataset<Row> tableData = readTableData(metaClient, stage);
      LOG.info("Data validation passed {} (table accessible, {} rows)", stage, tableData.count());
    } catch (Exception e) {
      throw new RuntimeException("Data validation failed " + stage, e);
    }
  }
}
