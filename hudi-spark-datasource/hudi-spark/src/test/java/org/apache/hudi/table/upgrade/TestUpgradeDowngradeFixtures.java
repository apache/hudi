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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for upgrade/downgrade operations using pre-created fixture tables
 * from different Hudi releases. Tests round-trip operations: upgrade one version up,
 * then downgrade back to original version.
 */
public class TestUpgradeDowngradeFixtures extends HoodieSparkClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeDowngradeFixtures.class);
  private static final String FIXTURES_BASE_PATH = "/upgrade-downgrade-fixtures/mor-tables/";
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    initSparkContexts();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
  }

  @ParameterizedTest
  @MethodSource("fixtureVersions")
  public void testRoundTripUpgradeDowngrade(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing round-trip upgrade/downgrade for version {}", originalVersion);
    
    // Load fixture table
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    assertEquals(originalVersion, originalMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at expected version");
    
    // Calculate target version (next version up)
    HoodieTableVersion targetVersion = getNextVersion(originalVersion);
    if (targetVersion == null) {
      LOG.info("Skipping upgrade test for version {} (no higher version available)", originalVersion);
      return;
    }
    
    // Create write config for upgrade operations
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, true);
    
    // Step 1: Upgrade to next version
    LOG.info("Step 1: Upgrading from {} to {}", originalVersion, targetVersion);
    new UpgradeDowngrade(originalMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to read updated table configuration after upgrade
    HoodieTableMetaClient upgradedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    assertTableVersionOnDataAndMetadataTable(upgradedMetaClient, targetVersion);
    validateVersionSpecificProperties(upgradedMetaClient, originalVersion, targetVersion);
    performDataValidationOnTable(upgradedMetaClient, "after upgrade");

    // Step 2: Downgrade back to original version
    LOG.info("Step 2: Downgrading from {} back to {}", targetVersion, originalVersion);
    new UpgradeDowngrade(upgradedMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(originalVersion, null);
    
    // Create fresh meta client to read updated table configuration after downgrade
    HoodieTableMetaClient finalMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(upgradedMetaClient.getBasePath())
        .build();
    assertTableVersionOnDataAndMetadataTable(finalMetaClient, originalVersion);
    validateVersionSpecificProperties(finalMetaClient, targetVersion, originalVersion);
    performDataValidationOnTable(finalMetaClient, "after round-trip");

    LOG.info("Successfully completed round-trip test for version {}", originalVersion);
  }

  @ParameterizedTest
  @MethodSource("fixtureVersions")
  public void testAutoUpgradeDisabled(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing auto-upgrade disabled for version {}", originalVersion);
    
    // Load fixture table
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    
    // Calculate target version (next version up)
    HoodieTableVersion targetVersion = getNextVersion(originalVersion);
    if (targetVersion == null) {
      LOG.info("Skipping auto-upgrade test for version {} (no higher version available)", originalVersion);
      return;
    }
    
    // Create write config with auto-upgrade disabled
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, false);
    
    // Attempt upgrade with auto-upgrade disabled
    new UpgradeDowngrade(originalMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to validate that version remained unchanged 
    HoodieTableMetaClient unchangedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    assertEquals(originalVersion, unchangedMetaClient.getTableConfig().getTableVersion(),
        "Table version should remain unchanged when auto-upgrade is disabled");
    performDataValidationOnTable(unchangedMetaClient, "after auto-upgrade disabled test");
    
    LOG.info("Auto-upgrade disabled test passed for version {}", originalVersion);
  }

  @ParameterizedTest
  @MethodSource("fixtureVersions") 
  public void testRollbackAndCompactionBehavior(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing rollback and compaction behavior for version {}", originalVersion);
    
    // Load fixture table
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    
    // Calculate target version
    HoodieTableVersion targetVersion = getNextVersion(originalVersion);
    if (targetVersion == null) {
      LOG.info("Skipping rollback/compaction test for version {} (no higher version available)", originalVersion);
      return;
    }
    
    // Create write config for upgrade operations
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, true);
    
    // Count initial timeline state
    int initialPendingCommits = originalMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    int initialCompletedCommits = originalMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    
    // Perform upgrade
    new UpgradeDowngrade(originalMetaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to validate timeline state after upgrade
    HoodieTableMetaClient upgradedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    
    // Perform data validation after upgrade
    performDataValidationOnTable(upgradedMetaClient, "after upgrade in rollback/compaction test");
    
    // Verify rollback behavior - pending commits should be cleaned up or reduced
    int finalPendingCommits = upgradedMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    assertTrue(finalPendingCommits <= initialPendingCommits,
        "Pending commits should be cleaned up or reduced after upgrade");
    
    // Verify we still have completed commits
    int finalCompletedCommits = upgradedMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
    assertTrue(finalCompletedCommits >= initialCompletedCommits,
        "Completed commits should be preserved or increased after upgrade");
    
    LOG.info("Rollback and compaction behavior validated for version {}", originalVersion);
  }

  /**
   * Load a fixture table from resources and copy it to a temporary location for testing.
   */
  private HoodieTableMetaClient loadFixtureTable(HoodieTableVersion version) throws IOException {
    String fixtureName = getFixtureName(version);
    String resourcePath = FIXTURES_BASE_PATH + fixtureName;
    
    // Extract fixture zip from resources to temp directory
    extractFixtureToTempDir(resourcePath, tempDir.toString());
    
    // Get the table name from fixture (remove .zip extension)
    String tableName = fixtureName.replace(".zip", "");
    String tablePath = tempDir.resolve(tableName).toString();
    
    // Initialize meta client for the copied fixture
    metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance())
        .setBasePath(tablePath)
        .build();
    
    LOG.info("Loaded fixture table {} at version {}", fixtureName, metaClient.getTableConfig().getTableVersion());
    return metaClient;
  }

  /**
   * Extract fixture zip file from resources to temporary directory.
   */
  private void extractFixtureToTempDir(String resourcePath, String tempPath) throws IOException {
    LOG.info("Loading fixture from resource path: {}", resourcePath);
    
    try (ZipInputStream zip = new ZipInputStream(getClass().getResourceAsStream(resourcePath))) {
      if (zip == null) {
        throw new IOException("Fixture not found at: " + resourcePath);
      }
      
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        File file = Paths.get(tempPath, entry.getName()).toFile();
        if (entry.isDirectory()) {
          file.mkdirs();
          continue;
        }
        
        // Create parent directories if they don't exist
        file.getParentFile().mkdirs();
        
        // Extract file content
        byte[] buffer = new byte[10000];
        try (BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(file.toPath()))) {
          int count;
          while ((count = zip.read(buffer)) != -1) {
            out.write(buffer, 0, count);
          }
        }
      }
    }
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
  private HoodieTableVersion getNextVersion(HoodieTableVersion current) {
    switch (current) {
      case FOUR:
        return HoodieTableVersion.FIVE;
      case FIVE:
        return HoodieTableVersion.SIX;
      case SIX:
        // even though there is a table version 7, this is not an official release and serves as a bridge
        // so the next version should be 8
        return HoodieTableVersion.EIGHT;
      case EIGHT:
        return HoodieTableVersion.NINE;
      case NINE:
        return null; // No higher version available
      default:
        return null;
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
   * Provide test parameters for fixture versions.
   */
  private static Stream<Arguments> fixtureVersions() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.FOUR),   // Hudi 0.11.1
        Arguments.of(HoodieTableVersion.FIVE),   // Hudi 0.12.2
        Arguments.of(HoodieTableVersion.SIX),    // Hudi 0.14
        Arguments.of(HoodieTableVersion.EIGHT),  // Hudi 1.0.2
        Arguments.of(HoodieTableVersion.NINE)    // Hudi 1.1
    );
  }

  /**
   * Assert table version on both data table and metadata table (if exists).
   * Adapted from TestUpgradeDowngrade.assertTableVersionOnDataAndMetadataTable().
   */
  private void assertTableVersionOnDataAndMetadataTable(
      HoodieTableMetaClient metaClient, HoodieTableVersion expectedVersion) throws IOException {
    assertTableVersion(metaClient, expectedVersion);

    if (expectedVersion.versionCode() >= HoodieTableVersion.FOUR.versionCode()) {
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

  /**
   * Assert table version by checking both in-memory config and persisted properties file.
   * Adapted from TestUpgradeDowngrade.assertTableVersion().
   */
  private void assertTableVersion(
      HoodieTableMetaClient metaClient, HoodieTableVersion expectedVersion) throws IOException {
    assertEquals(expectedVersion.versionCode(),
        metaClient.getTableConfig().getTableVersion().versionCode());
    StoragePath propertyFile = new StoragePath(
        metaClient.getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    // Load the properties and verify
    InputStream inputStream = metaClient.getStorage().open(propertyFile);
    HoodieConfig config = new HoodieConfig();
    config.getProps().load(inputStream);
    inputStream.close();
    assertEquals(Integer.toString(expectedVersion.versionCode()),
        config.getString(HoodieTableConfig.VERSION));
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
    
    // Validate upgrade/downgrade specific changes
    if (fromVersion.versionCode() < toVersion.versionCode()) {
      validateUpgradeProperties(fromVersion, toVersion, metaClient, tableConfig);
    } else if (fromVersion.versionCode() > toVersion.versionCode()) {
      validateDowngradeProperties(fromVersion, toVersion, metaClient, tableConfig);
    }
  }

  /**
   * Validate basic properties for version 4.
   */
  private void validateVersion4Properties(HoodieTableConfig tableConfig) {
    // Basic table properties should exist
    assertNotNull(tableConfig.getTableName(), "Table name should be set");
    assertEquals(HoodieTableType.MERGE_ON_READ, tableConfig.getTableType(), "Table should be MOR type");
    assertTrue(tableConfig.getRecordKeyFieldProp() != null && !tableConfig.getRecordKeyFieldProp().isEmpty(),
        "Record key field should be set");
    assertTrue(tableConfig.getPartitionFieldProp() != null && !tableConfig.getPartitionFieldProp().isEmpty(),
        "Partition field should be set");
  }

  /**
   * Validate properties for version 5 (default partition path changes).
   */
  private void validateVersion5Properties(HoodieTableConfig tableConfig) {
    validateVersion4Properties(tableConfig);
    // Version 5 mainly focuses on default partition path validation
    // The upgrade handler validates this during upgrade, so we just ensure basic properties
  }

  /**
   * Validate properties for version 6 (auxiliary folder cleanup).
   */
  private void validateVersion6Properties(HoodieTableConfig tableConfig) {
    validateVersion5Properties(tableConfig);
    // Version 6 focuses on auxiliary folder cleanup
    // Note: Timeline layout validation moved to transition methods since it changes during upgrades
  }

  /**
   * Validate properties for version 8 (major upgrade with record merge mode, new configuration properties).
   */
  private void validateVersion8Properties(HoodieTableConfig tableConfig) {
    validateVersion6Properties(tableConfig);
    
    // Note: Timeline layout validation moved to transition methods since it changes during upgrades
    
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
   * Validate properties added during upgrade operations.
   */
  private void validateUpgradeProperties(HoodieTableVersion fromVersion, HoodieTableVersion toVersion,
                                       HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig) {
    LOG.debug("Validating upgrade properties: {} -> {}", fromVersion, toVersion);
    
    // Validate timeline layout changes during upgrades
    if (fromVersion.versionCode() < HoodieTableVersion.EIGHT.versionCode() 
        && toVersion.versionCode() >= HoodieTableVersion.EIGHT.versionCode()) {
      // Timeline layout should change from V1 to V2 during upgrade to V8+
      Option<TimelineLayoutVersion> layoutVersion = tableConfig.getTimelineLayoutVersion();
      assertTrue(layoutVersion.isPresent(), "Timeline layout version should be present after upgrade to V8+");
      assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_2, layoutVersion.get(),
          "Timeline layout should be upgraded to V2 during upgrade to V8+");
          
      // These properties should be added during upgrade to V8+
      assertTrue(tableConfig.contains(HoodieTableConfig.TIMELINE_PATH),
          "Timeline path should be added during upgrade to V8+");
      assertTrue(tableConfig.contains(HoodieTableConfig.RECORD_MERGE_MODE),
          "Record merge mode should be added during upgrade to V8+");
      assertTrue(tableConfig.contains(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID),
          "Record merge strategy ID should be added during upgrade to V8+");
      assertTrue(tableConfig.contains(HoodieTableConfig.INITIAL_VERSION),
          "Initial version should be added during upgrade to V8+");
    }
  }

  /**
   * Validate properties removed/changed during downgrade operations.
   */
  private void validateDowngradeProperties(HoodieTableVersion fromVersion, HoodieTableVersion toVersion,
                                         HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig) {
    LOG.debug("Validating downgrade properties: {} -> {}", fromVersion, toVersion);
    
    // Downgrading from V8+ to pre-V8 should remove certain properties
    if (fromVersion.versionCode() >= HoodieTableVersion.EIGHT.versionCode() 
        && toVersion.versionCode() < HoodieTableVersion.EIGHT.versionCode()) {
      // These V8+ properties should be removed or modified during downgrade
      assertFalse(tableConfig.contains(HoodieTableConfig.TIMELINE_PATH) 
          && !HoodieTableConfig.TIMELINE_PATH.defaultValue().equals(tableConfig.getString(HoodieTableConfig.TIMELINE_PATH)),
          "Timeline path should be reset during downgrade from V8+");
      
      // Timeline layout should be downgraded to V1
      Option<TimelineLayoutVersion> layoutVersion = tableConfig.getTimelineLayoutVersion();
      if (layoutVersion.isPresent()) {
        assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_1, layoutVersion.get(),
            "Timeline layout should be downgraded to V1");
      }
    }
  }

  /**
   * Perform data validation on the table to ensure it remains operational.
   * This performs a simple read-only validation to verify table accessibility.
   */
  private void performDataValidationOnTable(HoodieTableMetaClient metaClient, String stage) {
    LOG.info("Performing data validation on table {}", stage);
    
    try {
      // Simple read validation to ensure table is accessible
      long rowCount = performSimpleReadValidation(metaClient);
      LOG.debug("Read {} rows from table {}", rowCount, stage);
      
      // Verify table metadata is accessible
      assertNotNull(metaClient.getTableConfig(), "Table config should be accessible " + stage);
      assertNotNull(metaClient.getTableConfig().getTableName(), "Table name should be accessible " + stage);
      
      LOG.info("✓ Data validation passed {} (table accessible, {} rows)", stage, rowCount);
    } catch (Exception e) {
      throw new RuntimeException("Data validation failed " + stage, e);
    }
  }


  /**
   * Perform simple read validation and return the total row count.
   */
  private long performSimpleReadValidation(HoodieTableMetaClient metaClient) {
    LOG.debug("Performing read validation on table at: {}", metaClient.getBasePath());
    try {
      // Log table information for debugging
      String basePath = metaClient.getBasePath().toString();
      LOG.debug("Table base path: {}", basePath);
      LOG.debug("Table version: {}", metaClient.getTableConfig().getTableVersion());
      LOG.debug("Table type: {}", metaClient.getTableConfig().getTableType());
      
      // Read entire table using Spark's built-in Hudi support
      LOG.debug("Attempting to read table using Spark SQL...");
      Dataset<Row> tableData = sqlContext.read()
          .format("hudi")
          .load(basePath);
      
      // Validate read operation succeeded
      assertNotNull(tableData, "Table read should not return null");
      LOG.debug("Table read operation succeeded");
      
      // Count rows
      LOG.debug("Counting rows...");
      long rowCount = tableData.count();
      assertTrue(rowCount >= 0, "Row count should be non-negative");
      
      LOG.debug("Successfully read {} rows from table", rowCount);
      return rowCount;
    } catch (Exception e) {
      LOG.error("Read validation failed for table at: {} (version: {})", 
          metaClient.getBasePath(), metaClient.getTableConfig().getTableVersion(), e);
      throw new RuntimeException("Read validation failed", e);
    }
  }
}
