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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Disabled;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.apache.hudi.keygen.KeyGenUtils.getComplexKeygenErrorMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for upgrade/downgrade operations using pre-created fixture tables
 * from different Hudi releases.
 */
public class TestUpgradeDowngrade extends SparkClientFunctionalTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeDowngrade.class);
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private HoodieTableMetaClient metaClient;

  private static String getFixturesBasePath(String suffix) {
    if (suffix.contains("complex-keygen")) {
      return "/upgrade-downgrade-fixtures/complex-keygen-tables/";
    } else if (suffix.contains("mor")) {
      return "/upgrade-downgrade-fixtures/mor-tables/";
    } else if (suffix.contains("cow")) {
      return "/upgrade-downgrade-fixtures/cow-tables/";
    } else if (suffix.contains("payload")) {
      return "/upgrade-downgrade-fixtures/payload-tables/";
    } else {
      return "/upgrade-downgrade-fixtures/unsupported-upgrade-tables/";
    }
  }

  @Disabled
  @ParameterizedTest
  @MethodSource("upgradeDowngradeVersionPairs")
  public void testUpgradeOrDowngrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion, String suffix) throws Exception {
    boolean isUpgrade = fromVersion.lesserThan(toVersion);
    String operation = isUpgrade ? "upgrade" : "downgrade";
    LOG.info("Testing {} from version {} to {}", operation, fromVersion, toVersion);

    HoodieTableMetaClient originalMetaClient = loadFixtureTable(fromVersion, suffix);
    assertEquals(fromVersion, originalMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at expected version");

    HoodieWriteConfig config = createWriteConfig(originalMetaClient, true);

    int initialPendingCommits = originalMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    int initialCompletedCommits = originalMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();

    Dataset<Row> originalData = readTableData(originalMetaClient, "before " + operation);

    // Confirm that there are log files before rollback and compaction operations
    if (isRollbackAndCompactTransition(fromVersion, toVersion)) {
      validateLogFilesCount(originalMetaClient, operation, suffix.equals("-mor"));
    }

    new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(toVersion, null);

    HoodieTableMetaClient resultMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();

    assertTableVersionOnDataAndMetadataTable(resultMetaClient, toVersion);
    validateVersionSpecificProperties(resultMetaClient, toVersion);
    validateDataConsistency(originalData, resultMetaClient, "after " + operation);

    // Validate pending commits based on whether this transition performs rollback and compaction operations
    int finalPendingCommits = resultMetaClient.getCommitsTimeline().filterPendingExcludingCompaction().countInstants();
    if (isRollbackAndCompactTransition(fromVersion, toVersion)) {
      // Handlers that call rollbackFailedWritesAndCompact() clear all pending commits
      assertEquals(0, finalPendingCommits,
          "Pending commits should be cleared to 0 after " + operation);
      // Validate no log files remain after rollback and compaction
      validateLogFilesCount(resultMetaClient, operation, false);
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
  @MethodSource("versionsBelowSix")
  public void testUpgradeForVersionsStartingBelowSixBlocked(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing auto-upgrade disabled for version {} (below SIX)", originalVersion);
    
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion);
    HoodieTableVersion targetVersion = getNextVersion(originalVersion).get();
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, false);
    
    // For versions below SIX with autoUpgrade disabled, expect exception
    HoodieUpgradeDowngradeException exception = assertThrows(HoodieUpgradeDowngradeException.class,
            () -> new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance()).run(targetVersion, null),
            "Expected HoodieUpgradeDowngradeException for version " + originalVersion + " with autoUpgrade disabled"
    );
    
    // Validate exception message
    String expectedMessage = String.format("Hudi 1.x release only supports table version greater than version 6 or above. "
            + "Please upgrade table from version %s to %s using a Hudi release prior to 1.0.0",
        originalVersion.versionCode(), HoodieTableVersion.SIX.versionCode());
    assertEquals(expectedMessage, exception.getMessage(),
        "Exception message should match expected format");
  }

  @ParameterizedTest
  @MethodSource("versionsSixAndAbove")
  public void testAutoUpgradeDisabledForVersionsSixAndAbove(HoodieTableVersion originalVersion) throws Exception {
    LOG.info("Testing auto-upgrade disabled for version {} (SIX and above)", originalVersion);
    
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(originalVersion, "-mor");
    
    Option<HoodieTableVersion> targetVersionOpt = getNextVersion(originalVersion);
    if (!targetVersionOpt.isPresent()) {
      LOG.info("Skipping auto-upgrade test for version {} (no higher version available)", originalVersion);
      return;
    }
    HoodieTableVersion targetVersion = targetVersionOpt.get();
    
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, false);
    
    // For versions SIX and above, the original behavior should work
    new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Create fresh meta client to validate that version remained unchanged 
    HoodieTableMetaClient unchangedMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    assertEquals(originalVersion, unchangedMetaClient.getTableConfig().getTableVersion(),
        "Table version should remain unchanged when auto-upgrade is disabled");
    validateVersionSpecificProperties(unchangedMetaClient, originalVersion);
    performDataValidationOnTable(unchangedMetaClient, "after auto-upgrade disabled test");
    
    LOG.info("Auto-upgrade disabled test passed for version {}", originalVersion);
  }

  /**
   * Test cases for auto-upgrade with different hoodie.write.table.version configurations.
   * Each case starts with table version SIX and tests different write table version settings.
   * Test params are: Integer writeTableVersion, HoodieTableVersion expectedVersion, String description
   */
  private static Stream<Arguments> writeTableVersionTestCases() {
    return Stream.of(
            // Case 1: No explicit hoodie.write.table.version (uses default)
            Arguments.of(Option.empty(), HoodieTableVersion.current(), "Default version upgrade to current version NINE"),
            // Case 2: hoodie.write.table.version = 6 (same as current table version)
            Arguments.of(Option.of(HoodieTableVersion.SIX), HoodieTableVersion.SIX, "No upgrade when versions match"),
            // Case 3: hoodie.write.table.version = 8
            Arguments.of(Option.of(HoodieTableVersion.EIGHT), HoodieTableVersion.EIGHT, "Upgrade to table version EIGHT"),
            // Case 4: hoodie.write.table.version = 9
            Arguments.of(Option.of(HoodieTableVersion.NINE), HoodieTableVersion.NINE, "Upgrade to table version NINE")
    );
  }

  @ParameterizedTest
  @MethodSource("writeTableVersionTestCases")
  public void testAutoUpgradeWithWriteTableVersionConfiguration(
      Option<HoodieTableVersion> writeTableVersion, HoodieTableVersion expectedVersion, String description) throws Exception {
    LOG.info("Testing auto-upgrade configuration: {}", description);
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(HoodieTableVersion.SIX, "-mor");
    assertEquals(HoodieTableVersion.SIX, originalMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should start at version SIX");
    
    HoodieWriteConfig.Builder configBuilder = HoodieWriteConfig.newBuilder()
        .withPath(originalMetaClient.getBasePath().toString())
        .withAutoUpgradeVersion(true);
    if (writeTableVersion.isPresent()) {
      configBuilder.withWriteTableVersion(writeTableVersion.get().versionCode());
    }
    
    HoodieWriteConfig config = configBuilder.build();
    HoodieTableVersion targetVersion = config.getWriteVersion();
    
    Dataset<Row> originalData = readTableData(originalMetaClient, "before " + description);
    
    // Run upgrade process
    new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(targetVersion, null);
    
    // Verify final table version and comprehensive validation
    HoodieTableMetaClient resultMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(originalMetaClient.getBasePath())
        .build();
    
    assertEquals(expectedVersion, resultMetaClient.getTableConfig().getTableVersion(),
        description + " - Final table version should match expected version");

    assertTableVersionOnDataAndMetadataTable(resultMetaClient, expectedVersion);
    validateVersionSpecificProperties(resultMetaClient, expectedVersion);
    validateDataConsistency(originalData, resultMetaClient, "after " + description);
  }

  @Test
  public void testNeedsUpgradeWithAutoUpgradeDisabledAndWriteVersionOverride() throws Exception {
    LOG.info("Testing needsUpgrade with auto-upgrade disabled and write version override");
    
    // Test case: Table at version 6, write version set to 8, auto-upgrade disabled
    // Expected: needsUpgrade should return false and set write version to match table version
    HoodieTableMetaClient metaClient = loadFixtureTable(HoodieTableVersion.SIX, "-mor");
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at version SIX");
    
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath().toString())
        .withAutoUpgradeVersion(false)
        .withWriteTableVersion(8)
        .build();
    
    assertEquals(HoodieTableVersion.EIGHT, config.getWriteVersion(),
        "Initial write version should be EIGHT");
    
    boolean result = UpgradeDowngrade.needsUpgrade(metaClient, config, HoodieTableVersion.EIGHT);
    assertFalse(result, "needsUpgrade should return false when auto-upgrade is disabled");
    assertEquals(HoodieTableVersion.SIX, config.getWriteVersion(),
        "Write version should be set to match table version when auto-upgrade is disabled");
  }

  /**
   * Version pairs for testing blocked downgrades to versions below SIX.
   * These test cases should trigger exceptions in the needsDowngrade method.
   */
  private static Stream<Arguments> blockedDowngradeVersionPairs() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.FIVE),    // V6 -> V5 (blocked)
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.FOUR),    // V6 -> V4 (blocked)
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.FIVE),  // V8 -> V5 (blocked)
        Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.FOUR)    // V9 -> V4 (blocked)
    );
  }

  @ParameterizedTest
  @MethodSource("blockedDowngradeVersionPairs")
  public void testDowngradeToVersionsBelowSixBlocked(HoodieTableVersion fromVersion, HoodieTableVersion toVersion) throws Exception {
    LOG.info("Testing blocked downgrade from version {} to {} (below SIX)", fromVersion, toVersion);
    
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(fromVersion);
    assertEquals(fromVersion, originalMetaClient.getTableConfig().getTableVersion(),
        "Fixture table should be at expected fromVersion");
    
    HoodieWriteConfig config = createWriteConfig(originalMetaClient, true);
    
    // Attempt downgrade to version below SIX - should throw exception
    HoodieUpgradeDowngradeException exception = assertThrows(HoodieUpgradeDowngradeException.class,
            () -> new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance()).run(toVersion, null),
            "Expected HoodieUpgradeDowngradeException for downgrade from " + fromVersion + " to " + toVersion
    );
    String expectedMessage = String.format("Hudi 1.x release only supports table version greater than version 6 or above. "
            + "Please downgrade table from version 6 to %s using a Hudi release prior to 1.0.0",
        toVersion.versionCode());
    assertEquals(expectedMessage, exception.getMessage(),
        "Exception message should match expected blocked downgrade format");
  }

  private static Stream<Arguments> testComplexKeygenValidationDuringUpgradeDowngrade() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.NINE, true),
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.NINE, false),
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.EIGHT, true),
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.EIGHT, false),
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.NINE, true),
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.NINE, false),
        Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.SIX, true),
        Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.SIX, false),
        Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.EIGHT, true),
        Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.EIGHT, false),
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SIX, true),
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SIX, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testComplexKeygenValidationDuringUpgradeDowngrade(HoodieTableVersion fromVersion, HoodieTableVersion toVersion,
                                                                boolean enableValidation) throws Exception {
    HoodieTableMetaClient originalMetaClient = loadFixtureTable(fromVersion, "-complex-keygen");
    assertTrue(KeyGeneratorType.isComplexKeyGenerator(originalMetaClient.getTableConfig()));

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(originalMetaClient.getBasePath().toString())
        .withAutoUpgradeVersion(true)
        .withComplexKeygenValidation(enableValidation)
        .build();
    String operation = fromVersion.lesserThan(toVersion) ? "upgrade" : "downgrade";
    Dataset<Row> originalData = readTableData(originalMetaClient, "before " + operation);

    if (enableValidation) {
      HoodieUpgradeDowngradeException exception = assertThrows(HoodieUpgradeDowngradeException.class,
          () -> new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance()).run(toVersion, null),
          "Expected HoodieUpgradeDowngradeException for upgrade with complex keygen validation enabled");

      assertEquals(getComplexKeygenErrorMessage(operation), exception.getMessage(), "Exception message should mention complex key generator issue");
    } else {
      // Should succeed
      new UpgradeDowngrade(originalMetaClient, config, context(), SparkUpgradeDowngradeHelper.getInstance())
          .run(toVersion, null);

      HoodieTableMetaClient resultMetaClient = HoodieTableMetaClient.builder()
          .setConf(storageConf().newInstance())
          .setBasePath(originalMetaClient.getBasePath())
          .build();

      assertTableVersionOnDataAndMetadataTable(resultMetaClient, toVersion);
      validateVersionSpecificProperties(resultMetaClient, toVersion);
      validateDataConsistency(originalData, resultMetaClient, "after " + operation);
    }
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMdtPartitionNotDroppedWhenDowngradedFromTableVersionNine(HoodieTableType tableType) throws Exception {
    HoodieTableVersion fromVersion = HoodieTableVersion.NINE;
    HoodieTableVersion toVersion = HoodieTableVersion.EIGHT;

    Properties props = new Properties();
    props.put(HoodieTableConfig.TYPE.key(), tableType.name());
    HoodieTableMetaClient metaClient =
        getHoodieMetaClient(storageConf(), URI.create(basePath()).getPath(), props);

    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(metaClient.getBasePath())
        .withWriteTableVersion(fromVersion.versionCode())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                .withEnableRecordIndex(true).build())
        .withProps(props)
        .build();

    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context(), writeConfig);
    String partitionPath = "2021/09/11";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[]{partitionPath});

    String instant1 = getCommitTimeAtUTC(1);
    List<HoodieRecord> records = dataGenerator.generateInserts(instant1, 100);
    JavaRDD<HoodieRecord> dataset = jsc().parallelize(records, 2);

    WriteClientTestUtils.startCommitWithTime(writeClient, instant1);
    writeClient.commit(instant1, writeClient.insert(dataset, instant1));

    String instant2 = getCommitTimeAtUTC(5);
    List<HoodieRecord> updates = dataGenerator.generateUpdates(instant2, 50);
    JavaRDD<HoodieRecord> dataset2 = jsc().parallelize(updates, 2);

    WriteClientTestUtils.startCommitWithTime(writeClient, instant2);
    writeClient.commit(instant2, writeClient.upsert(dataset2, instant2));
    metaClient.reloadTableConfig();

    // verify record index partition exists before downgrade
    assertTrue(metaClient.getTableConfig().getMetadataPartitions().contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()));

    HoodieWriteConfig.Builder upgradeWriteConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withProps(props);

    new UpgradeDowngrade(metaClient, upgradeWriteConfig.build(), context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(toVersion, null);

    HoodieTableMetaClient resultMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(metaClient.getBasePath())
        .build();

    resultMetaClient.reloadTableConfig();
    // verify record index partition exists after downgrade
    assertTrue(resultMetaClient.getTableConfig().getMetadataPartitions().contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()));
  }

  /**
   * Load a fixture table from resources and copy it to a temporary location for testing.
   */
  private HoodieTableMetaClient loadFixtureTable(HoodieTableVersion version) throws IOException {
    return loadFixtureTable(version, "");
  }

  /**
   * Load a fixture table from resources and copy it to a temporary location for testing.
   */
  private HoodieTableMetaClient loadFixtureTable(HoodieTableVersion version, String suffix) throws IOException {
    String fixtureName = getFixtureName(version, suffix);
    String resourcePath = getFixturesBasePath(suffix) + fixtureName;

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
   * Load a payload-specific fixture table from resources.
   */
  private HoodieTableMetaClient loadPayloadFixtureTable(HoodieTableVersion version, String payloadType) throws IOException {
    String fixtureName = "hudi-v" + version.versionCode() + "-table-payload-" + payloadType + ".zip";
    String resourcePath = "/upgrade-downgrade-fixtures/payload-tables/" + fixtureName;

    LOG.info("Loading payload fixture from resource path: {}", resourcePath);
    HoodieTestUtils.extractZipToDirectory(resourcePath, tempDir, getClass());

    String tableName = fixtureName.replace(".zip", "");
    String tablePath = tempDir.resolve(tableName).toString();

    metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(tablePath)
        .build();

    LOG.info("Loaded payload fixture table {} at version {}", fixtureName, metaClient.getTableConfig().getTableVersion());
    return metaClient;
  }

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
  public static String getFixtureName(HoodieTableVersion version, String suffix) {
    String baseName;
    switch (version) {
      case FOUR:
        baseName = "hudi-v4";
        break;
      case FIVE:
        baseName = "hudi-v5";
        break;
      case SIX:
        baseName = "hudi-v6";
        break;
      case EIGHT:
        baseName = "hudi-v8";
        break;
      case NINE:
        baseName = "hudi-v9";
        break;
      default:
        throw new IllegalArgumentException("Unsupported fixture version: " + version);
    }

    // Handle different naming patterns based on suffix
    if (suffix.isEmpty()) {
      return baseName + "-table.zip";
    }  else {
      return baseName + "-table" + suffix + ".zip";
    }
  }

  private static Stream<Arguments> tableVersions() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.SIX),    // Hudi 0.14
        Arguments.of(HoodieTableVersion.EIGHT),  // Hudi 1.0.2
        Arguments.of(HoodieTableVersion.NINE)    // Hudi 1.1
    );
  }

  private static Stream<Arguments> versionsBelowSix() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.FOUR),   // Hudi 0.11.1
        Arguments.of(HoodieTableVersion.FIVE)    // Hudi 0.12.2
    );
  }

  private static Stream<Arguments> versionsSixAndAbove() {
    return Stream.of(
        Arguments.of(HoodieTableVersion.SIX),    // Hudi 0.14
        Arguments.of(HoodieTableVersion.EIGHT),  // Hudi 1.0.2
        Arguments.of(HoodieTableVersion.NINE)    // Hudi 1.1
    );
  }

  private static Stream<Arguments> upgradeDowngradeVersionPairs() {
    return Stream.of(
        // Upgrade test cases for six and greater
        Arguments.of(HoodieTableVersion.SIX, HoodieTableVersion.EIGHT, "-mor"),   // V6 -> V8
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.NINE, "-mor"),  // V8 -> V9
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.NINE, "-cow"),  // V8 -> V9

        // Downgrade test cases til six
        Arguments.of(HoodieTableVersion.NINE, HoodieTableVersion.EIGHT, "-mor"),  // V9 -> V8
        Arguments.of(HoodieTableVersion.EIGHT, HoodieTableVersion.SIX, "-mor")   // V8 -> V6
    );
  }

  private static Stream<Arguments> testArgsUpgradeDowngrade() {
    return Stream.of(
        Arguments.of("MOR", RecordMergeMode.EVENT_TIME_ORDERING),
        Arguments.of("MOR", RecordMergeMode.COMMIT_TIME_ORDERING)
    );
  }

  private static Stream<Arguments> testArgsPayloadUpgradeDowngrade() {
    String[] payloadTypes = {
        "default", "overwrite", "partial", "postgres", "mysql",
        "awsdms", "eventtime", "overwritenondefaults"
    };

    return Stream.of("MOR")
        .flatMap(tableType -> Stream.of(RecordMergeMode.EVENT_TIME_ORDERING, RecordMergeMode.COMMIT_TIME_ORDERING)
            .flatMap(recordMergeMode -> Stream.of(payloadTypes)
                .map(payloadType -> Arguments.of(tableType, recordMergeMode, payloadType))));
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
   * Validate version-specific properties for a single table version.
   */
  private void validateVersionSpecificProperties(
      HoodieTableMetaClient metaClient, HoodieTableVersion version) throws IOException {
    LOG.info("Validating version-specific properties for version {}", version);
    
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    
    // Validate properties for the version
    switch (version) {
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

  private void validateVersion4Properties(HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig) throws IOException {
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

  private boolean isMetadataTablePresent(HoodieTableMetaClient metaClient) throws IOException {
    StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
    return metaClient.getStorage().exists(metadataTablePath);
  }

  /**
   * Validate log files count based on expected scenario.
   * This ensures proper behavior before and after rollback and compaction operations.
   */
  private void validateLogFilesCount(HoodieTableMetaClient metaClient, String operation, boolean expectLogFiles) {
    String validationPhase = expectLogFiles ? "before" : "after";
    LOG.info("Validating log files {} rollback and compaction during {}", validationPhase, operation);
    
    // Get the latest completed commit to ensure we're looking at a consistent state
    org.apache.hudi.common.table.timeline.HoodieTimeline completedTimeline = 
        metaClient.getCommitsTimeline().filterCompletedInstants();
    String latestCommit = completedTimeline.lastInstant()
        .map(instant -> instant.requestedTime())
        .orElse(null);
    
    // Get file system view to check for log files using the latest commit state
    try (org.apache.hudi.common.table.view.HoodieTableFileSystemView fsView = 
        org.apache.hudi.common.table.view.HoodieTableFileSystemView.fileListingBasedFileSystemView(
            context(), metaClient, completedTimeline)) {
    
      // Get all partition paths using FSUtils
      List<String> partitionPaths = org.apache.hudi.common.fs.FSUtils.getAllPartitionPaths(
          context(), metaClient, false);
      
      int totalLogFiles = 0;
      
      for (String partitionPath : partitionPaths) {
        // Get latest file slices for this partition
        Stream<org.apache.hudi.common.model.FileSlice> fileSlicesStream = latestCommit != null
            ? fsView.getLatestFileSlicesBeforeOrOn(partitionPath, latestCommit, false)
            : fsView.getLatestFileSlices(partitionPath);
        
        for (org.apache.hudi.common.model.FileSlice fileSlice : fileSlicesStream.collect(Collectors.toList())) {
          int logFileCount = (int) fileSlice.getLogFiles().count();
          totalLogFiles += logFileCount;
        }
      }
      
      if (expectLogFiles) {
        assertTrue(totalLogFiles > 0, 
            "Expected log files but found none during " + operation);
      } else {
        assertEquals(0, totalLogFiles, 
            "No log files should remain after rollback and compaction during " + operation);
      }
      LOG.info("Log file validation passed: {} log files found (expected: {})", 
          totalLogFiles, expectLogFiles ? ">0" : "0");
    } catch (Exception e) {
      throw new RuntimeException("Failed to validate log files during " + operation, e);
    }
  }

  /**
   * Determine if a version transition performs rollback operations that clear all pending commits.
   * These handlers call rollbackFailedWritesAndCompact() which clears pending commits to 0.
   */
  private boolean isRollbackAndCompactTransition(HoodieTableVersion fromVersion, HoodieTableVersion toVersion) {
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

  private void validateVersion8Properties(HoodieTableConfig tableConfig) {
    validatePropertiesForV8Plus(tableConfig);
    assertTrue(tableConfig.contains(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID),
        "Record merge strategy ID should be set for V8");
  }

  private void validateVersion9Properties(HoodieTableMetaClient metaClient, HoodieTableConfig tableConfig) {
    validatePropertiesForV8Plus(tableConfig);

    // Check if index metadata exists and has proper version information
    Option<HoodieIndexMetadata> indexMetadata = metaClient.getIndexMetadata();
    if (indexMetadata.isPresent()) {
      indexMetadata.get().getIndexDefinitions().forEach((indexName, indexDef) -> {
        assertNotNull(indexDef.getVersion(),
            "Index " + indexName + " should have version information in V9");
      });
    }
  }

  private void validatePropertiesForV8Plus(HoodieTableConfig tableConfig) {
    Option<TimelineLayoutVersion> layoutVersion = tableConfig.getTimelineLayoutVersion();
    assertTrue(layoutVersion.isPresent(), "Timeline layout version should be present for V8+");
    assertEquals(TimelineLayoutVersion.LAYOUT_VERSION_2, layoutVersion.get(),
        "Timeline layout should be V2 for V8+");
    assertTrue(tableConfig.contains(HoodieTableConfig.TIMELINE_PATH),
        "Timeline path should be set for V8+");
    assertEquals(HoodieTableConfig.TIMELINE_PATH.defaultValue(),
        tableConfig.getString(HoodieTableConfig.TIMELINE_PATH),
        "Timeline path should have default value");
    assertTrue(tableConfig.contains(HoodieTableConfig.RECORD_MERGE_MODE),
        "Record merge mode should be set for V8+");
    RecordMergeMode mergeMode = tableConfig.getRecordMergeMode();
    assertNotNull(mergeMode, "Merge mode should not be null");
    assertTrue(tableConfig.contains(HoodieTableConfig.INITIAL_VERSION),
        "Initial version should be set for V8+");
    if (tableConfig.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME)) {
      assertTrue(tableConfig.contains(HoodieTableConfig.KEY_GENERATOR_TYPE),
          "Key generator type should be set when key generator class is present");
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

  @ParameterizedTest
  @MethodSource("testArgsPayloadUpgradeDowngrade")
  public void testPayloadUpgradeDowngrade(String tableType, RecordMergeMode recordMergeMode, String payloadType) throws Exception {
    LOG.info("Testing payload upgrade/downgrade for: {} (tableType: {}, recordMergeMode: {})",
        payloadType, tableType, recordMergeMode);

    // Load v6 fixture for this payload type
    HoodieTableMetaClient metaClientV6 = loadPayloadFixtureTable(HoodieTableVersion.SIX, payloadType);
    assertEquals(HoodieTableVersion.SIX, metaClientV6.getTableConfig().getTableVersion(),
        "Fixture table should be at version 6 for payload: " + payloadType);

    // Read original data before upgrade
    Dataset<Row> originalDataV6 = readTableData(metaClientV6, "original v6 for " + payloadType);

    HoodieWriteConfig config = createWriteConfig(metaClientV6, true);

    // Test upgrade v6 -> v9 via write operation
    originalDataV6.write()
        .format("hudi")
        .option(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "true")
        .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(HoodieTableVersion.NINE.versionCode()))
        .option(HoodieWriteConfig.TBL_NAME.key(), metaClientV6.getTableConfig().getTableName())
        .mode(SaveMode.Append)
        .save(metaClientV6.getBasePath().toString());

    HoodieTableMetaClient metaClientV9 = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(metaClientV6.getBasePath())
        .build();

    assertEquals(HoodieTableVersion.NINE, metaClientV9.getTableConfig().getTableVersion(),
        "Table should be upgraded to version 9 for payload: " + payloadType);
    validateDataConsistency(originalDataV6, metaClientV9, "after v6->v9 upgrade for " + payloadType);

    // Add one new record to v9 table by modifying one existing row
    // Read from upgraded table to get correct schema (with _change_operation_type and proper types)
    Dataset<Row> dataAfterUpgrade = readTableData(metaClientV9, "v9 data for new record");
    Dataset<Row> newRecordData = dataAfterUpgrade.limit(1).selectExpr(
        "14 as ts",
        "8L as _event_lsn",
        "'rider-NEW' as rider",
        "'driver-NEW' as driver",
        "fare",
        "Op",
        "'14.1' as _event_seq",
        "14 as _event_bin_file",
        "1 as _event_pos",
        "_change_operation_type"
    ).cache();
    newRecordData.write()
        .format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key(), metaClientV9.getTableConfig().getTableName())
        .mode(SaveMode.Append)
        .save(metaClientV9.getBasePath().toString());

    // Create expected dataset: original 5 records + 1 new record = 6 total
    // Drop Hudi metadata columns from originalDataV6 to match newRecordData schema, as select expr contains only data columns
    Dataset<Row> originalDataV6WithoutMeta = originalDataV6.drop(
        "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
        "_hoodie_partition_path", "_hoodie_file_name");
    Dataset<Row> expectedDataWithNewRecord = originalDataV6WithoutMeta.union(newRecordData).cache();

    // Refresh metaclient and read v9 data (which now contains original 5 + 1 new record = 6 total)
    metaClientV9 = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(metaClientV6.getBasePath())
        .build();

    Dataset<Row> readOptimizedDataUpgradeAndWrite = sqlContext().read()
            .format("hudi")
            .option("hoodie.datasource.query.type", "read_optimized")
            .load(metaClientV9.getBasePath().toString());

    assertEquals(6, readOptimizedDataUpgradeAndWrite.count(),
            "Read-optimized query should return 6 records after upgrade/write: " + payloadType);
    // will perform real time query and do dataframe validation
    validateDataConsistency(expectedDataWithNewRecord, metaClientV9, "dataframe validation after v9 upgrade/write for " + payloadType);

    // Test downgrade v9 -> v6
    new UpgradeDowngrade(metaClientV9, config, context(), SparkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.SIX, null);

    metaClientV6 = HoodieTableMetaClient.builder()
        .setConf(storageConf().newInstance())
        .setBasePath(metaClientV9.getBasePath())
        .build();

    assertEquals(HoodieTableVersion.SIX, metaClientV6.getTableConfig().getTableVersion(),
        "Table should be downgraded to version 6 for payload: " + payloadType);

    Dataset<Row> readOptimizedDataAfterDowngrade = sqlContext().read()
        .format("hudi")
        .option("hoodie.datasource.query.type", "read_optimized")
        .load(metaClientV6.getBasePath().toString());

    assertEquals(6, readOptimizedDataAfterDowngrade.count(), "Read-optimized query should return 6 records after downgrade: " + payloadType);
    // will perform real time query and do dataframe validation
    validateDataConsistency(expectedDataWithNewRecord, metaClientV6, "dataframe validation after v9->v6 downgrade for " + payloadType);
    LOG.info("Completed payload upgrade/downgrade test for: {}", payloadType);
  }
}
