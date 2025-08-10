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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.table.HoodieTableConfig.BASE_FILE_FORMAT;
import static org.apache.hudi.common.table.HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS_INFLIGHT;
import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests {@link UpgradeDowngrade}.
 * New tests for upgrade and downgrade operations should be added to TestUpgradeDowngrade using the test fixtures.
 */
public class TestUpgradeDowngradeLegacy extends HoodieClientTestBase {

  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with deletePartialMarkerFiles={0} and TableType = {1}";
  private static final String TEST_NAME_WITH_DOWNGRADE_PARAMS = "[{index}] Test with deletePartialMarkerFiles={0} and TableType = {1} and "
      + " enableMetadataTable = {2} and from {3} to {4}";

  public static Stream<Arguments> configParams() {
    Object[][] data = new Object[][] {
        {true, HoodieTableType.COPY_ON_WRITE},
        {false, HoodieTableType.COPY_ON_WRITE},
        {true, HoodieTableType.MERGE_ON_READ},
        {false, HoodieTableType.MERGE_ON_READ}
    };
    return Stream.of(data).map(Arguments::of);
  }

  public static Stream<Arguments> downGradeConfigParams() {
    Object[][] data = new Object[][] {
        {true, HoodieTableType.COPY_ON_WRITE, true, HoodieTableVersion.SIX, HoodieTableVersion.FIVE},
        {false, HoodieTableType.COPY_ON_WRITE, false, HoodieTableVersion.SIX, HoodieTableVersion.FIVE},
        {true, HoodieTableType.COPY_ON_WRITE, true, HoodieTableVersion.FIVE, HoodieTableVersion.FOUR},
        {false, HoodieTableType.COPY_ON_WRITE, false, HoodieTableVersion.FIVE, HoodieTableVersion.FOUR},
        {true, HoodieTableType.MERGE_ON_READ, true, HoodieTableVersion.FIVE, HoodieTableVersion.FOUR},
        {false, HoodieTableType.MERGE_ON_READ, false, HoodieTableVersion.FIVE, HoodieTableVersion.FOUR},
        {true, HoodieTableType.COPY_ON_WRITE, true, HoodieTableVersion.FOUR, HoodieTableVersion.TWO},
        {false, HoodieTableType.COPY_ON_WRITE, false, HoodieTableVersion.FOUR, HoodieTableVersion.TWO},
        {true, HoodieTableType.MERGE_ON_READ, true, HoodieTableVersion.FOUR, HoodieTableVersion.TWO},
        {false, HoodieTableType.MERGE_ON_READ, false, HoodieTableVersion.FOUR, HoodieTableVersion.TWO},
        {true, HoodieTableType.COPY_ON_WRITE, false, HoodieTableVersion.TWO, HoodieTableVersion.ONE},
        {false, HoodieTableType.COPY_ON_WRITE, false, HoodieTableVersion.TWO, HoodieTableVersion.ONE},
        {true, HoodieTableType.MERGE_ON_READ, false, HoodieTableVersion.TWO, HoodieTableVersion.ONE},
        {false, HoodieTableType.MERGE_ON_READ, false, HoodieTableVersion.TWO, HoodieTableVersion.ONE},
        {true, HoodieTableType.COPY_ON_WRITE, false, HoodieTableVersion.ONE, HoodieTableVersion.ZERO},
        {false, HoodieTableType.COPY_ON_WRITE, false, HoodieTableVersion.ONE, HoodieTableVersion.ZERO},
        {true, HoodieTableType.MERGE_ON_READ, false, HoodieTableVersion.ONE, HoodieTableVersion.ZERO},
        {false, HoodieTableType.MERGE_ON_READ, false, HoodieTableVersion.ONE, HoodieTableVersion.ZERO}
    };
    return Stream.of(data).map(Arguments::of);
  }

  public static Stream<Arguments> twoToThreeUpgradeConfigParams() {
    Object[][] data = new Object[][] {
        {HoodieTableType.COPY_ON_WRITE, Option.empty()},
        {HoodieTableType.COPY_ON_WRITE, Option.of(TimestampBasedKeyGenerator.class.getName())},
        {HoodieTableType.MERGE_ON_READ, Option.empty()},
        {HoodieTableType.MERGE_ON_READ, Option.of(TimestampBasedKeyGenerator.class.getName())}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void cleanUp() throws Exception {
    cleanupResources();
  }

  @Test
  public void testWithoutAutoUpgrade() throws IOException {
    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params);
    HoodieWriteConfig cfg = getConfigBuilder().withAutoUpgradeVersion(false).withProps(params).build();
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.SIX);
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.EIGHT, null);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
  }

  @Disabled
  @Test
  public void testLeftOverUpdatedPropFileCleanup() throws IOException {
    testUpgradeZeroToOneInternal(true, true, HoodieTableType.MERGE_ON_READ);
  }

  @Disabled
  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testUpgradeZeroToOne(boolean deletePartialMarkerFiles, HoodieTableType tableType) throws IOException {
    testUpgradeZeroToOneInternal(false, deletePartialMarkerFiles, tableType);
  }

  public void testUpgradeZeroToOneInternal(boolean induceResiduesFromPrevUpgrade, boolean deletePartialMarkerFiles, HoodieTableType tableType) throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(false).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    // prepare data. Make 2 commits, in which 2nd is not committed.
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    Pair<List<HoodieRecord>, List<HoodieRecord>> inputRecords = twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, client, false);

    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    prepForUpgradeFromZeroToOne(table);
    HoodieInstant commitsInstant = table.getPendingCommitsTimeline().lastInstant().get();

    // delete one of the marker files in 2nd commit if need be.
    WriteMarkers writeMarkers =
        WriteMarkersFactory.get(getConfig().getMarkersType(), table, commitsInstant.requestedTime());
    List<String> markerPaths = new ArrayList<>(writeMarkers.allMarkerFilePaths());
    if (deletePartialMarkerFiles) {
      String toDeleteMarkerFile = markerPaths.get(0);
      table.getStorage().deleteDirectory(new StoragePath(
          table.getMetaClient().getTempFolderPath() + "/" + commitsInstant.requestedTime()
              + "/" + toDeleteMarkerFile));
      markerPaths.remove(toDeleteMarkerFile);
    }

    // set hoodie.table.version to 0 in hoodie.properties file
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.ZERO);

    if (induceResiduesFromPrevUpgrade) {
      createResidualFile();
    }

    // should re-create marker files for 2nd commit since its pending.
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.ONE, null);

    // assert marker files
    assertMarkerFilesForUpgrade(table, commitsInstant, firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices);

    // verify hoodie.table.version got upgraded
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.ONE);

    // trigger 3rd commit with marker based rollback enabled.
    /* HUDI-2310
    List<HoodieRecord> thirdBatch = triggerCommit("003", tableType, true);

    // Check the entire dataset has all records only from 1st commit and 3rd commit since 2nd is expected to be rolledback.
    assertRows(inputRecords.getKey(), thirdBatch);
    if (induceResiduesFromPrevUpgrade) {
      assertFalse(dfs.exists(new Path(metaClient.getMetaPath(), SparkUpgradeDowngrade.HOODIE_UPDATED_PROPERTY_FILE)));
    }*/
  }

  @Disabled
  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testUpgradeOneToTwo(HoodieTableType tableType) throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params);
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(false).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    // Write inserts
    doInsert(client);

    // downgrade table props
    downgradeTableConfigsFromTwoToOne(cfg);

    // perform upgrade
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.TWO, null);

    // verify hoodie.table.version got upgraded
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.TWO);

    // verify table props
    assertTableProps(cfg);
  }

  @Disabled
  @ParameterizedTest
  @MethodSource("twoToThreeUpgradeConfigParams")
  public void testUpgradeTwoToThree(
      HoodieTableType tableType, Option<String> keyGeneratorClass) throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params);
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder()
        .withRollbackUsingMarkers(false).withProps(params);
    if (keyGeneratorClass.isPresent()) {
      cfgBuilder.withKeyGenerator(keyGeneratorClass.get());
    }
    HoodieWriteConfig cfg = cfgBuilder.build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    // Write inserts
    doInsert(client);

    // downgrade table props
    downgradeTableConfigsFromThreeToTwo(cfg);

    // perform upgrade
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.THREE, null);

    // verify hoodie.table.version got upgraded
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.THREE);

    // verify table props
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Properties originalProps = cfg.getProps();
    assertEquals(tableConfig.getUrlEncodePartitioning(),
        cfg.getStringOrDefault(HoodieTableConfig.URL_ENCODE_PARTITIONING));
    assertEquals(tableConfig.getHiveStylePartitioningEnable(),
        cfg.getStringOrDefault(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE));
    assertEquals(tableConfig.getKeyGeneratorClassName(), originalProps.getOrDefault(
        HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), SimpleKeyGenerator.class.getName()));
  }

  @Disabled
  @Test
  public void testUpgradeDowngradeBetweenThreeAndCurrentVersion() throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params);
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(false).withProps(params).build();

    // write inserts
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    doInsert(client);

    // current version should have TABLE_CHECKSUM key
    assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.current());
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    String checksum = metaClient.getTableConfig().getProps().getString(HoodieTableConfig.TABLE_CHECKSUM.key());

    // downgrade to version 3 and check TABLE_CHECKSUM is still present
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.THREE, null);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.THREE);
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    assertEquals(checksum, metaClient.getTableConfig().getProps().getString(HoodieTableConfig.TABLE_CHECKSUM.key()));

    // remove TABLE_CHECKSUM and upgrade to current version
    metaClient.getTableConfig().getProps().remove(HoodieTableConfig.TABLE_CHECKSUM.key());
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.SIX, null);

    // verify upgrade and TABLE_CHECKSUM
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.SIX);
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    assertEquals(checksum, metaClient.getTableConfig().getProps().getString(HoodieTableConfig.TABLE_CHECKSUM.key()));
  }

  @Disabled
  @Test
  public void testUpgradeFourtoFive() throws Exception {
    testUpgradeFourToFiveInternal(false, false, false);
  }

  @Disabled
  @Test
  public void testUpgradeFourtoFiveWithDefaultPartition() throws Exception {
    testUpgradeFourToFiveInternal(true, false, false);
  }

  @Disabled
  @Test
  public void testUpgradeFourtoFiveWithDefaultPartitionWithSkipValidation() throws Exception {
    testUpgradeFourToFiveInternal(true, true, false);
  }

  @Disabled
  @Test
  public void testUpgradeFourtoFiveWithHiveStyleDefaultPartition() throws Exception {
    testUpgradeFourToFiveInternal(true, false, true);
  }

  @Disabled
  @Test
  public void testUpgradeFourtoFiveWithHiveStyleDefaultPartitionWithSkipValidation() throws Exception {
    testUpgradeFourToFiveInternal(true, true, true);
  }

  private void testUpgradeFourToFiveInternal(boolean assertDefaultPartition, boolean skipDefaultPartitionValidation, boolean isHiveStyle) throws Exception {
    String tableName = metaClient.getTableConfig().getTableName();
    // clean up and re instantiate meta client w/ right table props
    cleanUp();
    initSparkContexts();
    initPath();
    initTestDataGenerator();

    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params, tableName);
    Properties properties = new Properties();
    params.forEach((k, v) -> properties.setProperty(k, v));

    initMetaClient(getTableType(), properties);
    // init config, table and client.
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(false).withWriteTableVersion(6)
        .doSkipDefaultPartitionValidation(skipDefaultPartitionValidation).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    // Write inserts
    doInsert(client);

    if (assertDefaultPartition) {
      if (isHiveStyle) {
        doInsertWithDefaultHiveStylePartition(client);
        cfg.setValue(HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
      } else {
        doInsertWithDefaultPartition(client);
      }
    }

    // downgrade table props
    downgradeTableConfigsFromFiveToFour(cfg);

    // perform upgrade
    if (assertDefaultPartition && !skipDefaultPartitionValidation) {
      // if "default" partition is present, upgrade should fail
      assertThrows(HoodieException.class, () -> new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
          .run(HoodieTableVersion.FIVE, null), "Upgrade from 4 to 5 is expected to fail if \"default\" partition is present.");
    } else {
      new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
          .run(HoodieTableVersion.FIVE, null);

      // verify hoodie.table.version got upgraded
      metaClient = HoodieTableMetaClient.builder()
          .setConf(context.getStorageConf().newInstance()).setBasePath(cfg.getBasePath()).build();
      assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.FIVE);

      // verify table props
      assertTableProps(cfg);
    }
  }

  private void addNewTableParamsToProps(Map<String, String> params) {
    addNewTableParamsToProps(params, metaClient.getTableConfig().getTableName());
  }

  private void addNewTableParamsToProps(Map<String, String> params, String tableName) {
    params.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid");
    params.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "uuid");
    params.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    params.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    params.put(HoodieTableConfig.NAME.key(), tableName);
    params.put(BASE_FILE_FORMAT.key(), BASE_FILE_FORMAT.defaultValue().name());
    params.put("hoodie.table.version", "6");
  }

  private void doInsert(SparkRDDWriteClient client) {
    // Write 1 (only inserts)
    String commit1 = "000";
    WriteClientTestUtils.startCommitWithTime(client, commit1);
    List<HoodieRecord> records = dataGen.generateInserts(commit1, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    client.insert(writeRecords, commit1).collect();
  }

  private void doInsertWithDefaultPartition(SparkRDDWriteClient client) {
    // Write 1 (only inserts)
    dataGen = new HoodieTestDataGenerator(new String[] {DEPRECATED_DEFAULT_PARTITION_PATH});
    String commit1 = "005";
    WriteClientTestUtils.startCommitWithTime(client, commit1);
    List<HoodieRecord> records = dataGen.generateInserts(commit1, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    client.insert(writeRecords, commit1).collect();
  }

  private void doInsertWithDefaultHiveStylePartition(SparkRDDWriteClient client) {
    // Write 1 (only inserts)
    dataGen = new HoodieTestDataGenerator(new String[] {"partition_path=" + DEPRECATED_DEFAULT_PARTITION_PATH});
    String commit1 = "005";
    WriteClientTestUtils.startCommitWithTime(client, commit1);
    List<HoodieRecord> records = dataGen.generateInserts(commit1, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    client.insert(writeRecords, commit1).collect();
  }

  private void downgradeTableConfigsFromTwoToOne(HoodieWriteConfig cfg) throws IOException {
    Properties properties = new Properties(cfg.getProps());
    properties.remove(HoodieTableConfig.RECORDKEY_FIELDS.key());
    properties.remove(HoodieTableConfig.PARTITION_FIELDS.key());
    properties.remove(HoodieTableConfig.NAME.key());
    properties.remove(BASE_FILE_FORMAT.key());
    properties.setProperty(HoodieTableConfig.VERSION.key(), "1");

    metaClient = HoodieTestUtils.init(storageConf, basePath, getTableType(), properties);
    // set hoodie.table.version to 1 in hoodie.properties file
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.ONE);
  }

  private void downgradeTableConfigsFromThreeToTwo(HoodieWriteConfig cfg) throws IOException {
    Properties properties = new Properties(cfg.getProps());
    properties.remove(HoodieTableConfig.URL_ENCODE_PARTITIONING.key());
    properties.remove(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key());
    properties.remove(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key());
    properties.remove(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key());
    properties.setProperty(HoodieTableConfig.VERSION.key(), "2");

    metaClient = HoodieTestUtils.init(storageConf, basePath, getTableType(), properties);
    // set hoodie.table.version to 2 in hoodie.properties file
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.TWO);
  }

  private void downgradeTableConfigsFromFiveToFour(HoodieWriteConfig cfg) throws IOException {
    Properties properties = new Properties();
    cfg.getProps().forEach((k, v) -> properties.setProperty((String) k, (String) v));
    properties.setProperty(HoodieTableConfig.VERSION.key(), "4");
    metaClient = HoodieTestUtils.init(storageConf, basePath, getTableType(), properties);
    // set hoodie.table.version to 4 in hoodie.properties file
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.FOUR);
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(),
        metaClient.getTableConfig().getProps());

    StoragePath metadataTablePath =
        HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
    if (metaClient.getStorage().exists(metadataTablePath)) {
      HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
          .setConf(metaClient.getStorageConf().newInstance()).setBasePath(metadataTablePath).build();
      metaClient.getTableConfig().setTableVersion(HoodieTableVersion.FOUR);
      HoodieTableConfig.update(
          mdtMetaClient.getStorage(), mdtMetaClient.getMetaPath(),
          metaClient.getTableConfig().getProps());
    }

    assertTableVersionOnDataAndMetadataTable(metaClient, HoodieTableVersion.FOUR);
  }

  private void assertTableProps(HoodieWriteConfig cfg) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Properties originalProps = cfg.getProps();
    assertEquals(tableConfig.getPartitionFieldProp(), originalProps.getProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
    assertEquals(tableConfig.getRecordKeyFieldProp(), originalProps.getProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
    assertEquals(tableConfig.getTableName(), cfg.getTableName());
    assertEquals(tableConfig.getBaseFileFormat().name(), originalProps.getProperty(BASE_FILE_FORMAT.key()));
  }

  @Disabled
  @Test
  public void testDowngradeSixToFiveShouldDeleteRecordIndexPartition() throws Exception {
    HoodieWriteConfig config = getConfigBuilder()
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexColumnStats(true)
            .withMetadataIndexBloomFilter(true)
            .withEnableRecordIndex(true).build())
        .build();
    for (MetadataPartitionType partitionType : MetadataPartitionType.getValidValues()) {
      metaClient.getTableConfig().setMetadataPartitionState(metaClient, partitionType.getPartitionPath(), true);
    }
    metaClient.getTableConfig().setMetadataPartitionsInflight(metaClient, MetadataPartitionType.getValidValues());
    String metadataTableBasePath = Paths.get(basePath, METADATA_TABLE_FOLDER_PATH).toString();
    HoodieTableMetaClient metadataTableMetaClient = HoodieTestUtils.init(metadataTableBasePath, MERGE_ON_READ);
    HoodieMetadataTestTable.of(metadataTableMetaClient)
        .addCommit("000")
        .withBaseFilesInPartition(RECORD_INDEX.getPartitionPath(), 0);

    // validate the relevant table states before downgrade
    java.nio.file.Path recordIndexPartitionPath = Paths.get(basePath,
        METADATA_TABLE_FOLDER_PATH, RECORD_INDEX.getPartitionPath());
    Set<String> allPartitions = MetadataPartitionType.getAllPartitionPaths();
    assertTrue(Files.exists(recordIndexPartitionPath), "record index partition should exist.");
    assertEquals(allPartitions, metaClient.getTableConfig().getMetadataPartitions(),
        TABLE_METADATA_PARTITIONS.key() + " should contain all partitions.");
    assertEquals(allPartitions, metaClient.getTableConfig().getMetadataPartitionsInflight(),
        TABLE_METADATA_PARTITIONS_INFLIGHT.key() + " should contain all partitions.");

    // perform downgrade
    prepForDowngradeFromVersion(HoodieTableVersion.SIX);
    new UpgradeDowngrade(metaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.FIVE, null);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // validate the relevant table states after downgrade
    assertFalse(Files.exists(recordIndexPartitionPath), "record index partition should be deleted.");
    assertEquals(Collections.emptySet(), metaClient.getTableConfig().getMetadataPartitions(),
        TABLE_METADATA_PARTITIONS.key() + " should contain all partitions except record_index.");
    assertEquals(Collections.emptySet(), metaClient.getTableConfig().getMetadataPartitionsInflight(),
        TABLE_METADATA_PARTITIONS_INFLIGHT.key() + " should contain all partitions except record_index.");

  }

  @Disabled
  @ParameterizedTest(name = TEST_NAME_WITH_DOWNGRADE_PARAMS)
  @MethodSource("downGradeConfigParams")
  public void testDowngrade(
      boolean deletePartialMarkerFiles, HoodieTableType tableType, boolean enableMetadataTable,
      HoodieTableVersion fromVersion, HoodieTableVersion toVersion) throws IOException {
    MarkerType markerType = (fromVersion.versionCode() >= HoodieTableVersion.TWO.versionCode())
        ? MarkerType.TIMELINE_SERVER_BASED : MarkerType.DIRECT;
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    if (fromVersion.versionCode() >= HoodieTableVersion.TWO.versionCode()) {
      addNewTableParamsToProps(params, metaClient.getTableConfig().getTableName());
    }
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(true)
        .withWriteTableVersion(6)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .withMarkersType(markerType.name()).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    if (fromVersion.versionCode() >= HoodieTableVersion.TWO.versionCode()) {
      // set table configs
      HoodieTableConfig tableConfig = metaClient.getTableConfig();
      tableConfig.setValue(HoodieTableConfig.NAME, cfg.getTableName());
      tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS, cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
      tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, cfg.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
      tableConfig.setValue(BASE_FILE_FORMAT, cfg.getString(BASE_FILE_FORMAT));
    }

    // Downgrade Script for table version 8 is still in progress.
    assertTrue(HoodieTableVersion.SEVEN.greaterThan(fromVersion));
    prepForDowngradeFromVersion(HoodieTableVersion.SIX);

    // prepare data. Make 2 commits, in which 2nd is not committed.
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, client, false);

    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    HoodieInstant commitsInstant = table.getPendingCommitsTimeline().lastInstant().get();

    // delete one of the marker files in 2nd commit if need be.
    WriteMarkers writeMarkers = WriteMarkersFactory.get(markerType, table, commitsInstant.requestedTime());
    List<String> markerPaths = new ArrayList<>(writeMarkers.allMarkerFilePaths());
    if (deletePartialMarkerFiles) {
      String toDeleteMarkerFile = markerPaths.get(0);
      table.getStorage().deleteDirectory(new StoragePath(
          table.getMetaClient().getTempFolderPath() + "/" + commitsInstant.requestedTime()
              + "/" + toDeleteMarkerFile));
      markerPaths.remove(toDeleteMarkerFile);
    }

    prepForDowngradeFromVersion(fromVersion);

    // downgrade should be performed. all marker files should be deleted
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(toVersion, null);

    if (fromVersion.versionCode() == HoodieTableVersion.TWO.versionCode()) {
      // assert marker files
      assertMarkerFilesForDowngrade(table, commitsInstant, toVersion == HoodieTableVersion.ONE);
    }

    // verify hoodie.table.version got downgraded
    metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf().newInstance()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertTableVersionOnDataAndMetadataTable(metaClient, toVersion);

    // trigger 3rd commit with marker based rollback disabled.
    /* HUDI-2310
    List<HoodieRecord> thirdBatch = triggerCommit("003", tableType, false);

    // Check the entire dataset has all records only from 1st commit and 3rd commit since 2nd is expected to be rolledback.
    assertRows(inputRecords.getKey(), thirdBatch);
     */
  }

  @Test
  void testNeedsUpgrade() {
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.autoUpgrade()).thenReturn(true);
    when(writeConfig.getWriteVersion()).thenReturn(HoodieTableVersion.EIGHT);

    // assert no downgrade for table version 7 from table version 8
    boolean shouldDowngrade = new UpgradeDowngrade(metaClient, writeConfig, context, null)
        .needsUpgrade(HoodieTableVersion.SEVEN);
    assertFalse(shouldDowngrade);

    // assert no downgrade for table version 6 from table version 8
    shouldDowngrade = new UpgradeDowngrade(metaClient, writeConfig, context, null)
        .needsUpgrade(HoodieTableVersion.SIX);
    assertFalse(shouldDowngrade);

    // assert no upgrade/downgrade for table version 8 from table version 8
    shouldDowngrade = new UpgradeDowngrade(metaClient, writeConfig, context, null)
        .needsUpgrade(HoodieTableVersion.EIGHT);
    assertFalse(shouldDowngrade);

    // test upgrade from table version six
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.SIX);
    // assert upgrade for table version 7 from table version 6
    boolean shouldUpgrade = new UpgradeDowngrade(metaClient, writeConfig, context, null)
        .needsUpgrade(HoodieTableVersion.SEVEN);
    assertTrue(shouldUpgrade);

    // assert upgrade for table version 8 from table version 6
    shouldUpgrade = new UpgradeDowngrade(metaClient, writeConfig, context, null)
        .needsUpgrade(HoodieTableVersion.EIGHT);
    assertTrue(shouldUpgrade);

    // assert upgrade for table version 8 from table version 6 with auto upgrade set to false
    // should return false
    when(writeConfig.autoUpgrade()).thenReturn(false);
    shouldUpgrade = new UpgradeDowngrade(metaClient, writeConfig, context, null)
        .needsUpgrade(HoodieTableVersion.EIGHT);
    assertFalse(shouldUpgrade);
  }

  private void assertMarkerFilesForDowngrade(HoodieTable table, HoodieInstant commitInstant, boolean assertExists) throws IOException {
    // Verify recreated marker files are as expected
    WriteMarkers writeMarkers = WriteMarkersFactory.get(getConfig().getMarkersType(), table, commitInstant.requestedTime());
    if (assertExists) {
      assertTrue(writeMarkers.doesMarkerDirExist());
      assertEquals(0, getTimelineServerBasedMarkerFileCount(
          table.getMetaClient().getMarkerFolderPath(commitInstant.requestedTime()),
          (FileSystem) table.getStorage().getFileSystem()));
    } else {
      assertFalse(writeMarkers.doesMarkerDirExist());
    }
  }

  private long getTimelineServerBasedMarkerFileCount(String markerDir, FileSystem fileSystem) throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(new Path(markerDir));
    Predicate<String> prefixFilter = pathStr -> pathStr.contains(MARKERS_FILENAME_PREFIX);
    return Arrays.stream(fileStatuses)
        .map(fileStatus -> fileStatus.getPath().toString())
        .filter(prefixFilter)
        .collect(Collectors.toList()).stream().count();
  }

  private void assertMarkerFilesForUpgrade(HoodieTable table, HoodieInstant commitInstant, List<FileSlice> firstPartitionCommit2FileSlices,
                                           List<FileSlice> secondPartitionCommit2FileSlices) throws IOException {
    // Verify recreated marker files are as expected
    WriteMarkers writeMarkers = WriteMarkersFactory.get(getConfig().getMarkersType(), table, commitInstant.requestedTime());
    assertTrue(writeMarkers.doesMarkerDirExist());
    Set<String> files = writeMarkers.allMarkerFilePaths();

    assertEquals(2, files.size());
    List<String> actualFiles = new ArrayList<>();
    for (String file : files) {
      String fileName = WriteMarkers.stripMarkerSuffix(file);
      actualFiles.add(fileName);
    }

    List<FileSlice> expectedFileSlices = new ArrayList<>();
    expectedFileSlices.addAll(firstPartitionCommit2FileSlices);
    expectedFileSlices.addAll(secondPartitionCommit2FileSlices);

    List<String> expectedPaths = new ArrayList<>();
    List<Pair<String, String>> expectedLogFilePaths = new ArrayList<>();

    for (FileSlice fileSlice : expectedFileSlices) {
      String partitionPath = fileSlice.getPartitionPath();
      if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
        for (HoodieLogFile logFile : fileSlice.getLogFiles().collect(Collectors.toList())) {
          // log file format can't be matched as is, since the write token can't be asserted. Hence asserting for partitionpath, fileId and baseCommit time.
          String logBaseCommitTime = logFile.getDeltaCommitTime();
          expectedLogFilePaths.add(Pair.of(partitionPath + "/" + logFile.getFileId(), logBaseCommitTime));
        }
      }
      if (fileSlice.getBaseInstantTime().equals(commitInstant.requestedTime())) {
        String path = fileSlice.getBaseFile().get().getPath();
        // for base files, path can be asserted as is.
        expectedPaths.add(path.substring(path.indexOf(partitionPath)));
      }
    }

    // Trim log file paths only
    List<String> trimmedActualFiles = new ArrayList<>();
    for (String actualFile : actualFiles) {
      if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
        trimmedActualFiles.add(actualFile.substring(0, actualFile.lastIndexOf('.')));
      } else {
        trimmedActualFiles.add(actualFile);
      }
    }
    // assert for base files.
    for (String expected : expectedPaths) {
      if (trimmedActualFiles.contains(expected)) {
        trimmedActualFiles.remove(expected);
      }
    }

    if (expectedLogFilePaths.size() > 0) {
      // assert for log files
      List<Pair<String, String>> actualLogFiles = new ArrayList<>();
      for (String actual : trimmedActualFiles) {
        actualLogFiles.add(Pair.of(actual.substring(0, actual.indexOf('_')), actual.substring(actual.lastIndexOf('_') + 1)));
      }
      assertEquals(expectedLogFilePaths.size(), actualLogFiles.size());
      for (Pair<String, String> entry : expectedLogFilePaths) {
        assertTrue(actualLogFiles.contains(entry));
      }
    } else {
      assertTrue(trimmedActualFiles.size() == 0);
    }
  }

  private List<HoodieRecord> triggerCommit(String newCommitTime, HoodieTableType tableType, boolean enableMarkedBasedRollback) {
    Map<String, String> params = new HashMap<>();
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    }
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(enableMarkedBasedRollback).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);
    client.commit(newCommitTime, jsc.parallelize(statuses));
    return records;
  }

  private void assertRows(List<HoodieRecord> firstBatch, List<HoodieRecord> secondBatch) {
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    Dataset<Row> rows = HoodieClientTestUtils.read(
        jsc, metaClient.getBasePath().toString(), sqlContext, metaClient.getStorage(),
        fullPartitionPaths);
    List<String> expectedRecordKeys = new ArrayList<>();
    for (HoodieRecord rec : firstBatch) {
      expectedRecordKeys.add(rec.getRecordKey());
    }

    for (HoodieRecord rec : secondBatch) {
      expectedRecordKeys.add(rec.getRecordKey());
    }
    List<Row> rowList = rows.collectAsList();
    assertEquals(expectedRecordKeys.size(), rows.count());

    List<String> actualRecordKeys = new ArrayList<>();
    for (Row row : rowList) {
      actualRecordKeys.add(row.getAs("_row_key"));
    }

    for (String expectedRecordKey : expectedRecordKeys) {
      assertTrue(actualRecordKeys.contains(expectedRecordKey));
    }
  }

  /**
   * Create two commits and may or may not commit 2nd commit.
   *
   * @param firstPartitionCommit2FileSlices  list to hold file slices in first partition.
   * @param secondPartitionCommit2FileSlices list of hold file slices from second partition.
   * @param cfg                              instance of {@link HoodieWriteConfig}
   * @param client                           instance of {@link SparkRDDWriteClient} to use.
   * @param commitSecondUpsert               true if 2nd commit needs to be committed. false otherwise.
   * @return a pair of list of records from 1st and 2nd batch.
   */
  private Pair<List<HoodieRecord>, List<HoodieRecord>> twoUpsertCommitDataWithTwoPartitions(List<FileSlice> firstPartitionCommit2FileSlices,
                                                                                            List<FileSlice> secondPartitionCommit2FileSlices,
                                                                                            HoodieWriteConfig cfg, SparkRDDWriteClient client,
                                                                                            boolean commitSecondUpsert) throws IOException {
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(
        new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    //1. prepare data
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(
        metaClient.getStorage(),
        new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}, basePath);
    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);
    client.commit(newCommitTime, jsc.parallelize(statuses));
    /**
     * Write 2 (updates)
     */
    newCommitTime = "002";
    WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

    List<HoodieRecord> records2 = dataGen.generateUpdates(newCommitTime, records);
    statuses = client.upsert(jsc.parallelize(records2, 1), newCommitTime).collect();
    assertNoWriteErrors(statuses);
    if (commitSecondUpsert) {
      client.commit(newCommitTime, jsc.parallelize(statuses));
    }

    //2. assert filegroup and get the first partition fileslice
    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    SyncableFileSystemView fsView = getFileSystemViewWithUnCommittedSlices(table.getMetaClient());
    List<HoodieFileGroup> firstPartitionCommit2FileGroups = fsView.getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit2FileGroups.size());
    firstPartitionCommit2FileSlices.addAll(firstPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList()));
    //3. assert filegroup and get the second partition fileslice
    List<HoodieFileGroup> secondPartitionCommit2FileGroups = fsView.getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionCommit2FileGroups.size());
    secondPartitionCommit2FileSlices.addAll(secondPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList()));

    //4. assert fileslice
    HoodieTableType tableType = metaClient.getTableType();
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      assertEquals(2, firstPartitionCommit2FileSlices.size());
      assertEquals(2, secondPartitionCommit2FileSlices.size());
    } else {
      assertEquals(1, firstPartitionCommit2FileSlices.size());
      assertEquals(1, secondPartitionCommit2FileSlices.size());
    }
    return Pair.of(records, records2);
  }

  /**
   * Since how markers are generated for log file changed in Version Six, we regenerate markers in the way version zero do.
   *
   * @param table instance of {@link HoodieTable}
   */
  private void prepForUpgradeFromZeroToOne(HoodieTable table) throws IOException {
    List<HoodieInstant> instantsToBeParsed  =
        metaClient.getActiveTimeline()
        .getCommitsTimeline()
        .getInstantsAsStream()
        .collect(Collectors.toList());
    for (HoodieInstant instant : instantsToBeParsed) {
      WriteMarkers writeMarkers =
          WriteMarkersFactory.get(table.getConfig().getMarkersType(), table, instant.requestedTime());
      Set<String> oldMarkers = writeMarkers.allMarkerFilePaths();
      boolean hasAppendMarker = oldMarkers.stream().anyMatch(marker -> marker.contains(IOType.APPEND.name()) || marker.contains(IOType.CREATE.name()));
      if (hasAppendMarker) {
        // delete all markers and regenerate
        writeMarkers.deleteMarkerDir(table.getContext(), 2);
        for (String oldMarker : oldMarkers) {
          String typeStr = oldMarker.substring(oldMarker.lastIndexOf(".") + 1);
          IOType type = IOType.valueOf(typeStr);
          String partitionFilePath = WriteMarkers.stripMarkerSuffix(oldMarker);
          StoragePath fullFilePath = new StoragePath(basePath, partitionFilePath);
          String partitionPath = FSUtils.getRelativePartitionPath(
              new StoragePath(basePath), fullFilePath.getParent());
          if (FSUtils.isBaseFile(fullFilePath)) {
            writeMarkers.create(partitionPath, fullFilePath.getName(), type);
          } else {
            String fileId = FSUtils.getFileIdFromFilePath(fullFilePath);
            String baseInstant = FSUtils.getDeltaCommitTimeFromLogPath(fullFilePath);
            String writeToken = FSUtils.getWriteTokenFromLogPath(fullFilePath);
            writeMarkers.create(partitionPath,
                FSUtils.makeBaseFileName(baseInstant, writeToken, fileId, table.getBaseFileFormat().getFileExtension()), type);
          }
        }
        writeMarkers.allMarkerFilePaths()
            .forEach(markerPath -> assertFalse(markerPath.contains(HoodieLogFile.DELTA_EXTENSION)));
      }
    }
  }

  private void prepForDowngradeFromVersion(HoodieTableVersion fromVersion) throws IOException {
    metaClient.getTableConfig().setTableVersion(fromVersion);
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
    metaClient.reloadTableConfig();
    StoragePath propertyFile = new StoragePath(
        metaClient.getMetaPath(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (OutputStream os = metaClient.getStorage().create(propertyFile)) {
      metaClient.getTableConfig().getProps().store(os, "");
    }
  }

  private void createResidualFile() throws IOException {
    Path propertyFile =
        new Path(metaClient.getMetaPath().toString(), HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    Path updatedPropertyFile =
        new Path(metaClient.getMetaPath().toString(), UpgradeDowngrade.HOODIE_UPDATED_PROPERTY_FILE);

    // Step1: Copy hoodie.properties to hoodie.properties.orig
    FileSystem fs = (FileSystem) metaClient.getStorage().getFileSystem();
    FileUtil.copy(fs, propertyFile, fs, updatedPropertyFile, false, storageConf.unwrap());
  }

  private void assertTableVersionOnDataAndMetadataTable(
      HoodieTableMetaClient metaClient, HoodieTableVersion expectedVersion) throws IOException {
    assertTableVersion(metaClient, expectedVersion);

    if (expectedVersion.versionCode() >= HoodieTableVersion.FOUR.versionCode()) {
      StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
      if (metaClient.getStorage().exists(metadataTablePath)) {
        HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
            .setConf(metaClient.getStorageConf().newInstance()).setBasePath(metadataTablePath).build();
        assertTableVersion(mdtMetaClient, expectedVersion);
      }
    }
  }

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
}
