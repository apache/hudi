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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.Assertions;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.BASE_FILE_FORMAT;
import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.util.MarkerUtils.MARKERS_FILENAME_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link UpgradeDowngrade}.
 */
public class TestUpgradeDowngrade extends HoodieClientTestBase {

  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with deletePartialMarkerFiles={0} and TableType = {1}";
  private static final String TEST_NAME_WITH_DOWNGRADE_PARAMS = "[{index}] Test with deletePartialMarkerFiles={0} and TableType = {1} and "
      + "From version = {2}";

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
        {true, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.TWO},
        {false, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.TWO},
        {true, HoodieTableType.MERGE_ON_READ, HoodieTableVersion.TWO},
        {false, HoodieTableType.MERGE_ON_READ, HoodieTableVersion.TWO},
        {true, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.ONE},
        {false, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.ONE},
        {true, HoodieTableType.MERGE_ON_READ, HoodieTableVersion.ONE},
        {false, HoodieTableType.MERGE_ON_READ, HoodieTableVersion.ONE}
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
  public void testLeftOverUpdatedPropFileCleanup() throws IOException {
    testUpgradeZeroToOneInternal(true, true, HoodieTableType.MERGE_ON_READ);
  }

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
      metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(false).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    // prepare data. Make 2 commits, in which 2nd is not committed.
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    Pair<List<HoodieRecord>, List<HoodieRecord>> inputRecords = twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, client, false);

    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    HoodieInstant commitInstant = table.getPendingCommitTimeline().lastInstant().get();

    // delete one of the marker files in 2nd commit if need be.
    WriteMarkers writeMarkers =
        WriteMarkersFactory.get(getConfig().getMarkersType(), table, commitInstant.getTimestamp());
    List<String> markerPaths = new ArrayList<>(writeMarkers.allMarkerFilePaths());
    if (deletePartialMarkerFiles) {
      String toDeleteMarkerFile = markerPaths.get(0);
      table.getMetaClient().getFs().delete(new Path(table.getMetaClient().getTempFolderPath() + "/" + commitInstant.getTimestamp() + "/" + toDeleteMarkerFile));
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
    assertMarkerFilesForUpgrade(table, commitInstant, firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices);

    // verify hoodie.table.version got upgraded
    metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.ONE.versionCode());
    assertTableVersionFromPropertyFile(HoodieTableVersion.ONE);

    // trigger 3rd commit with marker based rollback enabled.
    /* HUDI-2310
    List<HoodieRecord> thirdBatch = triggerCommit("003", tableType, true);

    // Check the entire dataset has all records only from 1st commit and 3rd commit since 2nd is expected to be rolledback.
    assertRows(inputRecords.getKey(), thirdBatch);
    if (induceResiduesFromPrevUpgrade) {
      assertFalse(dfs.exists(new Path(metaClient.getMetaPath(), SparkUpgradeDowngrade.HOODIE_UPDATED_PROPERTY_FILE)));
    }*/
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testUpgradeOneToTwo(HoodieTableType tableType) throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params);
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(false).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    // Write inserts
    doInsert(client);

    // downgrade table props
    downgradeTableConfigsFromTwoToOne(cfg);

    // perform upgrade
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.TWO, null);

    // verify hoodie.table.version got upgraded
    metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.TWO.versionCode());
    assertTableVersionFromPropertyFile(HoodieTableVersion.TWO);

    // verify table props
    assertTableProps(cfg);
  }

  @ParameterizedTest
  @MethodSource("twoToThreeUpgradeConfigParams")
  public void testUpgradeTwoToThree(
      HoodieTableType tableType, Option<String> keyGeneratorClass) throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params);
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder()
        .withAutoCommit(false).withRollbackUsingMarkers(false).withProps(params);
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
    metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), HoodieTableVersion.THREE.versionCode());
    assertTableVersionFromPropertyFile(HoodieTableVersion.THREE);

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

  @Test
  public void testUpgradeDowngradeBetweenThreeAndCurrentVersion() throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    addNewTableParamsToProps(params);
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(false).withProps(params).build();

    // write inserts
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    doInsert(client);

    // current version should have TABLE_CHECKSUM key
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig().getTableVersion());
    assertTableVersionFromPropertyFile(HoodieTableVersion.current());
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    String checksum = metaClient.getTableConfig().getProps().getString(HoodieTableConfig.TABLE_CHECKSUM.key());

    // downgrade to version 3 and check TABLE_CHECKSUM is still present
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.THREE, null);
    assertEquals(HoodieTableVersion.THREE.versionCode(), metaClient.getTableConfig().getTableVersion().versionCode());
    assertTableVersionFromPropertyFile(HoodieTableVersion.THREE);
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    assertEquals(checksum, metaClient.getTableConfig().getProps().getString(HoodieTableConfig.TABLE_CHECKSUM.key()));

    // remove TABLE_CHECKSUM and upgrade to current version
    metaClient.getTableConfig().getProps().remove(HoodieTableConfig.TABLE_CHECKSUM.key());
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.current(), null);

    // verify upgrade and TABLE_CHECKSUM
    metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertEquals(HoodieTableVersion.current().versionCode(), metaClient.getTableConfig().getTableVersion().versionCode());
    assertTableVersionFromPropertyFile(HoodieTableVersion.current());
    assertTrue(metaClient.getTableConfig().getProps().containsKey(HoodieTableConfig.TABLE_CHECKSUM.key()));
    assertEquals(checksum, metaClient.getTableConfig().getProps().getString(HoodieTableConfig.TABLE_CHECKSUM.key()));
  }

  private void addNewTableParamsToProps(Map<String, String> params) {
    params.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid");
    params.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    params.put(HoodieTableConfig.NAME.key(), metaClient.getTableConfig().getTableName());
    params.put(BASE_FILE_FORMAT.key(), BASE_FILE_FORMAT.defaultValue().name());
  }

  private void doInsert(SparkRDDWriteClient client) {
    // Write 1 (only inserts)
    String commit1 = "000";
    client.startCommitWithTime(commit1);
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

    metaClient = HoodieTestUtils.init(hadoopConf, basePath, getTableType(), properties);
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

    metaClient = HoodieTestUtils.init(hadoopConf, basePath, getTableType(), properties);
    // set hoodie.table.version to 2 in hoodie.properties file
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.TWO);
  }

  private void assertTableProps(HoodieWriteConfig cfg) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    Properties originalProps = cfg.getProps();
    assertEquals(tableConfig.getPartitionFieldProp(), originalProps.getProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
    assertEquals(tableConfig.getRecordKeyFieldProp(), originalProps.getProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
    assertEquals(tableConfig.getTableName(), cfg.getTableName());
    assertEquals(tableConfig.getBaseFileFormat().name(), originalProps.getProperty(BASE_FILE_FORMAT.key()));
  }

  @ParameterizedTest(name = TEST_NAME_WITH_DOWNGRADE_PARAMS)
  @MethodSource("downGradeConfigParams")
  public void testDowngrade(boolean deletePartialMarkerFiles, HoodieTableType tableType, HoodieTableVersion fromVersion) throws IOException {
    MarkerType markerType = fromVersion == HoodieTableVersion.TWO ? MarkerType.TIMELINE_SERVER_BASED : MarkerType.DIRECT;
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    if (fromVersion == HoodieTableVersion.TWO) {
      addNewTableParamsToProps(params);
    }
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(true)
        .withMarkersType(markerType.name()).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    if (fromVersion == HoodieTableVersion.TWO) {
      // set table configs
      HoodieTableConfig tableConfig = metaClient.getTableConfig();
      tableConfig.setValue(HoodieTableConfig.NAME, cfg.getTableName());
      tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS, cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
      tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, cfg.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()));
      tableConfig.setValue(BASE_FILE_FORMAT, cfg.getString(BASE_FILE_FORMAT));
    }

    // prepare data. Make 2 commits, in which 2nd is not committed.
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    Pair<List<HoodieRecord>, List<HoodieRecord>> inputRecords = twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, client, false);

    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    HoodieInstant commitInstant = table.getPendingCommitTimeline().lastInstant().get();

    // delete one of the marker files in 2nd commit if need be.
    WriteMarkers writeMarkers = WriteMarkersFactory.get(markerType, table, commitInstant.getTimestamp());
    List<String> markerPaths = new ArrayList<>(writeMarkers.allMarkerFilePaths());
    if (deletePartialMarkerFiles) {
      String toDeleteMarkerFile = markerPaths.get(0);
      table.getMetaClient().getFs().delete(new Path(table.getMetaClient().getTempFolderPath() + "/" + commitInstant.getTimestamp() + "/" + toDeleteMarkerFile));
      markerPaths.remove(toDeleteMarkerFile);
    }

    // set hoodie.table.version to fromVersion in hoodie.properties file
    HoodieTableVersion toVersion = HoodieTableVersion.ZERO;
    if (fromVersion == HoodieTableVersion.TWO) {
      prepForDowngradeFromTwoToOne();
      toVersion = HoodieTableVersion.ONE;
    } else {
      prepForDowngradeFromOneToZero();
    }

    // downgrade should be performed. all marker files should be deleted
    new UpgradeDowngrade(metaClient, cfg, context, SparkUpgradeDowngradeHelper.getInstance())
        .run(toVersion, null);

    if (fromVersion == HoodieTableVersion.TWO) {
      // assert marker files
      assertMarkerFilesForDowngrade(table, commitInstant, toVersion == HoodieTableVersion.ONE);
    }
    
    // verify hoodie.table.version got downgraded
    metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(cfg.getBasePath())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(cfg.getTimelineLayoutVersion()))).build();
    assertEquals(metaClient.getTableConfig().getTableVersion().versionCode(), toVersion.versionCode());
    assertTableVersionFromPropertyFile(toVersion);

    // trigger 3rd commit with marker based rollback disabled.
    /* HUDI-2310
    List<HoodieRecord> thirdBatch = triggerCommit("003", tableType, false);

    // Check the entire dataset has all records only from 1st commit and 3rd commit since 2nd is expected to be rolledback.
    assertRows(inputRecords.getKey(), thirdBatch);
     */
  }

  private void assertMarkerFilesForDowngrade(HoodieTable table, HoodieInstant commitInstant, boolean assertExists) throws IOException {
    // Verify recreated marker files are as expected
    WriteMarkers writeMarkers = WriteMarkersFactory.get(getConfig().getMarkersType(), table, commitInstant.getTimestamp());
    if (assertExists) {
      assertTrue(writeMarkers.doesMarkerDirExist());
      assertEquals(0, getTimelineServerBasedMarkerFileCount(table.getMetaClient().getMarkerFolderPath(commitInstant.getTimestamp()),
          table.getMetaClient().getFs()));
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
    WriteMarkers writeMarkers = WriteMarkersFactory.get(getConfig().getMarkersType(), table, commitInstant.getTimestamp());
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
          String logBaseCommitTime = logFile.getBaseCommitTime();
          expectedLogFilePaths.add(Pair.of(partitionPath + "/" + logFile.getFileId(), logBaseCommitTime));
        }
      }
      if (fileSlice.getBaseInstantTime().equals(commitInstant.getTimestamp())) {
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
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(enableMarkedBasedRollback).withProps(params).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime);
    Assertions.assertNoWriteErrors(statuses.collect());
    client.commit(newCommitTime, statuses);
    return records;
  }

  private void assertRows(List<HoodieRecord> firstBatch, List<HoodieRecord> secondBatch) {
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    Dataset<Row> rows = HoodieClientTestUtils.read(jsc, metaClient.getBasePath(), sqlContext, metaClient.getFs(), fullPartitionPaths);
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
   * @param firstPartitionCommit2FileSlices list to hold file slices in first partition.
   * @param secondPartitionCommit2FileSlices list of hold file slices from second partition.
   * @param cfg instance of {@link HoodieWriteConfig}
   * @param client instance of {@link SparkRDDWriteClient} to use.
   * @param commitSecondUpsert true if 2nd commit needs to be committed. false otherwise.
   * @return a pair of list of records from 1st and 2nd batch.
   */
  private Pair<List<HoodieRecord>, List<HoodieRecord>> twoUpsertCommitDataWithTwoPartitions(List<FileSlice> firstPartitionCommit2FileSlices,
      List<FileSlice> secondPartitionCommit2FileSlices,
      HoodieWriteConfig cfg, SparkRDDWriteClient client,
      boolean commitSecondUpsert) throws IOException {
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    //1. prepare data
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(metaClient.getFs(), new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}, basePath);
    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime);
    Assertions.assertNoWriteErrors(statuses.collect());
    client.commit(newCommitTime, statuses);
    /**
     * Write 2 (updates)
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records2 = dataGen.generateUpdates(newCommitTime, records);
    statuses = client.upsert(jsc.parallelize(records2, 1), newCommitTime);
    Assertions.assertNoWriteErrors(statuses.collect());
    if (commitSecondUpsert) {
      client.commit(newCommitTime, statuses);
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

  private void prepForDowngradeFromOneToZero() throws IOException {
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.ONE);
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream os = metaClient.getFs().create(propertyFile)) {
      metaClient.getTableConfig().getProps().store(os, "");
    }
  }

  private void prepForDowngradeFromTwoToOne() throws IOException {
    metaClient.getTableConfig().setTableVersion(HoodieTableVersion.TWO);
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream os = metaClient.getFs().create(propertyFile)) {
      metaClient.getTableConfig().getProps().store(os, "");
    }
  }

  private void createResidualFile() throws IOException {
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    Path updatedPropertyFile = new Path(metaClient.getMetaPath() + "/" + UpgradeDowngrade.HOODIE_UPDATED_PROPERTY_FILE);

    // Step1: Copy hoodie.properties to hoodie.properties.orig
    FileUtil.copy(metaClient.getFs(), propertyFile, metaClient.getFs(), updatedPropertyFile,
        false, hadoopConf);
  }

  private void assertTableVersionFromPropertyFile(HoodieTableVersion expectedVersion) throws IOException {
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    // Load the properties and verify
    FSDataInputStream fsDataInputStream = metaClient.getFs().open(propertyFile);
    HoodieConfig hoodieConfig = HoodieConfig.create(fsDataInputStream);
    fsDataInputStream.close();
    assertEquals(Integer.toString(expectedVersion.versionCode()), hoodieConfig
        .getString(HoodieTableConfig.VERSION));
  }
}
