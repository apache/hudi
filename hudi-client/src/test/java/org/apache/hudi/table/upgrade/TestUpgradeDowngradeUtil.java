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

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.MarkerFiles;
import org.apache.hudi.testutils.Assertions;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link UpgradeDowngradeUtil}.
 */
public class TestUpgradeDowngradeUtil extends HoodieClientTestBase {

  private static final Logger LOG = LogManager.getLogger(TestUpgradeDowngradeUtil.class);
  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with induceResiduesFromPrevUpgrade={0}, deletePartialMarkerFiles={1} and TableType = {2}";

  public static Stream<Arguments> configParams() {
    Object[][] data =
        new Object[][] {{true, true, HoodieTableType.COPY_ON_WRITE}, {true, false, HoodieTableType.COPY_ON_WRITE}, {false, true, HoodieTableType.COPY_ON_WRITE},
            {false, false, HoodieTableType.COPY_ON_WRITE},
            {true, true, HoodieTableType.MERGE_ON_READ}, {true, false, HoodieTableType.MERGE_ON_READ}, {false, true, HoodieTableType.MERGE_ON_READ}, {false, false, HoodieTableType.MERGE_ON_READ}};

    //new Object[][] {{false, false, HoodieTableType.COPY_ON_WRITE}};
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testUpgrade(boolean induceResiduesFromPrevUpgrade, boolean deletePartialMarkerFiles, HoodieTableType tableType) throws IOException {
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(HOODIE_TABLE_TYPE_PROP_NAME, HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(false).withProps(params).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);

    // prepare data. Make 2 commits, in which 2nd is not committed.
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    Pair<List<HoodieRecord>, List<HoodieRecord>> inputRecords = twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, client, false);

    HoodieTable<?> table = this.getHoodieTable(metaClient, cfg);
    HoodieInstant commitInstant = table.getPendingCommitTimeline().lastInstant().get();

    // delete one of the marker files in 2nd commit if need be.
    MarkerFiles markerFiles = new MarkerFiles(table, commitInstant.getTimestamp());
    List<String> markerPaths = markerFiles.allMarkerFilePaths();
    if (deletePartialMarkerFiles) {
      String toDeleteMarkerFile = markerPaths.get(0);
      table.getMetaClient().getFs().delete(new Path(table.getMetaClient().getTempFolderPath() + "/" + commitInstant.getTimestamp() + "/" + toDeleteMarkerFile));
      markerPaths.remove(toDeleteMarkerFile);
    }

    // set hoodie.table.version to 0 in hoodie.properties file
    metaClient.getTableConfig().setHoodieTableVersion(HoodieTableVersion.ZERO, metaClient.getFs(), metaClient.getMetaPath());

    // if induce residues are set, copy property file to orig file. Upgrade should skip actual upgrade steps but should update hoodie.table.version
    if (induceResiduesFromPrevUpgrade) {
      copyPropertyFileToOrig();
    }

    // should re-create marker files for 2nd commit since its pending. If there was any residues, no upgrade steps should happen except for updating the hoodie.table.version
    UpgradeDowngradeUtil.doUpgradeOrDowngrade(metaClient, HoodieTableVersion.ONE, cfg, jsc, null);

    // assert marker files
    assertMarkerFilesForUpgrade(table, commitInstant, induceResiduesFromPrevUpgrade, firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, markerPaths);

    // verify hoodie.table.version got upgraded
    assertEquals(metaClient.getTableConfig().getHoodieTableVersionFromPropertyFile().version, HoodieTableVersion.ONE.version);
    assertTableVersionFromPropertyFile(HoodieTableVersion.ONE);

    // trigger 3rd commit with marker based rollback enabled.
    List<HoodieRecord> thirdBatch = triggerCommit("003", tableType, true);

    // Check the entire dataset has all records only from 1st commit and 3rd commit since 2nd is expected to be rolledback.
    assertRows(inputRecords.getKey(), thirdBatch);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testDowngrade(boolean induceResiduesFromPrevUpgrade, boolean deletePartialMarkerFiles, HoodieTableType tableType) throws IOException {
    prepForDowngrade();
    // init config, table and client.
    Map<String, String> params = new HashMap<>();
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(HOODIE_TABLE_TYPE_PROP_NAME, HoodieTableType.MERGE_ON_READ.name());
      metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    }
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(false).withProps(params).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);

    // prepare data. Make 2 commits, in which 2nd is not committed.
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    Pair<List<HoodieRecord>, List<HoodieRecord>> inputRecords = twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, client, false);

    HoodieTable<?> table = this.getHoodieTable(metaClient, cfg);
    HoodieInstant commitInstant = table.getPendingCommitTimeline().lastInstant().get();

    // delete one of the marker files in 2nd commit if need be.
    MarkerFiles markerFiles = new MarkerFiles(table, commitInstant.getTimestamp());
    List<String> markerPaths = markerFiles.allMarkerFilePaths();
    if (deletePartialMarkerFiles) {
      String toDeleteMarkerFile = markerPaths.get(0);
      table.getMetaClient().getFs().delete(new Path(table.getMetaClient().getTempFolderPath() + "/" + commitInstant.getTimestamp() + "/" + toDeleteMarkerFile));
      markerPaths.remove(toDeleteMarkerFile);
    }

    // set hoodie.table.version to 1 in hoodie.properties file
    prepForDowngrade();

    // if induce residues are set, copy property file to orig file. Downgrade should skip actual downgrade steps but should update hoodie.table.version
    if (induceResiduesFromPrevUpgrade) {
      copyPropertyFileToOrig();
    }

    // downgrade should be performed. all marker files should be deleted
    UpgradeDowngradeUtil.doUpgradeOrDowngrade(metaClient, HoodieTableVersion.ZERO, cfg, jsc, null);

    // assert marker files
    assertMarkerFilesForDowngrade(table, commitInstant, induceResiduesFromPrevUpgrade);

    // verify hoodie.table.version got downgraded
    assertEquals(metaClient.getTableConfig().getHoodieTableVersionFromPropertyFile().version, HoodieTableVersion.ZERO.version);
    assertTableVersionFromPropertyFile(HoodieTableVersion.ZERO);

    // trigger 3rd commit with marker based rollback disabled.
    List<HoodieRecord> thirdBatch = triggerCommit("003", tableType, false);

    // Check the entire dataset has all records only from 1st commit and 3rd commit since 2nd is expected to be rolledback.
    assertRows(inputRecords.getKey(), thirdBatch);
  }

  private void assertMarkerFilesForDowngrade(HoodieTable table, HoodieInstant commitInstant, boolean induceResiduesFromPrevUpgrade) throws IOException {
    // Verify recreated marker files are as expected
    MarkerFiles markerFiles = new MarkerFiles(table, commitInstant.getTimestamp());
    if (induceResiduesFromPrevUpgrade) {
      assertTrue(markerFiles.doesMarkerDirExist());
    } else {
      assertFalse(markerFiles.doesMarkerDirExist());
    }
  }

  private void assertMarkerFilesForUpgrade(HoodieTable table, HoodieInstant commitInstant, boolean induceResiduesFromPrevUpgrade, List<FileSlice> firstPartitionCommit2FileSlices,
      List<FileSlice> secondPartitionCommit2FileSlices, List<String> originalMarkerPaths) throws IOException {
    // Verify recreated marker files are as expected
    MarkerFiles markerFiles = new MarkerFiles(table, commitInstant.getTimestamp());
    assertTrue(markerFiles.doesMarkerDirExist());
    List<String> files = markerFiles.allMarkerFilePaths();

    if (!induceResiduesFromPrevUpgrade) {
      assertEquals(2, files.size());
      List<String> actualFiles = new ArrayList<>();
      for (String file : files) {
        String fileName = MarkerFiles.stripMarkerSuffix(file);
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
    } else {
      // if residues are present i.e hoodie.properties.orig, no recreation of marker files happens. So, verify original marker files are left intact.
      assertEquals(originalMarkerPaths.size(), files.size());
      List<String> actualFiles = new ArrayList<>();
      for (String file : files) {
        actualFiles.add(file);
      }
      assertEquals(originalMarkerPaths, actualFiles);
    }
  }

  private List<HoodieRecord> triggerCommit(String newCommitTime, HoodieTableType tableType, boolean enableMarkedBasedRollback) {
    Map<String, String> params = new HashMap<>();
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      params.put(HOODIE_TABLE_TYPE_PROP_NAME, HoodieTableType.MERGE_ON_READ.name());
    }
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withRollbackUsingMarkers(enableMarkedBasedRollback).withProps(params).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg, true);

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
   * @param client instance of {@link HoodieWriteClient} to use.
   * @param commitSecondUpsert true if 2nd commit needs to be committed. false otherwise.
   * @return a pair of list of records from 1st and 2nd batch.
   */
  private Pair<List<HoodieRecord>, List<HoodieRecord>> twoUpsertCommitDataWithTwoPartitions(List<FileSlice> firstPartitionCommit2FileSlices,
      List<FileSlice> secondPartitionCommit2FileSlices,
      HoodieWriteConfig cfg, HoodieWriteClient client,
      boolean commitSecondUpsert) throws IOException {
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    //1. prepare data
    HoodieTestDataGenerator.writePartitionMetadata(fs, new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}, basePath);
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

  private void prepForDowngrade() {
    metaClient.getTableConfig().setHoodieTableVersion(HoodieTableVersion.ONE, metaClient.getFs(), metaClient.getMetaPath());
  }

  private void copyPropertyFileToOrig() throws IOException {
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    Path origPropertyFile = new Path(metaClient.getMetaPath() + "/" + UpgradeDowngradeUtil.HOODIE_ORIG_PROPERTY_FILE);

    // Step1: Copy hoodie.properties to hoodie.properties.orig
    FileUtil.copy(metaClient.getFs(), propertyFile, metaClient.getFs(), origPropertyFile,
        false, metaClient.getHadoopConf());
  }

  private void assertTableVersionFromPropertyFile(HoodieTableVersion expectedVersion) throws IOException {
    Path propertyFile = new Path(metaClient.getMetaPath() + "/" + HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    // Load the properties and verify
    FSDataInputStream fsDataInputStream = metaClient.getFs().open(propertyFile);
    Properties prop = new Properties();
    prop.load(fsDataInputStream);
    fsDataInputStream.close();
    assertEquals(Integer.toString(expectedVersion.version), prop.getProperty(HoodieTableConfig.HOODIE_TABLE_VERSION_PROP_NAME));
  }

}
