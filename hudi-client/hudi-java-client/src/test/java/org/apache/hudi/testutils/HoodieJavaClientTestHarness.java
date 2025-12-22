/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.testutils;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.JavaHoodieIndexFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.JavaHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.utils.HoodieWriterClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.RAW_TRIPS_TEST_NAME;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.GenericRecordValidationTestUtils.readHFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The test harness for resource initialization and cleanup.
 */
public abstract class HoodieJavaClientTestHarness extends HoodieWriterClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieJavaClientTestHarness.class);

  protected StorageConfiguration<Configuration> storageConf;
  protected HoodieJavaEngineContext context;
  protected TestJavaTaskContextSupplier taskContextSupplier;
  protected HoodieStorage storage;
  protected ExecutorService executorService;
  protected HoodieTableFileSystemView tableView;
  protected HoodieJavaWriteClient writeClient;

  @AfterAll
  public static void tearDownAll() throws IOException {
    FileSystem.closeAll();
  }

  @BeforeEach
  protected void initResources() throws IOException {
    basePath = tempDir.resolve("java_client_tests" + System.currentTimeMillis()).toAbsolutePath().toUri().getPath();
    storageConf = getDefaultStorageConf();
    taskContextSupplier = new TestJavaTaskContextSupplier();
    context = new HoodieJavaEngineContext(storageConf, taskContextSupplier);
    initFileSystem(basePath, storageConf);
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  protected void cleanupResources() throws IOException {
    cleanupClients();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupExecutorService();
  }

  public class TestJavaTaskContextSupplier extends TaskContextSupplier {
    int partitionId = 0;
    int stageId = 0;
    long attemptId = 0;

    public void reset() {
      stageId += 1;
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> partitionId;
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return () -> stageId;
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return () -> attemptId;
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }
  }

  protected void initFileSystem(String basePath, StorageConfiguration<?> hadoopConf) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    storage = HoodieStorageUtils.getStorage(basePath, hadoopConf);
    if (storage.getFileSystem() instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) storage.getFileSystem();
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  protected void cleanupFileSystem() throws IOException {
    if (storage != null) {
      LOG.warn("Closing HoodieStorage instance used in previous test-run");
      storage.close();
      storage = null;
    }
  }

  protected void initMetaClient() throws IOException {
    initMetaClient(getTableType());
  }

  protected void initMetaClient(HoodieTableType tableType) throws IOException {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    metaClient = HoodieTestUtils.init(storageConf, basePath, tableType);
  }

  protected void cleanupClients() {
    if (metaClient != null) {
      metaClient = null;
    }
    if (writeClient != null) {
      writeClient.close();
      writeClient = null;
    }
    if (tableView != null) {
      tableView.close();
      tableView = null;
    }
  }

  protected void cleanupExecutorService() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
      this.executorService = null;
    }
  }

  protected HoodieJavaWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    if (null != writeClient) {
      writeClient.close();
      writeClient = null;
    }
    writeClient = new HoodieJavaWriteClient(context, cfg);
    return writeClient;
  }

  public void syncTableMetadata(HoodieWriteConfig writeConfig) {
    if (!writeConfig.getMetadataConfig().isEnabled()) {
      return;
    }
    // Open up the metadata table again, for syncing
    try (HoodieTableMetadataWriter writer = JavaHoodieBackedTableMetadataWriter.create(storageConf, writeConfig, context, Option.empty())) {
      LOG.info("Successfully synced to metadata table");
    } catch (Exception e) {
      throw new HoodieMetadataException("Error syncing to metadata table.", e);
    }
  }

  protected HoodieTableMetadata metadata(HoodieWriteConfig clientConfig, HoodieEngineContext engineContext) {
    return HoodieTableMetadata.create(engineContext, metaClient.getStorage(), clientConfig.getMetadataConfig(), clientConfig.getBasePath());
  }

  /**
   * Validate the metadata tables contents to ensure it matches what is on the file system.
   */
  public void validateMetadata(HoodieTestTable testTable, List<String> inflightCommits, HoodieWriteConfig writeConfig,
                               String metadataTableBasePath, boolean doFullValidation) throws IOException {
    HoodieTableMetadata tableMetadata = metadata(writeConfig, context);
    assertNotNull(tableMetadata, "MetadataReader should have been initialized");
    if (!writeConfig.isMetadataTableEnabled()) {
      return;
    }

    if (tableMetadata instanceof FileSystemBackedTableMetadata || !tableMetadata.getSyncedInstantTime().isPresent()) {
      throw new IllegalStateException("Metadata should have synced some commits or tableMetadata should not be an instance "
          + "of FileSystemBackedTableMetadata");
    }
    assertEquals(inflightCommits, testTable.inflightCommits());

    HoodieTimer timer = HoodieTimer.start();
    HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(storageConf);

    // Partitions should match
    List<java.nio.file.Path> fsPartitionPaths = testTable.getAllPartitionPaths();
    List<String> fsPartitions = new ArrayList<>();
    fsPartitionPaths.forEach(entry -> fsPartitions.add(entry.getFileName().toString()));
    if (fsPartitions.isEmpty() && testTable.isNonPartitioned()) {
      fsPartitions.add("");
    }
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertEquals(fsPartitions, metadataPartitions, "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieJavaTable.create(writeConfig, engineContext);
    TableFileSystemView tableView = table.getHoodieView();
    List<String> fullPartitionPaths =
        fsPartitions.stream().map(partition -> basePath + "/" + partition)
            .collect(Collectors.toList());
    Map<String, List<StoragePathInfo>> partitionToFilesMap =
        tableMetadata.getAllFilesInPartitions(fullPartitionPaths);
    assertEquals(fsPartitions.size(), partitionToFilesMap.size());

    fsPartitions.forEach(partition -> {
      try {
        validateFilesPerPartition(testTable, tableMetadata, tableView, partitionToFilesMap,
            partition);
      } catch (IOException e) {
        fail("Exception should not be raised: " + e);
      }
    });
    if (doFullValidation) {
      runFullValidation(writeConfig, metadataTableBasePath, engineContext);
    }

    LOG.info("Validation time=" + timer.endTimer());
  }

  protected void validateFilesPerPartition(HoodieTestTable testTable,
                                           HoodieTableMetadata tableMetadata,
                                           TableFileSystemView tableView,
                                           Map<String, List<StoragePathInfo>> partitionToFilesMap,
                                           String partition) throws IOException {
    StoragePath partitionPath;
    if (partition.equals("")) {
      // Should be the non-partitioned case
      partitionPath = new StoragePath(basePath);
    } else {
      partitionPath = new StoragePath(basePath, partition);
    }

    FileStatus[] fsStatuses = testTable.listAllFilesInPartition(partition);
    List<StoragePathInfo> metaFilesList = tableMetadata.getAllFilesInPartition(partitionPath);
    List<String> fsFileNames = Arrays.stream(fsStatuses)
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    List<String> metadataFilenames = metaFilesList.stream()
        .map(s -> s.getPath().getName()).collect(Collectors.toList());
    Collections.sort(fsFileNames);
    Collections.sort(metadataFilenames);

    assertLinesMatch(fsFileNames, metadataFilenames);
    assertEquals(fsStatuses.length, partitionToFilesMap.get(partitionPath.toString()).size());

    // Block sizes should be valid
    metaFilesList.forEach(s -> assertTrue(s.getBlockSize() > 0));
    List<Long> fsBlockSizes = Arrays.stream(fsStatuses).map(FileStatus::getBlockSize).sorted().collect(Collectors.toList());
    List<Long> metadataBlockSizes = metaFilesList.stream().map(StoragePathInfo::getBlockSize).sorted().collect(Collectors.toList());
    assertEquals(fsBlockSizes, metadataBlockSizes);

    assertEquals(fsFileNames.size(), metadataFilenames.size(),
        "Files within partition " + partition + " should match");
    assertEquals(fsFileNames, metadataFilenames,
        "Files within partition " + partition + " should match");

    // FileSystemView should expose the same data
    List<HoodieFileGroup> fileGroups =
        tableView.getAllFileGroups(partition).collect(Collectors.toList());
    fileGroups.addAll(tableView.getAllReplacedFileGroups(partition).collect(Collectors.toList()));

    fileGroups.forEach(g -> LoggerFactory.getLogger(getClass()).info(g.toString()));
    fileGroups.forEach(g -> g.getAllBaseFiles()
        .forEach(b -> LoggerFactory.getLogger(getClass()).info(b.toString())));
    fileGroups.forEach(g -> g.getAllFileSlices()
        .forEach(s -> LoggerFactory.getLogger(getClass()).info(s.toString())));

    long numFiles = fileGroups.stream()
        .mapToLong(g -> g.getAllBaseFiles().count()
            + g.getAllFileSlices().mapToLong(s -> s.getLogFiles().count()).sum())
        .sum();
    assertEquals(metadataFilenames.size(), numFiles);
  }

  protected HoodieBackedTableMetadataWriter metadataWriter(HoodieWriteConfig clientConfig) {
    return (HoodieBackedTableMetadataWriter) JavaHoodieBackedTableMetadataWriter
        .create(storageConf, clientConfig, new HoodieJavaEngineContext(storageConf), Option.empty());
  }

  private void runFullValidation(HoodieWriteConfig writeConfig,
                                 String metadataTableBasePath,
                                 HoodieEngineContext engineContext) {
    try (HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(writeConfig)) {
      assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

      // Validate write config for metadata table
      HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
      assertFalse(metadataWriteConfig.isMetadataTableEnabled(), "No metadata table for metadata table");

      HoodieTableMetaClient metadataMetaClient = HoodieTestUtils.createMetaClient(storageConf, metadataTableBasePath);

      // Metadata table is MOR
      assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

      // Metadata table is HFile format
      assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
          "Metadata Table base file format should be HFile");

      // Metadata table has a fixed number of partitions
      // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
      // in the .hoodie folder.
      List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, storage, HoodieTableMetadata.getMetadataTableBasePath(basePath), false, false);

      List<MetadataPartitionType> enabledPartitionTypes = metadataWriter.getEnabledPartitionTypes();

      assertEquals(enabledPartitionTypes.size(), metadataTablePartitions.size());

      Map<String, MetadataPartitionType> partitionTypeMap = enabledPartitionTypes.stream()
          .collect(Collectors.toMap(MetadataPartitionType::getPartitionPath, Function.identity()));

      // Metadata table should automatically compact and clean
      // versions are +1 as autoClean / compaction happens end of commits
      int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient, metadataMetaClient.getActiveTimeline());
      metadataTablePartitions.forEach(partition -> {
        MetadataPartitionType partitionType = partitionTypeMap.get(partition);

        List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());

        assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).filter(Objects::nonNull).count() > 0, "Should have a single latest base file");
        assertTrue(latestSlices.size() > 0, "Should have a single latest file slice");
        assertTrue(latestSlices.size() <= numFileVersions, "Should limit file slice to "
            + numFileVersions + " but was " + latestSlices.size());
      });
    } catch (Exception e) {
      throw new RuntimeException("Error closing metadata writer", e);
    }
  }

  public HoodieJavaTable getHoodieTable(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    HoodieJavaTable table = HoodieJavaTable.create(config, context, metaClient);
    ((SyncableFileSystemView) (table.getSliceView())).reset();
    return table;
  }

  public List<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, HoodieJavaWriteClient client, String newCommitTime,
                                            String initCommitTime, int numRecordsInThisCommit,
                                            Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                            boolean assertForCommit, int expRecordsInThisCommit) throws Exception {
    return insertFirstBatch(writeConfig, client, newCommitTime, initCommitTime, numRecordsInThisCommit, writeFn, isPreppedAPI, assertForCommit, expRecordsInThisCommit, true);
  }

  /**
   * Helper to insert first batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for insertion
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public List<WriteStatus> insertFirstBatch(HoodieWriteConfig writeConfig, HoodieJavaWriteClient client, String newCommitTime,
                                            String initCommitTime, int numRecordsInThisCommit,
                                            Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                            boolean assertForCommit, int expRecordsInThisCommit, boolean filterForCommitTimeWithAssert) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateInserts);

    return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
        recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expRecordsInThisCommit, 1, false, filterForCommitTimeWithAssert);
  }

  /**
   * Helper to insert another batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param writeFn                Write Function to be used for insertion
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords        Expected number of records when scanned
   * @param expTotalCommits        Expected number of commits (including this commit)
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public List<WriteStatus> insertBatch(HoodieWriteConfig writeConfig, HoodieJavaWriteClient client, String newCommitTime,
                                       String initCommitTime, int numRecordsInThisCommit,
                                       Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                       boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, Option<String> partition) throws Exception {

    if (partition.isPresent()) {
      final Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction =
          generateWrapRecordsForPartitionFn(isPreppedAPI, writeConfig, dataGen::generateInsertsForPartition);

      return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
          recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, false,
          partition.get());
    } else {
      final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
          generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateInserts);

      return writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime, numRecordsInThisCommit,
          recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, false);
    }
  }

  public List<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, HoodieJavaWriteClient client, String newCommitTime,
                                       String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                       int numRecordsInThisCommit,
                                       Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                       boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits) throws Exception {
    return updateBatch(writeConfig, client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, writeFn,
        isPreppedAPI, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, true);
  }

  /**
   * Helper to upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param writeConfig                  Hoodie Write Config
   * @param client                       Hoodie Write Client
   * @param newCommitTime                New Commit Timestamp to be used
   * @param prevCommitTime               Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime               Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit       Number of records to be added in the new commit
   * @param writeFn                      Write Function to be used for upsert
   * @param isPreppedAPI                 Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit              Enable Assertion of Writes
   * @param expRecordsInThisCommit       Expected number of records in this commit
   * @param expTotalRecords              Expected number of records when scanned
   * @param expTotalCommits              Expected number of commits (including this commit)
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public List<WriteStatus> updateBatch(HoodieWriteConfig writeConfig, HoodieJavaWriteClient client, String newCommitTime,
                                       String prevCommitTime, Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                       int numRecordsInThisCommit,
                                       Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn, boolean isPreppedAPI,
                                       boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                       boolean filterForCommitTimeWithAssert) throws Exception {
    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueUpdates);

    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, recordGenFunction, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, false, filterForCommitTimeWithAssert);
  }

  public List<WriteStatus> deleteBatch(HoodieWriteConfig writeConfig, HoodieJavaWriteClient client, String newCommitTime, String prevCommitTime,
                                       String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI, boolean assertForCommit,
                                       int expRecordsInThisCommit, int expTotalRecords) throws Exception {
    return deleteBatch(writeConfig, client, newCommitTime, prevCommitTime, initCommitTime, numRecordsInThisCommit, isPreppedAPI,
        assertForCommit, expRecordsInThisCommit, expTotalRecords, true);
  }

  /**
   * Helper to delete batch of keys and do regular assertions on the state after successful completion.
   *
   * @param writeConfig            Hoodie Write Config
   * @param client                 Hoodie Write Client
   * @param newCommitTime          New Commit Timestamp to be used
   * @param prevCommitTime         Commit Timestamp used in previous commit
   * @param initCommitTime         Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit Number of records to be added in the new commit
   * @param isPreppedAPI           Boolean flag to indicate writeFn expects prepped records
   * @param assertForCommit        Enable Assertion of Writes
   * @param expRecordsInThisCommit Expected number of records in this commit
   * @param expTotalRecords        Expected number of records when scanned
   * @return RDD of write-status
   * @throws Exception in case of error
   */
  public List<WriteStatus> deleteBatch(HoodieWriteConfig writeConfig, HoodieJavaWriteClient client, String newCommitTime,
                                       String prevCommitTime, String initCommitTime, int numRecordsInThisCommit, boolean isPreppedAPI,
                                       boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, boolean filterForCommitTimeWithAssert) throws Exception {

    if (isPreppedAPI) {
      final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
          generateWrapRecordsFn(isPreppedAPI, writeConfig, dataGen::generateUniqueDeleteRecords);

      // Delete 1 (only deletes)
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> deleteRecords = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);

      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> deleteFn = HoodieJavaWriteClient::deletePrepped;
      List<WriteStatus> result = deleteFn.apply(client, deleteRecords, newCommitTime);
      return getWriteStatusAndVerifyDeleteOperation(newCommitTime, prevCommitTime, initCommitTime, assertForCommit, expRecordsInThisCommit, expTotalRecords, filterForCommitTimeWithAssert, result);
    } else {
      final Function<Integer, List<HoodieKey>> keyGenFunction =
          generateWrapDeleteKeysFn(isPreppedAPI, writeConfig, dataGen::generateUniqueDeletes);

      // Delete 1 (only deletes)
      client.startCommitWithTime(newCommitTime);

      List<HoodieKey> deleteRecords = keyGenFunction.apply(numRecordsInThisCommit);

      // check the partition metadata is written out
      assertPartitionMetadataForKeys(basePath, deleteRecords, storage);

      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieKey>, String> deleteFn = HoodieJavaWriteClient::delete;
      List<WriteStatus> result = deleteFn.apply(client, deleteRecords, newCommitTime);
      return getWriteStatusAndVerifyDeleteOperation(newCommitTime, prevCommitTime, initCommitTime, assertForCommit, expRecordsInThisCommit, expTotalRecords, filterForCommitTimeWithAssert, result);
    }
  }

  public List<WriteStatus> writeBatch(HoodieJavaWriteClient client, String newCommitTime, String prevCommitTime,
                                      Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn,
                                      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, boolean doCommit) throws Exception {
    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, doCommit, true);
  }

  public List<WriteStatus> writeBatch(HoodieJavaWriteClient client, String newCommitTime, String prevCommitTime,
                                      Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                      Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction,
                                      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn,
                                      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits,
                                      boolean doCommit, String partition) throws Exception {
    return writeBatch(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime, numRecordsInThisCommit, recordGenFunction,
        writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords, expTotalCommits, doCommit, true, partition);
  }

  /**
   * Helper to insert/upsert batch of records and do regular assertions on the state after successful completion.
   *
   * @param client                       Hoodie Write Client
   * @param newCommitTime                New Commit Timestamp to be used
   * @param prevCommitTime               Commit Timestamp used in previous commit
   * @param commitTimesBetweenPrevAndNew Sample of Timestamps between prevCommitTime and newCommitTime
   * @param initCommitTime               Begin Timestamp (usually "000")
   * @param numRecordsInThisCommit       Number of records to be added in the new commit
   * @param recordGenFunction            Records Generation Function
   * @param writeFn                      Write Function to be used for upsert
   * @param assertForCommit              Enable Assertion of Writes
   * @param expRecordsInThisCommit       Expected number of records in this commit
   * @param expTotalRecords              Expected number of records when scanned
   * @param expTotalCommits              Expected number of commits (including this commit)
   * @param doCommit
   * @throws Exception in case of error
   */
  public List<WriteStatus> writeBatch(HoodieJavaWriteClient client, String newCommitTime, String prevCommitTime,
                                      Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                      Function2<List<HoodieRecord>, String, Integer> recordGenFunction,
                                      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn,
                                      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, boolean doCommit,
                                      boolean filterForCommitTimeWithAssert) throws Exception {

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit);
    return writeBatchHelper(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, records, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, doCommit, filterForCommitTimeWithAssert);
  }

  public List<WriteStatus> writeBatch(HoodieJavaWriteClient client, String newCommitTime, String prevCommitTime,
                                      Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime, int numRecordsInThisCommit,
                                      Function3<List<HoodieRecord>, String, Integer, String> recordGenFunction,
                                      Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn,
                                      boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords, int expTotalCommits, boolean doCommit,
                                      boolean filterForCommitTimeWithAssert,
                                      String partition) throws Exception {

    List<HoodieRecord> records = recordGenFunction.apply(newCommitTime, numRecordsInThisCommit, partition);
    return writeBatchHelper(client, newCommitTime, prevCommitTime, commitTimesBetweenPrevAndNew, initCommitTime,
        numRecordsInThisCommit, records, writeFn, assertForCommit, expRecordsInThisCommit, expTotalRecords,
        expTotalCommits, doCommit, filterForCommitTimeWithAssert);
  }

  private List<WriteStatus> writeBatchHelper(HoodieJavaWriteClient client, String newCommitTime, String prevCommitTime,
                                             Option<List<String>> commitTimesBetweenPrevAndNew, String initCommitTime,
                                             int numRecordsInThisCommit, List<HoodieRecord> records,
                                             Function3<List<WriteStatus>, HoodieJavaWriteClient, List<HoodieRecord>, String> writeFn,
                                             boolean assertForCommit, int expRecordsInThisCommit, int expTotalRecords,
                                             int expTotalCommits, boolean doCommit, boolean filterForCommitTimeWithAssert) throws IOException {
    // Write 1 (only inserts)
    client.startCommitWithTime(newCommitTime);

    List<WriteStatus> result = writeFn.apply(client, records, newCommitTime);
    assertNoWriteErrors(result);

    if (doCommit) {
      client.commit(newCommitTime, result);
    }
    // check the partition metadata is written out
    assertPartitionMetadataForRecords(basePath, records, storage);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = createMetaClient();
    HoodieTimeline timeline = metaClient.getCommitsTimeline();

    if (assertForCommit) {
      assertEquals(expTotalCommits, timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants(),
          "Expecting " + expTotalCommits + " commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().getTimestamp(),
          "Latest commit should be " + newCommitTime);
      if (filterForCommitTimeWithAssert) { // when meta cols are disabled, we can't really do per commit assertion.
        assertEquals(expRecordsInThisCommit, numRowsInCommit(basePath, timeline, newCommitTime, true),
            "Must contain " + expRecordsInThisCommit + " records");
      }

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals(expTotalRecords, countRowsInPaths(basePath, storage, fullPartitionPaths),
          "Must contain " + expTotalRecords + " records");

      if (filterForCommitTimeWithAssert) {
        // Check that the incremental consumption from prevCommitTime
        assertEquals(numRowsInCommit(basePath, timeline, newCommitTime, true),
            countRecordsOptionallySince(basePath, timeline, Option.of(prevCommitTime)),
            "Incremental consumption from " + prevCommitTime + " should give all records in latest commit");
        if (commitTimesBetweenPrevAndNew.isPresent()) {
          commitTimesBetweenPrevAndNew.get().forEach(ct -> {
            assertEquals(numRowsInCommit(basePath, timeline, newCommitTime, true),
                countRecordsOptionallySince(basePath, timeline, Option.of(ct)),
                "Incremental consumption from " + ct + " should give all records in latest commit");
          });
        }
      }
    }
    return result;
  }

  /**
   * Generate wrapper for record generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig  Hoodie Write Config
   * @param wrapped      Actual Records Generation function
   * @return Wrapped Function
   */
  public Function2<List<HoodieRecord>, String, Integer> generateWrapRecordsFn(boolean isPreppedAPI,
                                                                              HoodieWriteConfig writeConfig,
                                                                              Function2<List<HoodieRecord>, String, Integer> wrapped) {
    if (isPreppedAPI) {
      return wrapRecordsGenFunctionForPreppedCalls(basePath, storageConf, context, writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  /**
   * Generate wrapper for record generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig  Hoodie Write Config
   * @param wrapped      Actual Records Generation function (for partition)
   * @return Wrapped Function
   */
  public Function3<List<HoodieRecord>, String, Integer, String> generateWrapRecordsForPartitionFn(boolean isPreppedAPI,
                                                                                                  HoodieWriteConfig writeConfig, Function3<List<HoodieRecord>, String, Integer, String> wrapped) {
    if (isPreppedAPI) {
      return wrapPartitionRecordsGenFunctionForPreppedCalls(basePath, storageConf, context, writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  /**
   * Helper to generate records generation function for testing Prepped version of API. Prepped APIs expect the records
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by record-generation function itself.
   *
   * @param writeConfig        Hoodie Write Config
   * @param recordsGenFunction Records Generation function
   * @return Wrapped function
   */
  public static Function2<List<HoodieRecord>, String, Integer> wrapRecordsGenFunctionForPreppedCalls(
      final String basePath,
      final StorageConfiguration<Configuration> storageConf,
      final HoodieEngineContext context,
      final HoodieWriteConfig writeConfig,
      final Function2<List<HoodieRecord>, String, Integer> recordsGenFunction) {
    return (commit, numRecords) -> {
      final HoodieIndex index = JavaHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieRecord> records = recordsGenFunction.apply(commit, numRecords);
      final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
      HoodieJavaTable table = HoodieJavaTable.create(writeConfig, context, metaClient);
      return tagLocation(index, context, records, table);
    };
  }

  /**
   * Helper to generate records generation function for testing Prepped version of API. Prepped APIs expect the records
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by record-generation function itself.
   *
   * @param writeConfig        Hoodie Write Config
   * @param recordsGenFunction Records Generation function (for partition)
   * @return Wrapped function
   */
  public static Function3<List<HoodieRecord>, String, Integer, String> wrapPartitionRecordsGenFunctionForPreppedCalls(
      final String basePath,
      final StorageConfiguration<Configuration> storageConf,
      final HoodieEngineContext context,
      final HoodieWriteConfig writeConfig,
      final Function3<List<HoodieRecord>, String, Integer, String> recordsGenFunction) {
    return (commit, numRecords, partition) -> {
      final HoodieIndex index = JavaHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieRecord> records = recordsGenFunction.apply(commit, numRecords, partition);
      final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
      HoodieJavaTable table = HoodieJavaTable.create(writeConfig, context, metaClient);
      return tagLocation(index, context, records, table);
    };
  }

  /**
   * Generate wrapper for delete key generation function for testing Prepped APIs.
   *
   * @param isPreppedAPI Flag to indicate if this is for testing prepped-version of APIs
   * @param writeConfig  Hoodie Write Config
   * @param wrapped      Actual Records Generation function
   * @return Wrapped Function
   */
  public Function<Integer, List<HoodieKey>> generateWrapDeleteKeysFn(boolean isPreppedAPI,
                                                                     HoodieWriteConfig writeConfig, Function<Integer, List<HoodieKey>> wrapped) {
    if (isPreppedAPI) {
      return wrapDeleteKeysGenFunctionForPreppedCalls(basePath, storageConf, context, writeConfig, wrapped);
    } else {
      return wrapped;
    }
  }

  /**
   * Helper to generate delete keys generation function for testing Prepped version of API. Prepped APIs expect the keys
   * to be already de-duped and have location set. This wrapper takes care of record-location setting. Uniqueness is
   * guaranteed by key-generation function itself.
   *
   * @param writeConfig    Hoodie Write Config
   * @param keyGenFunction Keys Generation function
   * @return Wrapped function
   */
  public static Function<Integer, List<HoodieKey>> wrapDeleteKeysGenFunctionForPreppedCalls(
      final String basePath,
      final StorageConfiguration<Configuration> storageConf,
      final HoodieEngineContext context,
      final HoodieWriteConfig writeConfig,
      final Function<Integer, List<HoodieKey>> keyGenFunction) {
    return (numRecords) -> {
      final HoodieIndex index = JavaHoodieIndexFactory.createIndex(writeConfig);
      List<HoodieKey> records = keyGenFunction.apply(numRecords);
      final HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, basePath);
      HoodieTable table = HoodieJavaTable.create(writeConfig, context, metaClient);
      List<HoodieRecord> recordsToDelete = records.stream()
          .map(key -> new HoodieAvroRecord(key, new EmptyHoodieRecordPayload())).collect(Collectors.toList());
      List<HoodieRecord> taggedRecords = tagLocation(index, context, recordsToDelete, table);
      return taggedRecords.stream().map(record -> record.getKey()).collect(Collectors.toList());
    };
  }

  public static List<HoodieRecord> tagLocation(
      HoodieIndex index, HoodieEngineContext context, List<HoodieRecord> records, HoodieTable table) {
    return index.tagLocation(HoodieListData.eager(records), context, table).collectAsList();
  }

  private List<WriteStatus> getWriteStatusAndVerifyDeleteOperation(String newCommitTime, String prevCommitTime, String initCommitTime, boolean assertForCommit, int expRecordsInThisCommit,
                                                                   int expTotalRecords, boolean filerForCommitTimeWithAssert, List<WriteStatus> result) {
    assertNoWriteErrors(result);

    // verify that there is a commit
    HoodieTableMetaClient metaClient = createMetaClient();
    HoodieTimeline timeline = new HoodieActiveTimeline(metaClient).getCommitAndReplaceTimeline();

    if (assertForCommit) {
      assertEquals(3, timeline.findInstantsAfter(initCommitTime, Integer.MAX_VALUE).countInstants(),
          "Expecting 3 commits.");
      assertEquals(newCommitTime, timeline.lastInstant().get().getTimestamp(),
          "Latest commit should be " + newCommitTime);
      if (filerForCommitTimeWithAssert) { // if meta cols are disabled, we can't do assertion based on assertion time
        assertEquals(expRecordsInThisCommit, numRowsInCommit(basePath, timeline, newCommitTime, true),
            "Must contain " + expRecordsInThisCommit + " records");
      }

      // Check the entire dataset has all records still
      String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
      for (int i = 0; i < fullPartitionPaths.length; i++) {
        fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
      }
      assertEquals(expTotalRecords, countRowsInPaths(basePath, storage, fullPartitionPaths),
          "Must contain " + expTotalRecords + " records");

      if (filerForCommitTimeWithAssert) {
        // Check that the incremental consumption from prevCommitTime
        assertEquals(numRowsInCommit(basePath, timeline, newCommitTime, true),
            countRecordsOptionallySince(basePath, timeline, Option.of(prevCommitTime)),
            "Incremental consumption from " + prevCommitTime + " should give no records in latest commit,"
                + " since it is a delete operation");
      }
    }
    return result;
  }

  public long numRowsInCommit(String basePath, HoodieTimeline commitTimeline,
                              String instantTime, boolean filterByCommitTime) {
    HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, instantTime);
    if (!commitTimeline.containsInstant(commitInstant)) {
      throw new HoodieException("No commit exists at " + instantTime);
    }
    try {
      HashMap<String, String> paths =
          getLatestFileIDsToFullPath(basePath, commitTimeline, Arrays.asList(commitInstant));
      return paths.values().stream().map(StoragePath::new).flatMap(path ->
              HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(path)
                  .readAvroRecords(storage, path).stream())
          .filter(record -> {
            if (filterByCommitTime) {
              Object commitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
              return commitTime != null && commitTime.toString().equals(instantTime);
            } else {
              return true;
            }
          }).count();
    } catch (Exception e) {
      throw new HoodieException("Error reading commit " + instantTime, e);
    }
  }

  private static HashMap<String, String> getLatestFileIDsToFullPath(String basePath, HoodieTimeline commitTimeline,
                                                                    List<HoodieInstant> commitsToReturn) throws IOException {
    HashMap<String, String> fileIdToFullPath = new HashMap<>();
    for (HoodieInstant commit : commitsToReturn) {
      HoodieCommitMetadata metadata =
          HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit).get(), HoodieCommitMetadata.class);
      fileIdToFullPath.putAll(metadata.getFileIdAndFullPaths(new StoragePath(basePath)));
    }
    return fileIdToFullPath;
  }

  public long countRowsInPaths(String basePath, HoodieStorage storage, String... paths) {
    try {
      List<HoodieBaseFile> latestFiles = getLatestBaseFiles(basePath, storage, paths);
      return latestFiles.stream().mapToLong(baseFile ->
              HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(baseFile.getStoragePath())
                  .readAvroRecords(storage, baseFile.getStoragePath()).size())
          .sum();
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
  }

  public static List<HoodieBaseFile> getLatestBaseFiles(String basePath, HoodieStorage storage,
                                                        String... paths) {
    List<HoodieBaseFile> latestFiles = new ArrayList<>();
    try {
      HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage, basePath);
      for (String path : paths) {
        TableFileSystemView.BaseFileOnlyView fileSystemView =
            new HoodieTableFileSystemView(metaClient,
                metaClient.getCommitsTimeline().filterCompletedInstants(),
                storage.globEntries(new StoragePath(path)));
        latestFiles.addAll(fileSystemView.getLatestBaseFiles().collect(Collectors.toList()));
      }
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
    return latestFiles;
  }

  /**
   * Obtain all new data written into the Hoodie table with an optional from timestamp.
   */
  public long countRecordsOptionallySince(String basePath, HoodieTimeline commitTimeline, Option<String> lastCommitTimeOpt) {
    List<HoodieInstant> commitsToReturn =
        lastCommitTimeOpt.isPresent() ? commitTimeline.findInstantsAfter(lastCommitTimeOpt.get(), Integer.MAX_VALUE).getInstants() :
            commitTimeline.getInstants();
    try {
      // Go over the commit metadata, and obtain the new files that need to be read.
      HashMap<String, String> fileIdToFullPath = getLatestFileIDsToFullPath(basePath, commitTimeline, commitsToReturn);
      String[] paths = fileIdToFullPath.values().toArray(new String[fileIdToFullPath.size()]);
      if (paths[0].endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
        return Arrays.stream(paths).map(StoragePath::new).flatMap(path ->
                HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(path)
                    .readAvroRecords(storage, path).stream())
            .filter(record -> {
              if (lastCommitTimeOpt.isPresent()) {
                Object commitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
                return commitTime != null && commitTime.toString().compareTo(lastCommitTimeOpt.get()) > 0;
              } else {
                return true;
              }
            }).count();
      } else if (paths[0].endsWith(HoodieFileFormat.HFILE.getFileExtension())) {
        Stream<GenericRecord> genericRecordStream = readHFile(context.getStorageConf().unwrapAs(Configuration.class), paths);
        if (lastCommitTimeOpt.isPresent()) {
          return genericRecordStream.filter(gr -> HoodieTimeline.compareTimestamps(lastCommitTimeOpt.get(), HoodieActiveTimeline.LESSER_THAN,
                  gr.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()))
              .count();
        } else {
          return genericRecordStream.count();
        }
      }
      throw new HoodieException("Unsupported base file format for file :" + paths[0]);
    } catch (IOException e) {
      throw new HoodieException("Error pulling data incrementally from commitTimestamp :" + lastCommitTimeOpt.get(), e);
    }
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr, HoodieIndex.IndexType indexType,
                                                    HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withEngineType(EngineType.JAVA)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(cleaningPolicy).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .forTable(RAW_TRIPS_TEST_NAME)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(false).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort).build());
    if (StringUtils.nonEmpty(schemaStr)) {
      builder.withSchema(schemaStr);
    }
    return builder;
  }

  protected HoodieTableMetaClient createMetaClient() {
    return HoodieTestUtils.createMetaClient(storageConf, basePath);
  }
}
