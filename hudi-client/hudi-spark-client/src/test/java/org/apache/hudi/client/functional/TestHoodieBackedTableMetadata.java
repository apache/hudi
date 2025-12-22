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

package org.apache.hudi.client.functional;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataLogRecordReader;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;
import static org.apache.hudi.common.model.WriteOperationType.COMPACT;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestHoodieBackedTableMetadata extends TestHoodieMetadataBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieBackedTableMetadata.class);

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTableOperations(boolean reuseReaders) throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    init(tableType);
    doWriteInsertAndUpsert(testTable);

    // trigger an upsert
    doWriteOperation(testTable, "0000003");
    verifyBaseMetadataTable(reuseReaders);
  }

  /**
   * Create a cow table and call getAllFilesInPartition api in parallel which reads data files from MDT
   * This UT is guard that multi readers for MDT#getAllFilesInPartition api is safety.
   * @param reuse
   * @throws Exception
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMultiReaderForHoodieBackedTableMetadata(boolean reuse) throws Exception {
    final int taskNumber = 3;
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    init(tableType);
    testTable.doWriteOperation("000001", INSERT, emptyList(), asList("p1"), 1);
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), reuse);
    assertTrue(tableMetadata.enabled());
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();
    String partition = metadataPartitions.get(0);
    String finalPartition = basePath + "/" + partition;
    ExecutorService executors = Executors.newFixedThreadPool(taskNumber);
    AtomicBoolean flag = new AtomicBoolean(false);
    CountDownLatch downLatch = new CountDownLatch(taskNumber);
    AtomicInteger filesNumber = new AtomicInteger(0);

    // call getAllFilesInPartition api from meta data table in parallel
    for (int i = 0; i < taskNumber; i++) {
      executors.submit(new Runnable() {
        @Override
        public void run() {
          try {
            downLatch.countDown();
            downLatch.await();
            List<StoragePathInfo> files =
                tableMetadata.getAllFilesInPartition(new StoragePath(finalPartition));
            if (files.size() != 1) {
              LOG.warn("Miss match data file numbers.");
              throw new RuntimeException("Miss match data file numbers.");
            }
            filesNumber.addAndGet(files.size());
          } catch (Exception e) {
            LOG.warn("Catch Exception while reading data files from MDT.", e);
            flag.compareAndSet(false, true);
          }
        }
      });
    }
    executors.shutdown();
    executors.awaitTermination(5, TimeUnit.MINUTES);
    assertFalse(flag.get());
    assertEquals(filesNumber.get(), taskNumber);
  }

  private void doWriteInsertAndUpsert(HoodieTestTable testTable) throws Exception {
    doWriteInsertAndUpsert(testTable, "0000001", "0000002", false);
  }

  private void verifyBaseMetadataTable(boolean reuseMetadataReaders) throws IOException {
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(
        context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), reuseMetadataReaders);
    assertTrue(tableMetadata.enabled());
    List<java.nio.file.Path> fsPartitionPaths = testTable.getAllPartitionPaths();
    List<String> fsPartitions = new ArrayList<>();
    fsPartitionPaths.forEach(entry -> fsPartitions.add(entry.getFileName().toString()));
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertEquals(fsPartitions, metadataPartitions, "Partitions should match");

    // Files within each partition should match
    HoodieTable table = HoodieSparkTable.create(writeConfig, context);
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
  }

  /**
   * Verify if the Metadata table is constructed with table properties including
   * the right key generator class name.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataTableKeyGenerator(final HoodieTableType tableType) throws Exception {
    init(tableType);

    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), false);

    assertEquals(HoodieTableMetadataKeyGenerator.class.getCanonicalName(),
        tableMetadata.getMetadataMetaClient().getTableConfig().getKeyGeneratorClassName());
  }

  /**
   * [HUDI-2852] Table metadata returns empty for non-exist partition.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testNotExistPartition(final HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), false);
    List<StoragePathInfo> allFilesInPartition = tableMetadata.getAllFilesInPartition(
        new StoragePath(writeConfig.getBasePath() + "dummy"));
    assertEquals(allFilesInPartition.size(), 0);
  }

  /**
   * 1. Verify metadata table records key deduplication feature. When record key
   * deduplication is enabled, verify the metadata record payload on disk has empty key.
   * Otherwise, verify the valid key.
   * 2. Verify populate meta fields work irrespective of record key deduplication config.
   * 3. Verify table services like compaction benefit from record key deduplication feature.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testMetadataRecordKeyExcludeFromPayload(final HoodieTableType tableType) throws Exception {
    initPath();
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(3)
            .build())
        .build();
    init(tableType, writeConfig);

    // 2nd commit
    doWriteOperation(testTable, "0000001", INSERT);

    final HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    HoodieWriteConfig metadataTableWriteConfig = getMetadataWriteConfig(writeConfig);
    metadataMetaClient.reloadActiveTimeline();
    final HoodieTable table = HoodieSparkTable.create(metadataTableWriteConfig, context, metadataMetaClient);

    // Compaction has not yet kicked in. Verify all the log files
    // for the metadata records persisted on disk as per the config.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000001");
    }, "Metadata table should have valid log files!");

    verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);

    // 2 more commits
    doWriteOperation(testTable, "0000002", UPSERT);
    doWriteOperation(testTable, "0000004", UPSERT);

    // Compaction should be triggered by now. Let's verify the log files
    // if any for the metadata records persisted on disk as per the config.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000002");
    }, "Metadata table should have valid log files!");

    // Verify the base file created by the just completed compaction.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);
    }, "Metadata table should have a valid base file!");

    // 2 more commits to trigger one more compaction, along with a clean
    doWriteOperation(testTable, "0000005", UPSERT);
    doClean(testTable, "0000006", Arrays.asList("0000004"));
    doWriteOperation(testTable, "0000007", UPSERT);

    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "7");
    }, "Metadata table should have valid log files!");

    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);
    }, "Metadata table should have a valid base file!");

    validateMetadata(testTable);
  }

  /**
   * This tests the case where the two clean actions delete the same file and commit
   * to the metadata table. The metadata table should not contain the deleted file afterwards.
   * A new cleaner plan may contain the same file to delete if the previous cleaner
   * plan has not been successfully executed before the new one is scheduled.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testRepeatedCleanActionsWithMetadataTableEnabled(final HoodieTableType tableType) throws Exception {
    initPath();
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build())
        .build();
    init(tableType, writeConfig);
    String partition = "p1";
    // Simulate two bulk insert operations adding two data files in partition "p1"
    String instant1 = HoodieActiveTimeline.createNewInstantTime();
    HoodieCommitMetadata commitMetadata1 =
        testTable.doWriteOperation(instant1, BULK_INSERT, emptyList(), asList(partition), 1);
    String instant2 = HoodieActiveTimeline.createNewInstantTime();
    HoodieCommitMetadata commitMetadata2 =
        testTable.doWriteOperation(instant2, BULK_INSERT, emptyList(), asList(partition), 1);

    final HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    while (getNumCompactions(metadataMetaClient) == 0) {
      // Write until the compaction happens in the metadata table
      testTable.doWriteOperation(
          HoodieActiveTimeline.createNewInstantTime(), BULK_INSERT, emptyList(), asList(partition), 1);
      metadataMetaClient.reloadActiveTimeline();
    }

    assertEquals(1, getNumCompactions(metadataMetaClient));

    List<String> fileIdsToReplace = new ArrayList<>();
    fileIdsToReplace.addAll(commitMetadata1.getFileIdAndRelativePaths().keySet());
    fileIdsToReplace.addAll(commitMetadata2.getFileIdAndRelativePaths().keySet());
    // Simulate clustering operation replacing two data files with a new data file
    testTable.doCluster(
        HoodieActiveTimeline.createNewInstantTime(),
        Collections.singletonMap(partition, fileIdsToReplace), asList(partition), 1);
    Set<String> fileSetBeforeCleaning = getFilePathsInPartition(partition);

    // Simulate two clean actions deleting the same set of date files
    // based on the first two commits
    String cleanInstant = HoodieActiveTimeline.createNewInstantTime();
    HoodieCleanMetadata cleanMetadata = testTable.doCleanBasedOnCommits(cleanInstant, asList(instant1, instant2));
    List<String> deleteFileList = cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns();
    assertTrue(deleteFileList.size() > 0);

    Set<String> fileSetAfterFirstCleaning = getFilePathsInPartition(partition);
    validateFilesAfterCleaning(deleteFileList, fileSetBeforeCleaning, fileSetAfterFirstCleaning);

    metaClient.reloadActiveTimeline();
    HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(
        metaClient, new HoodieInstant(HoodieInstant.State.REQUESTED, CLEAN_ACTION, cleanInstant));
    testTable.repeatClean(HoodieActiveTimeline.createNewInstantTime(), cleanerPlan, cleanMetadata);

    // Compaction should not happen after the first compaction in this test case
    assertEquals(1, getNumCompactions(metadataMetaClient));
    Set<String> fileSetAfterSecondCleaning = getFilePathsInPartition(partition);
    validateFilesAfterCleaning(deleteFileList, fileSetBeforeCleaning, fileSetAfterSecondCleaning);
  }

  private int getNumCompactions(HoodieTableMetaClient metaClient) {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    return timeline
        .filter(s -> {
          try {
            return s.getAction().equals(HoodieTimeline.COMMIT_ACTION)
                && HoodieCommitMetadata.fromBytes(
                    timeline.getInstantDetails(s).get(), HoodieCommitMetadata.class)
                .getOperationType().equals(COMPACT);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .countInstants();
  }

  private Set<String> getFilePathsInPartition(String partition) throws IOException {
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(storageConf),
        storage,
        HoodieMetadataConfig.newBuilder().enable(true).build(),
        basePath);
    return tableMetadata.getAllFilesInPartition(new StoragePath(basePath, partition))
        .stream()
        .map(status -> status.getPath().getName()).collect(Collectors.toSet());
  }

  private void validateFilesAfterCleaning(List<String> deleteFileList,
                                          Set<String> fileSetBeforeCleaning,
                                          Set<String> fileSetAfterCleaning) {
    assertEquals(deleteFileList.size(), fileSetBeforeCleaning.size() - fileSetAfterCleaning.size());
    for (String deleteFile : deleteFileList) {
      assertFalse(fileSetAfterCleaning.contains(deleteFile));
    }
    for (String file : fileSetAfterCleaning) {
      assertTrue(fileSetBeforeCleaning.contains(file));
    }
  }

  /**
   * Verify the metadata table log files for the record field correctness. On disk format
   * should be based on meta fields and key deduplication config. And the in-memory merged
   * records should all be materialized fully irrespective of the config.
   *
   * @param table                 - Hoodie metadata test table
   * @param metadataMetaClient    - Metadata meta client
   * @param latestCommitTimestamp - Latest commit timestamp
   * @throws IOException
   */
  private void verifyMetadataRecordKeyExcludeFromPayloadLogFiles(HoodieTable table, HoodieTableMetaClient metadataMetaClient,
                                                                 String latestCommitTimestamp) throws IOException {
    table.getHoodieView().sync();

    // Compaction should not be triggered yet. Let's verify no base file
    // and few log files available.
    List<FileSlice> fileSlices = table.getSliceView()
        .getLatestFileSlices(FILES.getPartitionPath()).collect(Collectors.toList());
    if (fileSlices.isEmpty()) {
      throw new IllegalStateException("LogFile slices are not available!");
    }

    // Verify the log files honor the key deduplication and virtual keys config
    List<HoodieLogFile> logFiles = fileSlices.get(0).getLogFiles().map(logFile -> {
      return logFile;
    }).collect(Collectors.toList());

    List<String> logFilePaths = logFiles.stream().map(logFile -> {
      return logFile.getPath().toString();
    }).collect(Collectors.toList());

    // Verify the on-disk raw records before they get materialized
    verifyMetadataRawRecords(table, logFiles);

    // Verify the in-memory materialized and merged records
    verifyMetadataMergedRecords(metadataMetaClient, logFilePaths, latestCommitTimestamp);
  }

  /**
   * Verify the metadata table on-disk raw records. When populate meta fields is enabled,
   * these records should have additional meta fields in the payload. When key deduplication
   * is enabled, these records on the disk should have key in the payload as empty string.
   *
   * @param table
   * @param logFiles - Metadata table log files to be verified
   * @throws IOException
   */
  private void verifyMetadataRawRecords(HoodieTable table, List<HoodieLogFile> logFiles) throws IOException {
    for (HoodieLogFile logFile : logFiles) {
      List<StoragePathInfo> pathInfoList = storage.listDirectEntries(logFile.getPath());
      Schema writerSchema  =
          TableSchemaResolver.readSchemaFromLogFile(storage, logFile.getPath());
      if (writerSchema == null) {
        // not a data block
        continue;
      }

      try (HoodieLogFormat.Reader logFileReader = HoodieLogFormat.newReader(storage,
          new HoodieLogFile(pathInfoList.get(0).getPath()), writerSchema)) {
        while (logFileReader.hasNext()) {
          HoodieLogBlock logBlock = logFileReader.next();
          if (logBlock instanceof HoodieDataBlock) {
            try (
                ClosableIterator<HoodieRecord<IndexedRecord>> recordItr = ((HoodieDataBlock) logBlock).getRecordIterator(
                    HoodieRecordType.AVRO)) {
              recordItr.forEachRemaining(indexRecord -> {
                final GenericRecord record = (GenericRecord) indexRecord.getData();
                assertNull(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
                assertNull(record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
                final String key = String.valueOf(record.get(HoodieMetadataPayload.KEY_FIELD_NAME));
                assertFalse(key.isEmpty());
              });
            }
          }
        }
      }
    }
  }

  /**
   * Verify the metadata table in-memory merged records. Irrespective of key deduplication
   * config, the in-memory merged records should always have the key field in the record
   * payload fully materialized.
   *
   * @param metadataMetaClient    - Metadata table meta client
   * @param logFilePaths          - Metadata table log file paths
   * @param latestCommitTimestamp - Latest commit timestamp
   */
  private void verifyMetadataMergedRecords(HoodieTableMetaClient metadataMetaClient, List<String> logFilePaths, String latestCommitTimestamp) {
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    HoodieMetadataLogRecordReader logRecordReader = HoodieMetadataLogRecordReader.newBuilder()
        .withStorage(metadataMetaClient.getStorage())
        .withBasePath(metadataMetaClient.getBasePath())
        .withLogFilePaths(logFilePaths)
        .withLatestInstantTime(latestCommitTimestamp)
        .withPartition(FILES.getPartitionPath())
        .withReaderSchema(schema)
        .withMaxMemorySizeInBytes(100000L)
        .withBufferSize(4096)
        .withSpillableMapBasePath(tempDir.toString())
        .withDiskMapType(ExternalSpillableMap.DiskMapType.BITCASK)
        .build();

    for (HoodieRecord<?> entry : logRecordReader.getRecords()) {
      assertFalse(entry.getRecordKey().isEmpty());
      assertEquals(entry.getKey().getRecordKey(), entry.getRecordKey());
    }
  }

  /**
   * Verify metadata table base files for the records persisted based on the config. When
   * the key deduplication is enabled, the records persisted on the disk in the base file
   * should have key field in the payload as empty string.
   *
   * @param table - Metadata table
   */
  private void verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(HoodieTable table) throws IOException {
    table.getHoodieView().sync();
    List<FileSlice> fileSlices = table.getSliceView()
        .getLatestFileSlices(FILES.getPartitionPath()).collect(Collectors.toList());
    if (!fileSlices.get(0).getBaseFile().isPresent()) {
      throw new IllegalStateException("Base file not available!");
    }
    final HoodieBaseFile baseFile = fileSlices.get(0).getBaseFile().get();

    HoodieAvroHFileReaderImplBase hoodieHFileReader = (HoodieAvroHFileReaderImplBase)
        getHoodieSparkIOFactory(storage)
            .getReaderFactory(HoodieRecordType.AVRO)
            .getFileReader(table.getConfig(), new StoragePath(baseFile.getPath()));
    List<IndexedRecord> records = HoodieAvroHFileReaderImplBase.readAllRecords(hoodieHFileReader);
    records.forEach(entry -> {
      assertNull(((GenericRecord) entry).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      final String keyInPayload = (String) ((GenericRecord) entry)
          .get(HoodieMetadataPayload.KEY_FIELD_NAME);
      assertFalse(keyInPayload.isEmpty());
    });
  }
}
