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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;
import static org.apache.hudi.common.model.WriteOperationType.COMPACT;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class TestHoodieBackedTableMetadata extends TestHoodieMetadataBase {

  public static List<Arguments> testTableOperationsArgs() {
    return Arrays.asList(
        Arguments.of(true, 6),
        Arguments.of(true, 8),
        Arguments.of(true, HoodieTableVersion.current().versionCode()),
        Arguments.of(false, 6),
        Arguments.of(false, 8),
        Arguments.of(false, HoodieTableVersion.current().versionCode())
    );
  }

  @ParameterizedTest
  @MethodSource("testTableOperationsArgs")
  public void testTableOperations(boolean reuseReaders, int tableVersion) throws Exception {
    HoodieTableType tableType = COPY_ON_WRITE;
    initPath();
    HoodieWriteConfig config = getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, true, false)
        .build();
    config.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(tableVersion));
    init(tableType, config);
    doWriteInsertAndUpsert(testTable);

    // trigger an upsert
    doWriteOperation(testTable, "0000003");
    verifyBaseMetadataTable(reuseReaders);
    HoodieTableVersion finalTableVersion = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
        .getTableConfig().getTableVersion();
    assertEquals(tableVersion, finalTableVersion.versionCode());
  }

  /**
   * Create a cow table and call getAllFilesInPartition api in parallel which reads data files from MDT
   * This UT is guard that multi readers for MDT#getAllFilesInPartition api is safety.
   * @param reuse
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("testTableOperationsArgs")
  public void testMultiReaderForHoodieBackedTableMetadata(boolean reuse, int tableVersion) throws Exception {
    final int taskNumber = 18;
    HoodieTableType tableType = COPY_ON_WRITE;
    initPath();
    HoodieWriteConfig config = getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, true, false)
        .build();
    config.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(tableVersion));
    init(tableType, config);
    testTable.doWriteOperation("000001", INSERT, emptyList(), asList("p1"), 1);
    testTable.doWriteOperation("000002", INSERT, emptyList(), asList("p2"), 2);
    testTable.doWriteOperation("000003", INSERT, emptyList(), asList("p3"), 3);
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context, storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), reuse);
    assertTrue(tableMetadata.enabled());
    ExecutorService executors = Executors.newFixedThreadPool(taskNumber);
    AtomicBoolean flag = new AtomicBoolean(false);
    CountDownLatch downLatch = new CountDownLatch(taskNumber);
    AtomicInteger filesNumber = new AtomicInteger(0);

    // call getAllFilesInPartition api from metadata table in parallel across different partitions
    // when reuse is true, we will reuse the same buffer of merged log records so we must ensure this works in a multithreaded environment
    for (int i = 0; i < taskNumber; i++) {
      final int partitionNumber = (i % 3) + 1;
      executors.submit(() -> {
        try {
          String finalPartition = basePath + "/p" + partitionNumber;
          downLatch.countDown();
          downLatch.await();
          List<StoragePathInfo> files =
              tableMetadata.getAllFilesInPartition(new StoragePath(finalPartition));
          // p1 has 1 file, p2 has 2 files, p3 has 3 files for convenience
          if (files.size() != partitionNumber) {
            throw new RuntimeException("Miss match data file numbers for partition: " + finalPartition + ", expected: " + partitionNumber + ", actual: " + files.size());
          }
          filesNumber.addAndGet(files.size());
        } catch (Exception e) {
          log.warn("Catch Exception while reading data files from MDT.", e);
          flag.compareAndSet(false, true);
        }
      });
    }
    executors.shutdown();
    executors.awaitTermination(5, TimeUnit.MINUTES);
    assertFalse(flag.get());
    assertEquals(36, filesNumber.get()); // 3 files for p3, 2 files for p2, 1 file for p1 and two readers for each partition

    // validate table version
    HoodieTableVersion finalTableVersion = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
        .getTableConfig().getTableVersion();
    assertEquals(tableVersion, finalTableVersion.versionCode());
    tableMetadata.close();
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

  public static List<Arguments> testMetadataTableKeyGeneratorArgs() {
    return Arrays.asList(
        Arguments.of(COPY_ON_WRITE, 6),
        Arguments.of(COPY_ON_WRITE, 8),
        Arguments.of(COPY_ON_WRITE, HoodieTableVersion.current().versionCode()),
        Arguments.of(MERGE_ON_READ, 6),
        Arguments.of(MERGE_ON_READ, 8),
        Arguments.of(MERGE_ON_READ, HoodieTableVersion.current().versionCode())
    );
  }

  /**
   * Verify if the Metadata table is constructed with table properties including
   * the right key generator class name.
   */
  @ParameterizedTest
  @MethodSource("testMetadataTableKeyGeneratorArgs")
  public void testMetadataTableKeyGenerator(final HoodieTableType tableType, int tableVersion) throws Exception {
    initPath();
    HoodieWriteConfig config = getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, true, false)
        .build();
    config.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(tableVersion));
    init(tableType, config);

    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), false);

    assertEquals(HoodieTableMetadataKeyGenerator.class.getCanonicalName(),
        tableMetadata.getMetadataMetaClient().getTableConfig().getKeyGeneratorClassName());

    // validate table version
    HoodieTableVersion finalTableVersion = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
        .getTableConfig().getTableVersion();
    assertEquals(tableVersion, finalTableVersion.versionCode());
  }

  /**
   * [HUDI-2852] Table metadata returns empty for non-exist partition.
   */
  @ParameterizedTest
  @MethodSource("testMetadataTableKeyGeneratorArgs")
  public void testNotExistPartition(final HoodieTableType tableType, int tableVersion) throws Exception {
    initPath();
    HoodieWriteConfig config = getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true, true, false, true, false)
        .build();
    config.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(tableVersion));
    init(tableType, config);
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(context,
        storage, writeConfig.getMetadataConfig(), writeConfig.getBasePath(), false);
    List<StoragePathInfo> allFilesInPartition = tableMetadata.getAllFilesInPartition(
        new StoragePath(writeConfig.getBasePath() + "dummy"));
    assertEquals(allFilesInPartition.size(), 0);

    // validate table version
    HoodieTableVersion finalTableVersion = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
        .getTableConfig().getTableVersion();
    assertEquals(tableVersion, finalTableVersion.versionCode());
  }

  /**
   * 1. Verify metadata table records key deduplication feature. When record key
   * deduplication is enabled, verify the metadata record payload on disk has empty key.
   * Otherwise, verify the valid key.
   * 2. Verify populate meta fields work irrespective of record key deduplication config.
   * 3. Verify table services like compaction benefit from record key deduplication feature.
   */
  @ParameterizedTest
  @MethodSource("testMetadataTableKeyGeneratorArgs")
  public void testMetadataRecordKeyExcludeFromPayload(final HoodieTableType tableType, int tableVersion) throws Exception {
    initPath();
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(3)
            .build())
        .build();
    writeConfig.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(tableVersion));
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
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000001", metadataTableWriteConfig);
    }, "Metadata table should have valid log files!");

    verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);

    // 2 more commits
    doWriteOperation(testTable, "0000002", UPSERT);
    doWriteOperation(testTable, "0000004", UPSERT);

    // Compaction should be triggered by now. Let's verify the log files
    // if any for the metadata records persisted on disk as per the config.
    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "0000002", metadataTableWriteConfig);
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
      verifyMetadataRecordKeyExcludeFromPayloadLogFiles(table, metadataMetaClient, "7", metadataTableWriteConfig);
    }, "Metadata table should have valid log files!");

    assertDoesNotThrow(() -> {
      verifyMetadataRecordKeyExcludeFromPayloadBaseFiles(table);
    }, "Metadata table should have a valid base file!");

    validateMetadata(testTable);

    // validate table version
    HoodieTableVersion finalTableVersion = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
        .getTableConfig().getTableVersion();
    assertEquals(tableVersion, finalTableVersion.versionCode());
  }

  /**
   * This tests the case where the two clean actions delete the same file and commit
   * to the metadata table. The metadata table should not contain the deleted file afterwards.
   * A new cleaner plan may contain the same file to delete if the previous cleaner
   * plan has not been successfully executed before the new one is scheduled.
   */
  @ParameterizedTest
  @MethodSource("testMetadataTableKeyGeneratorArgs")
  public void testRepeatedCleanActionsWithMetadataTableEnabled(final HoodieTableType tableType, int tableVersion) throws Exception {
    initPath();
    writeConfig = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build())
        .build();
    writeConfig.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(tableVersion));
    init(tableType, writeConfig);
    String partition = "p1";
    // Simulate two bulk insert operations adding two data files in partition "p1"
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    HoodieCommitMetadata commitMetadata1 =
        testTable.doWriteOperation(instant1, BULK_INSERT, emptyList(), asList(partition), 1);
    String instant2 = WriteClientTestUtils.createNewInstantTime();
    HoodieCommitMetadata commitMetadata2 =
        testTable.doWriteOperation(instant2, BULK_INSERT, emptyList(), asList(partition), 1);

    final HoodieTableMetaClient metadataMetaClient = createMetaClient(metadataTableBasePath);
    while (getNumCompactions(metadataMetaClient) == 0) {
      // Write until the compaction happens in the metadata table
      testTable.doWriteOperation(
          WriteClientTestUtils.createNewInstantTime(), BULK_INSERT, emptyList(), asList(partition), 1);
      metadataMetaClient.reloadActiveTimeline();
    }

    assertEquals(1, getNumCompactions(metadataMetaClient));

    List<String> fileIdsToReplace = new ArrayList<>();
    fileIdsToReplace.addAll(commitMetadata1.getFileIdAndRelativePaths().keySet());
    fileIdsToReplace.addAll(commitMetadata2.getFileIdAndRelativePaths().keySet());
    // Simulate clustering operation replacing two data files with a new data file
    testTable.doCluster(
        WriteClientTestUtils.createNewInstantTime(),
        Collections.singletonMap(partition, fileIdsToReplace), asList(partition), 1);
    Set<String> fileSetBeforeCleaning = getFilePathsInPartition(partition);

    // Simulate two clean actions deleting the same set of date files
    // based on the first two commits
    String cleanInstant = WriteClientTestUtils.createNewInstantTime();
    HoodieCleanMetadata cleanMetadata = testTable.doCleanBasedOnCommits(cleanInstant, asList(instant1, instant2));
    List<String> deleteFileList = cleanMetadata.getPartitionMetadata().get(partition).getDeletePathPatterns();
    assertTrue(deleteFileList.size() > 0);

    Set<String> fileSetAfterFirstCleaning = getFilePathsInPartition(partition);
    validateFilesAfterCleaning(deleteFileList, fileSetBeforeCleaning, fileSetAfterFirstCleaning);

    metaClient.reloadActiveTimeline();
    HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(
        metaClient, INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, CLEAN_ACTION, cleanInstant));
    testTable.repeatClean(WriteClientTestUtils.createNewInstantTime(), cleanerPlan, cleanMetadata);

    // Compaction should not happen after the first compaction in this test case
    assertEquals(1, getNumCompactions(metadataMetaClient));
    Set<String> fileSetAfterSecondCleaning = getFilePathsInPartition(partition);
    validateFilesAfterCleaning(deleteFileList, fileSetBeforeCleaning, fileSetAfterSecondCleaning);

    // validate table version
    HoodieTableVersion finalTableVersion = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
        .getTableConfig().getTableVersion();
    assertEquals(tableVersion, finalTableVersion.versionCode());
  }

  private int getNumCompactions(HoodieTableMetaClient metaClient) {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    return timeline
        .filter(s -> {
          try {
            return s.getAction().equals(HoodieTimeline.COMMIT_ACTION)
                && timeline.readCommitMetadata(s).getOperationType().equals(COMPACT);
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
                                                                 String latestCommitTimestamp, HoodieWriteConfig metadataTableWriteConfig) throws IOException {
    table.getHoodieView().sync();

    // Compaction should not be triggered yet. Let's verify no base file
    // and few log files available.
    List<FileSlice> fileSlices = table.getSliceView()
        .getLatestFileSlices(FILES.getPartitionPath()).collect(Collectors.toList());
    if (fileSlices.isEmpty()) {
      throw new IllegalStateException("LogFile slices are not available!");
    }

    // Verify the log files honor the key deduplication and virtual keys config
    List<HoodieLogFile> logFiles = fileSlices.get(0).getLogFiles().collect(Collectors.toList());

    // Verify the on-disk raw records before they get materialized
    verifyMetadataRawRecords(table, logFiles);

    // Verify the in-memory materialized and merged records
    verifyMetadataMergedRecords(metadataMetaClient, logFiles, latestCommitTimestamp, metadataTableWriteConfig);
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
      HoodieSchema writerSchema  =
          HoodieSchema.fromAvroSchema(TableSchemaResolver.readSchemaFromLogFile(storage, logFile.getPath()));
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
   * @param logFiles              - Metadata table log files
   * @param latestCommitTimestamp - Latest commit timestamp
   */
  private void verifyMetadataMergedRecords(HoodieTableMetaClient metadataMetaClient, List<HoodieLogFile> logFiles, String latestCommitTimestamp, HoodieWriteConfig metadataTableWriteConfig) {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.fromAvroSchema(HoodieMetadataRecord.getClassSchema()));
    HoodieAvroReaderContext readerContext = new HoodieAvroReaderContext(metadataMetaClient.getStorageConf(), metadataMetaClient.getTableConfig(), Option.empty(), Option.empty());
    HoodieFileGroupReader<IndexedRecord> fileGroupReader = HoodieFileGroupReader.<IndexedRecord>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metadataMetaClient)
        .withLogFiles(logFiles.stream())
        .withBaseFileOption(Option.empty())
        .withPartitionPath(FILES.getPartitionPath())
        .withLatestCommitTime(latestCommitTimestamp)
        .withRequestedSchema(schema)
        .withDataSchema(schema)
        .withProps(new TypedProperties())
        .withEnableOptimizedLogBlockScan(metadataTableWriteConfig.getMetadataConfig().isOptimizedLogBlocksScanEnabled())
        .build();

    try (ClosableIterator<HoodieRecord<IndexedRecord>> iter = fileGroupReader.getClosableHoodieRecordIterator()) {
      iter.forEachRemaining(entry -> {
        assertFalse(entry.getRecordKey().isEmpty());
        assertEquals(entry.getKey().getRecordKey(), entry.getRecordKey());
      });
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
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
