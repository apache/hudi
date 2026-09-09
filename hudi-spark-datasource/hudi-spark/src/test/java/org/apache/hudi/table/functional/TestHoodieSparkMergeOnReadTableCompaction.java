/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.functional;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.action.rollback.RollbackUtils;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieSparkMergeOnReadTableCompaction extends SparkClientFunctionalTestHarness {

  private static Stream<Arguments> writeLogTest() {
    // enable metadata table, enable embedded time line server
    Object[][] data = new Object[][] {
        {true, true},
        {true, false},
        {false, true},
        {false, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private static Stream<Arguments> writePayloadTest() {
    // Payload class and index combinations
    return Stream.of(
        Arguments.of(DefaultHoodieRecordPayload.class.getName(), HoodieIndex.IndexType.BUCKET),
        Arguments.of(DefaultHoodieRecordPayload.class.getName(), HoodieIndex.IndexType.GLOBAL_SIMPLE),
        Arguments.of(PartialUpdateAvroPayload.class.getName(), HoodieIndex.IndexType.BUCKET),
        Arguments.of(PartialUpdateAvroPayload.class.getName(), HoodieIndex.IndexType.GLOBAL_SIMPLE));
  }

  private HoodieTestDataGenerator dataGen;
  private SparkRDDWriteClient client;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setup() {
    dataGen = new HoodieTestDataGenerator();
  }

  @AfterEach
  public void teardown() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @ParameterizedTest
  @MethodSource("writePayloadTest")
  public void testWriteDuringCompaction(String payloadClass, HoodieIndex.IndexType indexType) throws IOException {
    Properties props = getPropertiesForKeyGen(true);
    HoodieLayoutConfig layoutConfig = indexType == HoodieIndex.IndexType.BUCKET
        ? HoodieLayoutConfig.newBuilder().withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name()).withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build()
        : HoodieLayoutConfig.newBuilder().build();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withWritePayLoad(payloadClass)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .compactionSmallFileSize(0)
            .build())
        .withLayoutConfig(layoutConfig)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(indexType).withBucketNum("1").build())
        .withMarkersTimelineServerBasedBatchIntervalMs(10)
        .build();
    props.putAll(config.getProps());

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    client = getHoodieWriteClient(config);

    // write data and commit
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    writeData(instant1, dataGen.generateInserts(instant1, 100), true);
    // write mix of new data and updates, and in the case of bucket index, all records will go into log files (we use a small max_file_size)
    String instant2 = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> updates1 = dataGen.generateUpdates(instant2, 100);
    List<HoodieRecord> newRecords1 = dataGen.generateInserts(instant2, 100);
    writeData(instant2, Stream.concat(newRecords1.stream(), updates1.stream()).collect(Collectors.toList()), true);
    assertEquals(200, readTableTotalRecordsNum());
    // schedule compaction
    String compactionTime = (String) client.scheduleCompaction(Option.empty()).get();
    // write data, and do not commit. those records should not visible to reader
    String instant3 = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> updates2 = dataGen.generateUpdates(instant3, 200);
    List<HoodieRecord> newRecords2 = dataGen.generateInserts(instant3, 100);
    List<WriteStatus> writeStatuses = writeData(instant3, Stream.concat(newRecords2.stream(), updates2.stream()).collect(Collectors.toList()), false);
    assertEquals(200, readTableTotalRecordsNum());
    // commit the write. The records should be visible now even though the compaction does not complete.
    client.commitStats(instant3, writeStatuses.stream().map(WriteStatus::getStat)
        .collect(Collectors.toList()), Option.empty(), metaClient.getCommitActionType());
    assertEquals(300, readTableTotalRecordsNum());
    // after the compaction, total records should remain the same
    HoodieWriteMetadata result = client.compact(compactionTime);
    client.commitCompaction(compactionTime, result, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionTime));
    assertEquals(300, readTableTotalRecordsNum());
  }

  @ParameterizedTest
  @MethodSource("writeLogTest")
  public void testWriteLogDuringCompaction(boolean enableMetadataTable, boolean enableTimelineServer) throws IOException {
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
          .withEmbeddedTimelineServerEnabled(enableTimelineServer)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.BUCKET).withBucketNum("1").build())
        .withMarkersTimelineServerBasedBatchIntervalMs(10)
        .build();
    props.putAll(config.getProps());

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    client = getHoodieWriteClient(config);

    final List<HoodieRecord> records = dataGen.generateInserts("001", 100);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 2);

    // initialize 100 records
    String commit1 = client.startCommit();
    JavaRDD writeStatuses = client.upsert(writeRecords, commit1);
    client.commit(commit1, writeStatuses);

    // update 100 records
    String commit2 = client.startCommit();
    writeStatuses = client.upsert(writeRecords, commit2);
    client.commit(commit2, writeStatuses);
    // schedule compaction
    client.scheduleCompaction(Option.empty());
    // delete 50 records
    List<HoodieKey> toBeDeleted = records.stream().map(HoodieRecord::getKey).limit(50).collect(Collectors.toList());
    JavaRDD<HoodieKey> deleteRecords = jsc().parallelize(toBeDeleted, 2);
    String commit3 = client.startCommit();
    writeStatuses = client.delete(deleteRecords, commit3);
    client.commit(commit3, writeStatuses);

    // insert the same 100 records again
    String commit4 = client.startCommit();
    writeStatuses = client.upsert(writeRecords, commit4);
    client.commit(commit4, writeStatuses);
    assertEquals(100, readTableTotalRecordsNum());
  }

  /**
   * Tests marker-based rollback does not include non-existent log files
   *
   * @param enableMetadataTable whether to enable the metadata table
   * @param runRollback         whether to rollback failed commits
   * @throws IOException upon I/O errors
   */
  @ParameterizedTest
  @CsvSource(value = {"true,true", "true,false", "false,true", "false,false"})
  void testCompactionSchedulingWithUncommittedLogFileOrRollback(boolean enableMetadataTable,
                                                                boolean runRollback) throws IOException {
    Properties props = getPropertiesForKeyGen(true);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetMaxFileSize(20480).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(enableMetadataTable).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withHeartbeatIntervalInMs(120000)
        .withLockConfig(HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withMarkersTimelineServerBasedBatchIntervalMs(10)
        .build();
    props.putAll(config.getProps());

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    client = getHoodieWriteClient(config);

    // instant 1: write inserts and commit, generating base files
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> recordList = dataGen.generateInserts(instant1, 100);
    writeData(instant1, recordList, true);
    assertEquals(100, readTableTotalRecordsNum());
    validateFileListingInMetadataTable();
    // instant 2: write updates in log files and simulate failed deltacommit
    String instant2 = WriteClientTestUtils.createNewInstantTime();
    recordList = dataGen.generateUpdates(instant2, 100);
    List<WriteStatus> writeStatuses2 = writeData(instant2, recordList, false);

    // remove half of the log files written to simulate the failure case
    // where the marker is created but the log file is not written
    List<StoragePathInfo> files = hoodieStorage().listFiles(new StoragePath(basePath()));
    int numTotalLogFiles = 0;
    for (StoragePathInfo file : files) {
      if (file.isFile() && !file.getPath().toString().contains(METAFOLDER_NAME)
          && FSUtils.isLogFile(file.getPath())) {
        numTotalLogFiles++;
        if (numTotalLogFiles % 2 == 0) {
          hoodieStorage().deleteFile(file.getPath());
        }
      }
    }

    int numLogFilesAfterDeletion = 0;
    files = hoodieStorage().listFiles(new StoragePath(basePath()));
    for (StoragePathInfo file : files) {
      if (file.isFile() && !file.getPath().toString().contains(METAFOLDER_NAME)
          && FSUtils.isLogFile(file.getPath())) {
        numLogFilesAfterDeletion++;
      }
    }

    // validate the current table state to satisfy what is intended to test
    if (enableMetadataTable) {
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
          .setConf(storageConf()).setBasePath(HoodieTableMetadata.getMetadataTableBasePath(basePath())).build();
      assertEquals(instant1, metadataMetaClient.getActiveTimeline().lastInstant().get().requestedTime());
    }
    assertTrue(numLogFilesAfterDeletion > 0 && numLogFilesAfterDeletion < numTotalLogFiles);
    assertEquals(instant2, metaClient.getActiveTimeline().lastInstant().get().requestedTime());
    assertEquals(instant1, metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get().requestedTime());
    assertEquals(100, readTableTotalRecordsNum());

    // instant 3: write updates in log files and make a successful deltacommit
    String instant3 = WriteClientTestUtils.createNewInstantTime();
    recordList = dataGen.generateUpdates(instant3, 100);
    writeData(instant3, recordList, true);

    if (runRollback) {
      // If enabled, rollback the failed delta commit
      client.rollback(instant2);
      validateLogFilesExistInRollbackPlan();
      validateFileListingInMetadataTable();
    }

    // schedule compaction
    String compactionInstant = (String) client.scheduleCompaction(Option.empty()).get();
    validateFilesExistInCompactionPlan(compactionInstant);
    if (!runRollback) {
      // committing instant2 that conflicts with the compaction plan should fail
      assertThrows(HoodieWriteConflictException.class, () -> commitToTable(instant2, writeStatuses2));
    }
    HoodieWriteMetadata result = client.compact(compactionInstant);
    client.commitCompaction(compactionInstant, result, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionInstant));
    if (runRollback) {
      validateFileListingInMetadataTable();
    }
    assertEquals(100, readTableTotalRecordsNum());
    assertEquals(
        compactionInstant,
        metaClient.reloadActiveTimeline().filterCompletedInstants().lastInstant().get().requestedTime());
  }

  private void validateLogFilesExistInRollbackPlan() throws IOException {
    metaClient.reloadActiveTimeline();
    HoodieRollbackPlan rollbackPlan = RollbackUtils.getRollbackPlan(metaClient, metaClient.getActiveTimeline()
        .filter(e -> HoodieActiveTimeline.ROLLBACK_ACTION.equals(e.getAction()))
        .lastInstant().get());
    assertTrue(rollbackPlan.getRollbackRequests().stream()
        .map(request -> {
          boolean allExist = true;
          StoragePath partitionPath = new StoragePath(basePath(), request.getPartitionPath());
          for (String logFile : request.getLogBlocksToBeDeleted().keySet()) {
            try {
              allExist = allExist && hoodieStorage().exists(new StoragePath(partitionPath, logFile));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return allExist;
        })
        .reduce(Boolean::logicalAnd)
        .get());
  }

  private void validateFilesExistInCompactionPlan(String compactionInstant) {
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(metaClient, compactionInstant);
    assertTrue(compactionPlan.getOperations().stream()
        .map(op -> {
          boolean allExist;
          StoragePath partitionPath = new StoragePath(basePath(), op.getPartitionPath());
          try {
            allExist = hoodieStorage().exists(new StoragePath(partitionPath, op.getDataFilePath()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          for (String logFilePath : op.getDeltaFilePaths()) {
            try {
              allExist = allExist && hoodieStorage().exists(new StoragePath(partitionPath, logFilePath));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return allExist;
        })
        .reduce(Boolean::logicalAnd)
        .get());
  }

  private void validateFileListingInMetadataTable() {
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(context(), metaClient, false)
        .stream()
        .map(e -> new StoragePath(basePath(), e).toString())
        .collect(Collectors.toList());
    Map<String, List<StoragePathInfo>> filesFromStorage = FSUtils.getFilesInPartitions(
        context(),
        metaClient,
        HoodieMetadataConfig.newBuilder().enable(false).build(),
        partitionPaths.toArray(new String[0]));
    Map<String, List<StoragePathInfo>> filesFromMetadataTable = FSUtils.getFilesInPartitions(
        context(),
        metaClient,
        HoodieMetadataConfig.newBuilder().enable(true).build(),
        partitionPaths.toArray(new String[0]));
    assertEquals(filesFromStorage.size(), filesFromMetadataTable.size());
    for (String partition : filesFromStorage.keySet()) {
      List<StoragePathInfo> partitionFilesFromStorage = filesFromStorage.get(partition).stream()
          .sorted().collect(Collectors.toList());
      List<StoragePathInfo> partitionFilesFromMetadataTable = filesFromMetadataTable.get(partition).stream()
          .sorted().collect(Collectors.toList());
      assertEquals(partitionFilesFromStorage.size(), partitionFilesFromMetadataTable.size());
      for (int i = 0; i < partitionFilesFromStorage.size(); i++) {
        assertEquals(
            partitionFilesFromStorage.get(i).getPath().toString(),
            partitionFilesFromMetadataTable.get(i).getPath().toString());
      }
    }
  }

  private long readTableTotalRecordsNum() {
    return HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(storageConf(),
        Arrays.stream(dataGen.getPartitionPaths()).map(p -> Paths.get(basePath(), p).toString()).collect(Collectors.toList()), basePath()).size();
  }

  private List<WriteStatus> writeData(String instant, List<HoodieRecord> hoodieRecords, boolean doCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    JavaRDD<HoodieRecord> records = jsc().parallelize(hoodieRecords, 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> writeStatuses = client.upsert(records, instant).collect();
    assertNoWriteErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
      assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }

  private void commitToTable(String instant, List<WriteStatus> writeStatuses) {
    List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    boolean committed =
        client.commitStats(instant, writeStats, Option.empty(), metaClient.getCommitActionType());
    assertTrue(committed);
  }

  // Avro fixed(10) decimal(20,2). 10 is wider than the precision-minimal width (9 for precision
  // 20), so Spark's own DecimalType->Avro conversion would emit fixed(9); the declared 10 only
  // survives if the write path honors the Avro fixed size. Do not derive this schema from a
  // DataFrame, which would drop the fixed size.
  private static final String FIXED10_DECIMAL_SCHEMA =
      "{\"type\":\"record\",\"name\":\"decimalRec\",\"fields\":["
          + "{\"name\":\"_row_key\",\"type\":\"string\"},"
          + "{\"name\":\"partition_path\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":\"long\"},"
          + "{\"name\":\"dec\",\"type\":{\"type\":\"fixed\",\"name\":\"decFixed\",\"size\":10,"
          + "\"logicalType\":\"decimal\",\"precision\":20,\"scale\":2}}]}";

  private static final String DECIMAL_PARTITION = "p1";
  private static final int EXPECTED_DECIMAL_FIXED_LEN = 10;

  @Test
  void testDecimalFixedWidthPreservedAfterCompactionAndClustering() throws Exception {
    Properties props = getPropertiesForKeyGen(true);
    Properties rowWriterProps = new Properties();
    rowWriterProps.put("hoodie.datasource.write.row.writer.enable", "true");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .forTable("test-decimal-fixed")
        .withPath(basePath())
        .withSchema(FIXED10_DECIMAL_SCHEMA)
        .withParallelism(2, 2)
        .withPreCombineField("ts")
        .withProperties(rowWriterProps)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .compactionSmallFileSize(0)
            .withInlineCompaction(false)
            .build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringMaxNumGroups(10)
            .withClusteringTargetPartitions(0)
            .withInlineClustering(false)
            .withInlineClusteringNumCommits(1)
            .build())
        .build();
    props.putAll(config.getProps());

    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, props);
    client = getHoodieWriteClient(config);

    // two insert commits (small-file size 0 forces a fresh file group each) create two base-file
    // groups, both written via the Avro path at fixed(10)
    String instant1 = WriteClientTestUtils.createNewInstantTime();
    writeData(instant1, buildDecimalRecords(0, 10, 1L, new BigDecimal("123456789.12")), true);
    String instant2 = WriteClientTestUtils.createNewInstantTime();
    writeData(instant2, buildDecimalRecords(10, 10, 1L, new BigDecimal("223456789.34")), true);
    // update every key so both groups accumulate log files for compaction to merge
    String instant3 = WriteClientTestUtils.createNewInstantTime();
    writeData(instant3, buildDecimalRecords(0, 20, 2L, new BigDecimal("323456789.56")), true);

    // precondition: both file groups must carry log files, else compaction is a no-op and would not
    // exercise the row-writer merge path
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context(), metaClient);
    hoodieTable.getHoodieView().sync();
    List<FileSlice> latestSlices =
        hoodieTable.getHoodieView().getLatestFileSlices(DECIMAL_PARTITION).collect(Collectors.toList());
    assertEquals(2, latestSlices.size(), "expected two file groups before compaction");
    assertTrue(latestSlices.stream().allMatch(slice -> slice.getLogFiles().findAny().isPresent()),
        "each file group must have log files for compaction to merge");
    HoodieSchema tableSchema = new TableSchemaResolver(metaClient).getTableSchema(false);

    // compaction rewrites both base files through the Spark record type
    String compactionInstant = (String) client.scheduleCompaction(Option.empty()).get();
    HoodieWriteMetadata compactionResult = client.compact(compactionInstant);
    client.commitCompaction(compactionInstant, compactionResult, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionInstant));
    List<StoragePath> compactedBaseFiles = latestBaseFilePaths(config, DECIMAL_PARTITION);
    assertEquals(2, compactedBaseFiles.size(), "expected two compacted base files");
    for (StoragePath path : compactedBaseFiles) {
      assertDecimalFixedLen(path, EXPECTED_DECIMAL_FIXED_LEN);
    }
    assertEquals(tableSchema,
        new TableSchemaResolver(HoodieTableMetaClient.reload(metaClient)).getTableSchema(false),
        "table schema in commit metadata must not change after compaction");

    // clustering rewrites the two compacted groups through the same Spark row-writer path
    String clusteringInstant = (String) client.scheduleClustering(Option.empty()).get();
    HoodieWriteMetadata<JavaRDD<WriteStatus>> clusterMetadata = client.cluster(clusteringInstant, true);
    List<HoodieWriteStat> clusterStats = clusterMetadata.getWriteStats().get();
    assertFalse(clusterStats.isEmpty(), "clustering should write at least one base file");
    for (HoodieWriteStat stat : clusterStats) {
      assertDecimalFixedLen(new StoragePath(metaClient.getBasePath(), stat.getPath()),
          EXPECTED_DECIMAL_FIXED_LEN);
    }
    assertEquals(tableSchema,
        new TableSchemaResolver(HoodieTableMetaClient.reload(metaClient)).getTableSchema(false),
        "table schema in commit metadata must not change after clustering");
  }

  private List<HoodieRecord> buildDecimalRecords(int startKey, int count, long ts, BigDecimal decValue) {
    Schema schema = new Schema.Parser().parse(FIXED10_DECIMAL_SCHEMA);
    Schema decSchema = schema.getField("dec").schema();
    Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
    LogicalTypes.Decimal decimalType = LogicalTypes.decimal(20, 2);
    BigDecimal scaledValue = decValue.setScale(2);
    List<HoodieRecord> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String key = "key_" + (startKey + i);
      GenericRecord rec = new GenericData.Record(schema);
      rec.put("_row_key", key);
      rec.put("partition_path", DECIMAL_PARTITION);
      rec.put("ts", ts);
      GenericFixed fixed = decimalConversion.toFixed(scaledValue, decSchema, decimalType);
      rec.put("dec", fixed);
      records.add(new HoodieAvroRecord<>(new HoodieKey(key, DECIMAL_PARTITION),
          new OverwriteWithLatestAvroPayload(rec, ts)));
    }
    return records;
  }

  private List<StoragePath> latestBaseFilePaths(HoodieWriteConfig config, String partition) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable hoodieTable = HoodieSparkTable.create(config, context(), metaClient);
    hoodieTable.getHoodieView().sync();
    return hoodieTable.getBaseFileOnlyView().getLatestBaseFiles(partition)
        .map(HoodieBaseFile::getStoragePath).collect(Collectors.toList());
  }

  private void assertDecimalFixedLen(StoragePath baseFilePath, int expectedLen) {
    MessageType parquetSchema = ParquetUtils.readMetadata(hoodieStorage(), baseFilePath)
        .getFileMetaData().getSchema();
    PrimitiveType decType = parquetSchema.getType("dec").asPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, decType.getPrimitiveTypeName(),
        "decimal must be encoded as FIXED_LEN_BYTE_ARRAY");
    assertEquals(expectedLen, decType.getTypeLength(),
        "Avro fixed(10) decimal(20,2) must stay FIXED_LEN_BYTE_ARRAY(10), not narrow to 9");
  }
}
