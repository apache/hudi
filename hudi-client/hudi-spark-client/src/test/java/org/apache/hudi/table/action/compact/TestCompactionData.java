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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.GenericRecordValidationTestUtils.assertGenericRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCompactionData extends HoodieClientTestBase {
  private static final String RECORD_KEY_APPEND_VALUE1 = "-EXP1";
  private static final String RECORD_KEY_APPEND_VALUE2 = "-EXP2";
  private static final String RECORD_KEY_APPEND_VALUE3 = "-EXP3";

  private static final int PARALLELISM = 2;
  private final Random random = new Random();

  @TempDir
  java.nio.file.Path secondTableBasePath;

  @TempDir
  java.nio.file.Path thirdTableBasePath;

  Function3<List<HoodieRecord>, HoodieTestDataGenerator, String, Integer> insertsGenFunction =
      HoodieTestDataGenerator::generateInserts;
  Function3<List<HoodieRecord>, HoodieTestDataGenerator, String, Integer> updatesGenFunction =
      HoodieTestDataGenerator::generateUniqueUpdates;
  Function2<List<HoodieKey>, HoodieTestDataGenerator, Integer> deletesGenFunction =
      HoodieTestDataGenerator::generateUniqueDeletes;

  Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> insertsFunction = SparkRDDWriteClient::insert;
  Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> updatesFunction = SparkRDDWriteClient::upsert;
  Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieKey>, String> deletesFunction = SparkRDDWriteClient::delete;

  @BeforeEach
  public void setUpTestTable() {
    HoodieSparkWriteableTestTable.of(metaClient);
  }

  /**
   * Cleanups resource group for the subclasses of {@link HoodieClientTestBase}.
   */
  @AfterEach
  public void cleanupResources() throws IOException {
    cleanupTimelineService();
    cleanupClients();
    cleanupSparkContexts();
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupExecutorService();
    System.gc();
  }

  @ParameterizedTest
  @ValueSource(ints = {17})
  public void stressTestCompactions(int seed) throws Exception {

    // Set seed.
    random.setSeed(seed);

    // Setup First table.
    TestCompactionData.TestTableContents mainTable = setupTestTable1();

    // Setup second table.
    TestCompactionData.TestTableContents experimentTable1 = setupTestTable2();

    // Setup third table.
    TestCompactionData.TestTableContents experimentTable2 = setupTestTable3();

    // Initialize the first write action to be `insert`.
    writeOnMainTable(mainTable, WriteAction.INSERT);
    writeOnExperimentTable(mainTable, experimentTable1);
    writeOnExperimentTable(mainTable, experimentTable2);

    int totalCompactions = 10;
    int compactionCount = 0;

    while (compactionCount < totalCompactions) {
      WriteAction action = pickAWriteAction();
      if (action == WriteAction.COMPACT) {
        // schedule and run compaction on second table.
        scheduleCompactionOnExperimentTable(experimentTable1);

        scheduleCompactionOnExperimentTable(experimentTable2);

        // Verify that no compaction plans are left on the timeline.
        assertEquals(0, mainTable.metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().countInstants());
        assertEquals(0, experimentTable1.metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().countInstants());
        assertEquals(0, experimentTable2.metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().countInstants());

        // Verify the records in both the tables.
        verifyRecords(mainTable, experimentTable1);
        verifyRecords(mainTable, experimentTable2);
        verifyRecords(experimentTable1, experimentTable2);
        LOG.warn("For compaction No." + compactionCount + ", verification passed. Last ingestion commit timestamp is " + mainTable.commitTimeOnMainTable);
        compactionCount++;
      } else {
        writeOnMainTable(mainTable, action);
        // Write data into experiment table.
        writeOnExperimentTable(mainTable, experimentTable1);
        writeOnExperimentTable(mainTable, experimentTable2);
      }
    }
    mainTable.client.close();
    experimentTable1.client.close();
    experimentTable2.client.close();
  }

  private void verifyRecords(TestCompactionData.TestTableContents mainTable, TestCompactionData.TestTableContents experimentTable) {
    Map<String, GenericRecord> mainRecordsMap =
        GenericRecordValidationTestUtils.getRecordsMap(mainTable.config, storageConf, dataGen);
    Map<String, GenericRecord> experimentRecordsMap =
        GenericRecordValidationTestUtils.getRecordsMap(experimentTable.config, storageConf, dataGen);

    // Verify row count.
    assertEquals(mainRecordsMap.size(), experimentRecordsMap.size());

    Schema readerSchema = new Schema.Parser().parse(mainTable.config.getSchema());
    List<String> excludeFields = CollectionUtils.createImmutableList(COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD,
        FILENAME_METADATA_FIELD, OPERATION_METADATA_FIELD, RECORD_KEY_METADATA_FIELD);

    // Verify every field.
    mainRecordsMap.forEach((key, value) -> {
      String realKey = key.replace(mainTable.recordKeySuffix, "");
      assertTrue(experimentRecordsMap.containsKey(realKey + experimentTable.recordKeySuffix));
      assertGenericRecords(value, experimentRecordsMap.get(realKey + experimentTable.recordKeySuffix), readerSchema, excludeFields);
    });
  }

  private void scheduleCompactionOnExperimentTable(TestCompactionData.TestTableContents experimentTable) {
    Option<String> compactionTimeStamp = experimentTable.client.scheduleCompaction(Option.empty());
    if (compactionTimeStamp.isPresent()) {
      experimentTable.client.compact(compactionTimeStamp.get());
    }
  }

  private boolean writeOnMainTable(TestCompactionData.TestTableContents mainTable, WriteAction action) throws IOException {
    String commitTime = mainTable.client.createNewInstantTime();
    mainTable.client.startCommitWithTime(commitTime);

    JavaRDD<WriteStatus> result;
    switch (action) {
      case INSERT:
        result = insertDataIntoMainTable(mainTable, commitTime);
        break;
      case UPSERT:
        result = updateDataIntoMainTable(mainTable, commitTime);
        break;
      case DELETE:
        result = deleteDataIntoMainTable(mainTable, commitTime);
        break;
      case COMPACT:
        throw new IllegalArgumentException("Cannot perform compaction on main table.");
      default:
        throw new IllegalArgumentException("Unknown action type " + action);
    }
    verifyWriteStatus(result);
    return true;
  }

  /**
   * 40% insert, 30% upsert, 20% delete, 10% compact.
   */
  private WriteAction pickAWriteAction() {
    int val = random.nextInt(10);

    if (val < 4) {
      return WriteAction.INSERT;
    }
    if (val < 7) {
      return WriteAction.UPSERT;
    }
    if (val < 9) {
      return WriteAction.DELETE;
    }
    return WriteAction.COMPACT;
  }

  private enum WriteAction {
    INSERT,
    UPSERT,
    DELETE,
    COMPACT
  }

  private void writeOnExperimentTable(TestCompactionData.TestTableContents mainTable, TestCompactionData.TestTableContents experimentTable) throws IOException {
    String commitTime = mainTable.commitTimeOnMainTable;
    experimentTable.client.startCommitWithTime(commitTime);
    int actionType = mainTable.previousActionType;
    JavaRDD<WriteStatus> result;
    if (actionType == 0) {
      result = insertDataIntoExperimentTable(mainTable, experimentTable);
    } else if (actionType == 1) {
      result = updateDataIntoExperimentTable(mainTable, experimentTable);
    } else {
      result = deleteDataIntoExperimentTable(mainTable, experimentTable);
    }
    verifyWriteStatus(result);
  }

  private JavaRDD<WriteStatus> insertDataIntoMainTable(TestCompactionData.TestTableContents mainTable, String commitTime) throws IOException {
    int numRecords = 50 + random.nextInt(10);
    List<HoodieRecord> records = insertsGenFunction.apply(dataGen, commitTime, numRecords);
    mainTable.updatePreviousGeneration(records, commitTime, 0);
    List<HoodieRecord> realRecords = records.stream().map(mainTable::deepCopyAndModifyRecordKey).collect(Collectors.toList());
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(realRecords, PARALLELISM);
    return insertsFunction.apply(mainTable.client, writeRecords, commitTime);
  }

  private JavaRDD<WriteStatus> updateDataIntoMainTable(TestCompactionData.TestTableContents mainTable, String commitTime) throws IOException {
    int numRecords = 10 + random.nextInt(10);
    List<HoodieRecord> records = updatesGenFunction.apply(dataGen, commitTime, numRecords);
    mainTable.updatePreviousGeneration(records, commitTime, 1);
    List<HoodieRecord> realRecords = records.stream().map(mainTable::deepCopyAndModifyRecordKey).collect(Collectors.toList());
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(realRecords, PARALLELISM);
    return updatesFunction.apply(mainTable.client, writeRecords, commitTime);
  }

  private JavaRDD<WriteStatus> deleteDataIntoMainTable(TestCompactionData.TestTableContents mainTable, String commitTime) throws IOException {
    int numRecords = 5 + random.nextInt(10);
    List<HoodieKey> keys = deletesGenFunction.apply(dataGen, numRecords);
    mainTable.updatePreviousGenerationForDelete(keys, commitTime);
    List<HoodieKey> realRecords = keys.stream().map(mainTable::deepCopyAndModifyRecordKey).collect(Collectors.toList());
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(realRecords, PARALLELISM);
    return deletesFunction.apply(mainTable.client, deleteKeys, commitTime);
  }

  private JavaRDD<WriteStatus> insertDataIntoExperimentTable(TestCompactionData.TestTableContents mainTable, TestCompactionData.TestTableContents experimentTable) throws IOException {

    List<HoodieRecord> realRecords = mainTable.generatedRecords.stream().map(experimentTable::deepCopyAndModifyRecordKey).collect(Collectors.toList());
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(realRecords, PARALLELISM);
    return insertsFunction.apply(experimentTable.client, writeRecords, mainTable.commitTimeOnMainTable);
  }

  private JavaRDD<WriteStatus> updateDataIntoExperimentTable(TestCompactionData.TestTableContents mainTable, TestCompactionData.TestTableContents experimentTable) throws IOException {
    List<HoodieRecord> realRecords = mainTable.generatedRecords.stream().map(experimentTable::deepCopyAndModifyRecordKey).collect(Collectors.toList());
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(realRecords, PARALLELISM);
    return updatesFunction.apply(experimentTable.client, writeRecords, mainTable.commitTimeOnMainTable);
  }

  private JavaRDD<WriteStatus> deleteDataIntoExperimentTable(TestCompactionData.TestTableContents mainTable, TestCompactionData.TestTableContents experimentTable) throws IOException {
    List<HoodieKey> realRecords = mainTable.generatedKeysForDelete.stream().map(experimentTable::deepCopyAndModifyRecordKey).collect(Collectors.toList());
    JavaRDD<HoodieKey> writeKeys = jsc.parallelize(realRecords, PARALLELISM);
    return deletesFunction.apply(experimentTable.client, writeKeys, mainTable.commitTimeOnMainTable);
  }

  private void verifyWriteStatus(JavaRDD<WriteStatus> writeStatuses) {
    List<WriteStatus> statuses = writeStatuses.collect();
    assertNoWriteErrors(statuses);
  }

  private class TestTableContents {
    final String basePath;
    final String tableName;
    final HoodieTableMetaClient metaClient;
    final HoodieWriteConfig config;
    final SparkRDDWriteClient client;
    String commitTimeOnMainTable = "";
    List<HoodieRecord> generatedRecords = new ArrayList<>();
    List<HoodieKey> generatedKeysForDelete = new ArrayList<>();
    // 0 means insert, 1 means update, 2 means delete.
    int previousActionType = 0;
    String recordKeySuffix;

    public TestTableContents(String basePath, String tableName, HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                             SparkRDDWriteClient client, String recordKeySuffix) {
      this.basePath = basePath;
      this.tableName = tableName;
      this.metaClient = metaClient;
      this.config = config;
      this.client = client;
      this.recordKeySuffix = recordKeySuffix;
    }

    private void updatePreviousGeneration(List<HoodieRecord> generatedRecords, String commitTimeOnMainTable, int previousActionType) {
      Schema schema = new Schema.Parser().parse(this.config.getSchema());
      this.generatedRecords = generatedRecords;
      this.commitTimeOnMainTable = commitTimeOnMainTable;
      this.previousActionType = previousActionType;
    }

    private HoodieRecord deepCopyAndModifyRecordKey(HoodieRecord record) {
      HoodieKey key = deepCopyAndModifyRecordKey(record.getKey());
      RawTripTestPayload payload = ((RawTripTestPayload) record.getData()).clone();
      return new HoodieAvroRecord(key, payload);
    }

    private HoodieKey deepCopyAndModifyRecordKey(HoodieKey key) {
      return new HoodieKey(key.getRecordKey() + recordKeySuffix, key.getPartitionPath());
    }

    private void updatePreviousGenerationForDelete(List<HoodieKey> generatedKeysForDelete, String commitTimeOnMainTable) {
      this.generatedKeysForDelete = generatedKeysForDelete;
      this.commitTimeOnMainTable = commitTimeOnMainTable;
      this.previousActionType = 2;
    }
  }

  // not compaction table
  private TestCompactionData.TestTableContents setupTestTable1() {
    Properties properties = new Properties();
    properties.setProperty("hoodie.parquet.small.file.limit", "0");
    HoodieWriteConfig config = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withSortedMergeCompaction(false).withInlineCompaction(false).build())
        .withAutoCommit(true)
        .withProperties(properties)
        .build();
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, config);
    return new TestCompactionData.TestTableContents(basePath, tableName, metaClient, config, client, RECORD_KEY_APPEND_VALUE1);
  }

  // original compaction table
  private TestCompactionData.TestTableContents setupTestTable2() throws IOException {
    String tableName2 = "test-trip-table2";
    String basePath2 = createBasePathForSecondTable(secondTableBasePath, 2);
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.NAME.key(), tableName2);

    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(storageConf, basePath2,
        HoodieTableType.MERGE_ON_READ, properties);
    HoodieWriteConfig config2 = getConfigBuilderForSecondTable(tableName2, basePath2,
        TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).withSortedMergeCompaction(false).withInlineCompaction(true).build())
        .withAutoCommit(true).build();

    // Create writeClient
    SparkRDDWriteClient client2 = new SparkRDDWriteClient(context, config2);

    return new TestCompactionData.TestTableContents(basePath2, tableName2, metaClient2, config2, client2, RECORD_KEY_APPEND_VALUE2);
  }

  // sorted merge compaction table
  private TestCompactionData.TestTableContents setupTestTable3() throws IOException {
    String tableName2 = "test-trip-table3";
    String basePath2 = createBasePathForSecondTable(thirdTableBasePath, 3);
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.NAME.key(), tableName2);

    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(storageConf, basePath2,
        HoodieTableType.MERGE_ON_READ, properties);
    HoodieWriteConfig config2 = getConfigBuilderForSecondTable(tableName2, basePath2,
        TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).withSortedMergeCompaction(true).withInlineCompaction(true).build())
        .withAutoCommit(true).build();

    // Create writeClient
    SparkRDDWriteClient client2 = new SparkRDDWriteClient(context, config2);

    return new TestCompactionData.TestTableContents(basePath2, tableName2, metaClient2, config2, client2, RECORD_KEY_APPEND_VALUE3);
  }


  private String createBasePathForSecondTable(java.nio.file.Path secondTableBasePath, int num) throws IOException {
    java.nio.file.Path basePath = secondTableBasePath.resolve("dataset" + num);
    java.nio.file.Files.createDirectories(basePath);
    return basePath.toString();
  }

  private HoodieWriteConfig.Builder getConfigBuilderForSecondTable(String tableName, String basePath, String schemaStr, HoodieIndex.IndexType indexType) {
    Properties properties = new Properties();
    properties.setProperty("hoodie.parquet.small.file.limit", "0");
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(PARALLELISM, PARALLELISM).withBulkInsertParallelism(PARALLELISM).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort).build())
        .withProperties(properties);
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
