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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
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

public class TestDataValidationCheckForLogCompactionActions extends HoodieClientTestBase {

  private static final String RECORD_KEY_APPEND_VALUE = "-EXP";
  private static final int PARALLELISM = 2;
  private final Random random = new Random();

  @TempDir
  java.nio.file.Path secondTableBasePath;

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

  //TODO: include both the table's contents.
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

  /**
   * Stress test logcompaction along with compaction by following approach.
   * a. Create a random seed to do insert/upsert/deleting operations on main table and replicate same action on experiment table.
   * b. Schedule inline major compaction to run for every 5 deltacommits on both the tables.
   * c. After writes on both the tables configure log compaction to run on second table and keep no. of blocks threshold to 2.
   * d. After every commit operation refresh the timeline and run a validation query for all the records.
   */
  @ParameterizedTest
  @ValueSource(ints = {17})
  public void stressTestCompactionAndLogCompactionOperations(int seed) throws Exception {

    // Set seed.
    random.setSeed(seed);

    // Setup First table.
    TestTableContents mainTable = setupTestTable1();

    // Setup second table.
    TestTableContents experimentTable = setupTestTable2();

    // Total ingestion writes.
    int totalWrites = 15;

    LOG.warn("Starting trial with seed " + seed);

    // Current ingestion commit.
    int curr = 1;
    while (curr < totalWrites) {
      LOG.warn("Starting write No. " + curr);

      // Pick an action. It can be insert/update/delete and write data to main table.
      boolean status = writeOnMainTable(mainTable, curr);
      if (status) {
        // Write data into experiment table.
        writeOnExperimentTable(mainTable, experimentTable);

        // schedule and run log compaction on second table.
        scheduleLogCompactionOnExperimentTable(experimentTable);

        // Verify that no compaction plans are left on the timeline.
        assertEquals(0, mainTable.metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().countInstants());
        assertEquals(0, experimentTable.metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().countInstants());
        assertEquals(0, experimentTable.metaClient.reloadActiveTimeline().filterPendingLogCompactionTimeline().countInstants());

        // Verify the records in both the tables.
        verifyRecords(mainTable, experimentTable);
        LOG.warn("For write No." + curr + ", verification passed. Last ingestion commit timestamp is " + mainTable.commitTimeOnMainTable);
      }
      curr++;
    }
    mainTable.client.close();
    experimentTable.client.close();
  }

  private void verifyRecords(TestTableContents mainTable, TestTableContents experimentTable) {
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
      assertTrue(experimentRecordsMap.containsKey(key + RECORD_KEY_APPEND_VALUE));
      assertGenericRecords(value, experimentRecordsMap.get(key + RECORD_KEY_APPEND_VALUE), readerSchema, excludeFields);
    });
  }

  private void scheduleLogCompactionOnExperimentTable(TestTableContents experimentTable) {
    Option<String> logCompactionTimeStamp = experimentTable.logCompactionClient.scheduleLogCompaction(Option.empty());
    if (logCompactionTimeStamp.isPresent()) {
      experimentTable.logCompactionClient.logCompact(logCompactionTimeStamp.get());
    }
  }

  private boolean writeOnMainTable(TestTableContents mainTable, int curr) throws IOException {
    String commitTime = mainTable.client.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(mainTable.client, commitTime);

    int actionType = pickAWriteAction();
    JavaRDD<WriteStatus> result;
    if (curr == 1 || actionType == 0) {
      result = insertDataIntoMainTable(mainTable, commitTime);
    } else {
      try {
        if (actionType == 1) {
          result = updateDataIntoMainTable(mainTable, commitTime);
        } else {
          result = deleteDataIntoMainTable(mainTable, commitTime);
        }
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage() + " ignoring current command.");
        return false;
      }
    }
    verifyWriteStatus(result);
    return true;
  }

  /**
   * This method has 50% chance to pick an insert, 30% chance to pick an update and 20% chance to pick a delete operation
   */
  private int pickAWriteAction() {
    int val = random.nextInt(10);
    if (val < 5) {
      return 0;
    } else if (val < 8) {
      return 1;
    }
    return 2;
  }

  private void writeOnExperimentTable(TestTableContents mainTable, TestTableContents experimentTable) throws IOException {
    String commitTime = mainTable.commitTimeOnMainTable;
    WriteClientTestUtils.startCommitWithTime(experimentTable.client, commitTime);
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

  private JavaRDD<WriteStatus> insertDataIntoMainTable(TestTableContents mainTable, String commitTime) throws IOException {
    int numRecords = 50 + random.nextInt(10);
    List<HoodieRecord> records = insertsGenFunction.apply(dataGen, commitTime, numRecords);
    mainTable.updatePreviousGeneration(records, commitTime, 0);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, PARALLELISM);
    return insertsFunction.apply(mainTable.client, writeRecords, commitTime);
  }

  private JavaRDD<WriteStatus> updateDataIntoMainTable(TestTableContents mainTable, String commitTime) throws IOException {
    int numRecords = 10 + random.nextInt(10);
    List<HoodieRecord> records = updatesGenFunction.apply(dataGen, commitTime, numRecords);
    mainTable.updatePreviousGeneration(records, commitTime, 1);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, PARALLELISM);
    return updatesFunction.apply(mainTable.client, writeRecords, commitTime);
  }

  private JavaRDD<WriteStatus> deleteDataIntoMainTable(TestTableContents mainTable, String commitTime) throws IOException {
    int numRecords = 5 + random.nextInt(10);
    List<HoodieKey> keys = deletesGenFunction.apply(dataGen, numRecords);
    mainTable.updatePreviousGenerationForDelete(keys, commitTime);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(keys, PARALLELISM);
    return deletesFunction.apply(mainTable.client, deleteKeys, commitTime);
  }

  private JavaRDD<WriteStatus> insertDataIntoExperimentTable(TestTableContents mainTable, TestTableContents experimentTable) throws IOException {
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(mainTable.generatedRecords, PARALLELISM);
    return insertsFunction.apply(experimentTable.client, writeRecords, mainTable.commitTimeOnMainTable);
  }

  private JavaRDD<WriteStatus> updateDataIntoExperimentTable(TestTableContents mainTable, TestTableContents experimentTable) throws IOException {
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(mainTable.generatedRecords, PARALLELISM);
    return updatesFunction.apply(experimentTable.client, writeRecords, mainTable.commitTimeOnMainTable);
  }

  private JavaRDD<WriteStatus> deleteDataIntoExperimentTable(TestTableContents mainTable, TestTableContents experimentTable) throws IOException {
    JavaRDD<HoodieKey> writeKeys = jsc.parallelize(mainTable.generatedKeysForDelete, PARALLELISM);
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

    final SparkRDDWriteClient logCompactionClient;

    public TestTableContents(String basePath, String tableName, HoodieTableMetaClient metaClient,
                             HoodieWriteConfig config, SparkRDDWriteClient client) {
      this(basePath, tableName, metaClient, config, client, null);
    }

    public TestTableContents(String basePath, String tableName, HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                             SparkRDDWriteClient client, SparkRDDWriteClient logCompactionClient) {
      this.basePath = basePath;
      this.tableName = tableName;
      this.metaClient = metaClient;
      this.config = config;
      this.client = client;
      this.logCompactionClient = logCompactionClient;
    }

    private void updatePreviousGeneration(List<HoodieRecord> generatedRecords, String commitTimeOnMainTable, int previousActionType) {
      Schema schema = new Schema.Parser().parse(this.config.getSchema());
      this.generatedRecords = generatedRecords.stream().map(rec -> deepCopyAndModifyRecordKey(rec)).collect(Collectors.toList());
      this.commitTimeOnMainTable = commitTimeOnMainTable;
      this.previousActionType = previousActionType;
    }

    private HoodieRecord deepCopyAndModifyRecordKey(HoodieRecord record) {
      HoodieKey key = deepCopyAndModifyRecordKey(record.getKey());
      RawTripTestPayload payload = ((RawTripTestPayload)record.getData()).clone();
      return new HoodieAvroRecord(key, payload);
    }

    private HoodieKey deepCopyAndModifyRecordKey(HoodieKey key) {
      return new HoodieKey(key.getRecordKey() + RECORD_KEY_APPEND_VALUE, key.getPartitionPath());
    }

    private void updatePreviousGenerationForDelete(List<HoodieKey> generatedKeysForDelete, String commitTimeOnMainTable) {
      this.generatedKeysForDelete = generatedKeysForDelete.stream().map(this::deepCopyAndModifyRecordKey).collect(Collectors.toList());
      this.commitTimeOnMainTable = commitTimeOnMainTable;
      this.previousActionType = 2;
    }
  }

  private TestTableContents setupTestTable1() {
    Properties properties = new Properties();
    properties.setProperty("hoodie.parquet.small.file.limit", "0");
    HoodieWriteConfig config = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(true).build())
        .withProperties(properties)
        .build();
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, config);
    return new TestTableContents(basePath, tableName, metaClient, config, client);
  }

  private TestTableContents setupTestTable2() throws IOException {
    String tableName2 = "test-trip-table2";
    String basePath2 = createBasePathForSecondTable(secondTableBasePath);
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.NAME.key(), tableName2);

    // Create metaclient
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.init(storageConf, basePath2,
        HoodieTableType.MERGE_ON_READ, properties);
    HoodieWriteConfig config2 = getConfigBuilderForSecondTable(tableName2, basePath2,
        TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.INMEMORY)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withInlineCompaction(true).build())
        .build();

    // Create writeClient
    SparkRDDWriteClient client2 = new SparkRDDWriteClient(context, config2);

    // Create logcompaction client.
    HoodieWriteConfig logCompactionConfig = HoodieWriteConfig.newBuilder().withProps(config2.getProps())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withLogCompactionBlocksThreshold(2).build())
        .build();
    SparkRDDWriteClient logCompactionClient = new SparkRDDWriteClient(context, logCompactionConfig);

    return new TestTableContents(basePath2, tableName2, metaClient2, config2, client2, logCompactionClient);
  }

  private String createBasePathForSecondTable(java.nio.file.Path secondTableBasePath) throws IOException {
    java.nio.file.Path basePath = secondTableBasePath.resolve("dataset2");
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
