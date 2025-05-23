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

package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.SCHEMA_STR;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getDeletesWithEmptyPayloadAndNewPartition;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getDeletesWithNewPartition;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getInserts;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getKeyGenProps;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getPayloadProps;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getUpdates;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_BLOOM;
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_SIMPLE;
import static org.apache.hudi.index.HoodieIndex.IndexType.RECORD_INDEX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGlobalIndexEnableUpdatePartitions extends SparkClientFunctionalTestHarness {

  @Override
  public SparkConf conf() {
    return conf(SparkClientFunctionalTestHarness.getSparkSqlConf());
  }

  private static Stream<Arguments> getTableTypeAndIndexType() {
    return Stream.of(
        Arguments.of(COPY_ON_WRITE, GLOBAL_SIMPLE),
        Arguments.of(COPY_ON_WRITE, GLOBAL_BLOOM),
        Arguments.of(COPY_ON_WRITE, RECORD_INDEX),
        Arguments.of(MERGE_ON_READ, GLOBAL_SIMPLE),
        Arguments.of(MERGE_ON_READ, GLOBAL_BLOOM),
        Arguments.of(MERGE_ON_READ, RECORD_INDEX)
    );
  }

  private static Stream<Arguments> getTableTypeAndIndexTypeUpdateOrDelete() {
    return Stream.of(
        Arguments.of(MERGE_ON_READ, RECORD_INDEX, true),
        Arguments.of(MERGE_ON_READ, RECORD_INDEX, false)
    );
  }

  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testPartitionChanges(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = DefaultHoodieRecordPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";
      final String p2 = "p2";
      final String p3 = "p3";
      final String p4 = "p4";

      // 1st batch: inserts
      String commitTimeAtEpoch0 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtEpoch0 = getInserts(totalRecords, p1, 0, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch0);
      List<WriteStatus> writeStatusesList = client.upsert(jsc().parallelize(insertsAtEpoch0, 2), commitTimeAtEpoch0).collect();
      client.commit(commitTimeAtEpoch0, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);

      // 2nd batch: normal updates same partition
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = getUpdates(insertsAtEpoch0, 5, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch5);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtEpoch5, 2), commitTimeAtEpoch5).collect();
      client.commit(commitTimeAtEpoch5, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 5);

      // 3rd batch: update all from p1 to p2
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      List<HoodieRecord> updatesAtEpoch6 = getUpdates(updatesAtEpoch5, p2, 6, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch6);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtEpoch6, 2), commitTimeAtEpoch6).collect();
      client.commit(commitTimeAtEpoch6, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 6);

      // 4th batch: update all from p2 to p3
      String commitTimeAtEpoch7 = getCommitTimeAtUTC(7);
      List<HoodieRecord> updatesAtEpoch7 = getUpdates(updatesAtEpoch6, p3, 7, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch7);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtEpoch7, 2), commitTimeAtEpoch7).collect();
      client.commit(commitTimeAtEpoch7, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p3, 7);

      // 5th batch: late update all to p4; discarded
      String commitTimeAtEpoch8 = getCommitTimeAtUTC(8);
      List<HoodieRecord> updatesAtEpoch2 = getUpdates(insertsAtEpoch0, p4, 2, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch8);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtEpoch2, 2), commitTimeAtEpoch8).collect();
      client.commit(commitTimeAtEpoch8, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p3, 7);

      // 6th batch: update all from p3 to p1
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = getUpdates(updatesAtEpoch7, p1, 9, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch9);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtEpoch9, 2), commitTimeAtEpoch9).collect();
      client.commit(commitTimeAtEpoch9, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 9);
    }
  }

  /**
   * Tests getTableTypeAndIndexTypeUpdateOrDelete
   * @throws IOException
   */
  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexTypeUpdateOrDelete")
  public void testRollbacksWithPartitionUpdate(HoodieTableType tableType, IndexType indexType, boolean isUpsert) throws IOException {
    final Class<?> payloadClass = DefaultHoodieRecordPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    TimeGenerator timeGenerator = TimeGenerators.getTimeGenerator(writeConfig.getTimeGeneratorConfig(), storageConf());
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    final int totalRecords = 8;
    final String p1 = "p1";
    final String p2 = "p2";
    final String p3 = "p3";
    List<HoodieRecord> insertsAtEpoch0 = getInserts(totalRecords, p1, 0, payloadClass);
    List<HoodieRecord> updatesAtEpoch5 = getUpdates(insertsAtEpoch0.subList(0, 4), p2, 5, payloadClass);

    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      // 1st batch: inserts
      String commitTimeAtEpoch0 = TimelineUtils.generateInstantTime(false, timeGenerator);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch0);
      List<WriteStatus> writeStatusList = client.upsert(jsc().parallelize(insertsAtEpoch0, 2), commitTimeAtEpoch0).collect();
      client.commit(commitTimeAtEpoch0, jsc().parallelize(writeStatusList));
      assertNoWriteErrors(writeStatusList);

      // 2nd batch: update 4 records from p1 to p2
      String commitTimeAtEpoch5 = TimelineUtils.generateInstantTime(false, timeGenerator);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch5);
      if (isUpsert) {
        client.commit(commitTimeAtEpoch5, client.upsert(jsc().parallelize(updatesAtEpoch5, 2), commitTimeAtEpoch5));
        readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, 0);
        readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 5);
      } else {
        client.commit(commitTimeAtEpoch5, client.delete(jsc().parallelize(updatesAtEpoch5.stream().map(hoodieRecord -> hoodieRecord.getKey()).collect(Collectors.toList()), 2), commitTimeAtEpoch5));
        readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, 0);
        readTableAndValidate(metaClient, new int[] {}, p2, 0);
      }
      // simuate crash. delete latest completed dc.
      String latestCompletedDeltaCommit = INSTANT_FILE_NAME_GENERATOR.getFileName(metaClient.reloadActiveTimeline().getCommitsAndCompactionTimeline().lastInstant().get());
      metaClient.getStorage().deleteFile(new StoragePath(metaClient.getBasePath() + "/.hoodie/timeline/" + latestCompletedDeltaCommit));
    }

    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      // re-ingest same batch
      String commitTimeAtEpoch10 = TimelineUtils.generateInstantTime(false, timeGenerator);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch10);
      if (isUpsert) {
        client.commit(commitTimeAtEpoch10, client.upsert(jsc().parallelize(updatesAtEpoch5, 2), commitTimeAtEpoch10));
        // this also tests snapshot query. We had a bug where MOR snapshot was ignoring rollbacks while determining last instant while reading log records.
        readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, 0);
        readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 5);
      } else {
        client.commit(commitTimeAtEpoch10, client.delete(jsc().parallelize(updatesAtEpoch5.stream().map(hoodieRecord -> hoodieRecord.getKey()).collect(Collectors.toList()), 2), commitTimeAtEpoch10));
        readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, 0);
        readTableAndValidate(metaClient, new int[] {}, p2, 0);
      }

      // upsert test
      // update 4 of them from p2 to p3.
      // delete test:
      // update 4 of them to p3. these are treated as new inserts since they are deleted. no changes should be seen wrt p2.
      String commitTimeAtEpoch15 = TimelineUtils.generateInstantTime(false, timeGenerator);
      List<HoodieRecord> updatesAtEpoch15 = getUpdates(updatesAtEpoch5, p3, 15, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch15);
      client.commit(commitTimeAtEpoch15, client.upsert(jsc().parallelize(updatesAtEpoch15, 2), commitTimeAtEpoch15));
      // for the same bug pointed out earlier, (ignoring rollbacks while determining last instant while reading log records), this tests the HoodieFileGroupReader.
      readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, 0);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p3, 15);

      // lets move 2 of them back to p1
      String commitTimeAtEpoch20 = TimelineUtils.generateInstantTime(false, timeGenerator);
      List<HoodieRecord> updatesAtEpoch20 = getUpdates(updatesAtEpoch5.subList(0, 2), p1, 20, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch20);
      client.commit(commitTimeAtEpoch20, client.upsert(jsc().parallelize(updatesAtEpoch20, 1), commitTimeAtEpoch20));
      // for the same bug pointed out earlier, (ignoring rollbacks while determining last instant while reading log records), this tests the HoodieFileGroupReader.
      Map<String, Long> expectedTsMap = new HashMap<>();
      Arrays.stream(new int[] {0, 1}).forEach(entry -> expectedTsMap.put(String.valueOf(entry), 20L));
      Arrays.stream(new int[] {4, 5, 6, 7}).forEach(entry -> expectedTsMap.put(String.valueOf(entry), 0L));
      readTableAndValidate(metaClient, new int[] {0, 1, 4, 5, 6, 7}, p1, expectedTsMap);
      readTableAndValidate(metaClient, new int[] {2, 3}, p3, 15);
    }
  }

  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testUpdatePartitionsThenDelete(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = DefaultHoodieRecordPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";
      final String p2 = "p2";

      // 1st batch: inserts
      String commitTimeAtEpoch0 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtEpoch0 = getInserts(totalRecords, p1, 0, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch0);
      client.commit(commitTimeAtEpoch0, client.upsert(jsc().parallelize(insertsAtEpoch0, 2), commitTimeAtEpoch0));

      // 2nd batch: normal updates same partition
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = getUpdates(insertsAtEpoch0, 5, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch5);
      client.commit(commitTimeAtEpoch5, client.upsert(jsc().parallelize(updatesAtEpoch5, 2), commitTimeAtEpoch5));
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 5);

      // 3rd batch: update all from p1 to p2
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      List<HoodieRecord> updatesAtEpoch6 = getUpdates(updatesAtEpoch5, p2, 6, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch6);
      client.commit(commitTimeAtEpoch6, client.upsert(jsc().parallelize(updatesAtEpoch6, 2), commitTimeAtEpoch6));
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 6);

      // 4th batch: delete records with id=0,1
      String commitTimeAtEpoch7 = getCommitTimeAtUTC(7);
      List<HoodieRecord> deletesAtEpoch7 = getDeletesWithNewPartition(insertsAtEpoch0.subList(0, 2), p2, 7, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch7);
      client.commit(commitTimeAtEpoch7, client.upsert(jsc().parallelize(deletesAtEpoch7, 2), commitTimeAtEpoch7));
      readTableAndValidate(metaClient, new int[] {2, 3}, p2, 6);

      // 5th batch: delete records with id=2 (set to unknown partition but still matched)
      String commitTimeAtEpoch8 = getCommitTimeAtUTC(8);
      List<HoodieRecord> deletesAtEpoch8 = getDeletesWithEmptyPayloadAndNewPartition(insertsAtEpoch0.subList(2, 3), "unknown_pt");
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch8);
      client.commit(commitTimeAtEpoch8, client.upsert(jsc().parallelize(deletesAtEpoch8, 1), commitTimeAtEpoch8));
      readTableAndValidate(metaClient, new int[] {3}, p2, 6);

      // 6th batch: update all to p1
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = getUpdates(insertsAtEpoch0, p1, 9, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch9);
      client.commit(commitTimeAtEpoch9, client.upsert(jsc().parallelize(updatesAtEpoch9, 2), commitTimeAtEpoch9));
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 9);
    }
  }
  
  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testUdpateSubsetOfRecUpdates(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = DefaultHoodieRecordPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";
      final String p2 = "p2";

      List<HoodieRecord> allInserts = getInserts(totalRecords, p1, 0, payloadClass);

      // 1st batch: insert 1,2
      String commitTimeAtEpoch0 = getCommitTimeAtUTC(0);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch0);
      client.commit(commitTimeAtEpoch0, client.upsert(jsc().parallelize(allInserts.subList(0,2), 2), commitTimeAtEpoch0));
      readTableAndValidate(metaClient, new int[] {0, 1}, p1, 0L);

      // 2nd batch: update records 1,2 and insert 3
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = getUpdates(allInserts.subList(0,3), 5, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch5);
      client.commit(commitTimeAtEpoch5, client.upsert(jsc().parallelize(updatesAtEpoch5, 2), commitTimeAtEpoch5));
      readTableAndValidate(metaClient, new int[] {0, 1, 2}, p1, getExpectedTsMap(new int[] {0, 1, 2}, new Long[] {5L, 5L, 5L}));

      // 3rd batch: update records 1,2,3 and insert 4
      String commitTimeAtEpoch10 = getCommitTimeAtUTC(10);
      List<HoodieRecord> updatesAtEpoch10 = getUpdates(allInserts, 10, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch10);
      client.commit(commitTimeAtEpoch10, client.upsert(jsc().parallelize(updatesAtEpoch10, 2), commitTimeAtEpoch10));
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, getExpectedTsMap(new int[] {0, 1, 2, 3}, new Long[] {10L, 10L, 10L, 10L}));

      // 4th batch: update all from p1 to p2
      String commitTimeAtEpoch20 = getCommitTimeAtUTC(20);
      List<HoodieRecord> updatesAtEpoch20 = getUpdates(allInserts, p2, 20, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch20);
      client.commit(commitTimeAtEpoch20, client.upsert(jsc().parallelize(updatesAtEpoch20, 2), commitTimeAtEpoch20));
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 20);
    }
  }

  private Map<String, Long> getExpectedTsMap(int[] recordKeys, Long[] expectedTses) {
    Map<String, Long> expectedTsMap = new HashMap<>();
    for (int i = 0; i < recordKeys.length; i++) {
      expectedTsMap.put(String.valueOf(recordKeys[i]), expectedTses[i]);
    }
    return expectedTsMap;
  }

  private void readTableAndValidate(HoodieTableMetaClient metaClient, int[] expectedIds, String expectedPartition, long expectedTs) {
    Map<String, Long> expectedTsMap = new HashMap<>();
    Arrays.stream(expectedIds).forEach(entry -> expectedTsMap.put(String.valueOf(entry), expectedTs));
    readTableAndValidate(metaClient, expectedIds, expectedPartition, expectedTsMap);
  }

  private void readTableAndValidate(HoodieTableMetaClient metaClient, int[] expectedIds, String expectedPartition, Map<String, Long> expectedTsMap) {
    Dataset<Row> df = spark().read().format("hudi")
        .load(metaClient.getBasePath().toString())
        .sort("id")
        .select("_hoodie_record_key", "_hoodie_partition_path", "id", "pt", "ts")
        .cache();
    int expectedCount = expectedIds.length;
    Row[] allRows = (Row[]) df.filter(String.format("pt = '%s'", expectedPartition)).collect();
    assertEquals(expectedCount, allRows.length);
    for (int i = 0; i < expectedCount; i++) {
      int expectedId = expectedIds[i];
      Row r = allRows[i];
      assertEquals(String.valueOf(expectedId), r.getString(0));
      assertEquals(expectedPartition, r.getString(1));
      assertEquals(expectedId, r.getInt(2));
      assertEquals(expectedPartition, r.getString(3));
      assertEquals(expectedTsMap.get(String.valueOf(expectedId)), r.getLong(4));
    }
    df.unpersist();
  }

  private HoodieWriteConfig getWriteConfig(Class<?> payloadClass, IndexType indexType) {
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder();
    if (indexType == IndexType.RECORD_INDEX) {
      metadataConfigBuilder.enable(true).withEnableRecordIndex(true);
    } else {
      metadataConfigBuilder.enable(false);
    }
    return getConfigBuilder(false)
        .withProperties(getKeyGenProps(payloadClass))
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(2)
        .withMetadataConfig(metadataConfigBuilder.build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(indexType)
            .bloomIndexParallelism(2)
            .withSimpleIndexParallelism(2)
            .withGlobalSimpleIndexParallelism(2)
            .withGlobalIndexReconcileParallelism(2)
            .withGlobalBloomIndexUpdatePartitionPath(true)
            .withGlobalSimpleIndexUpdatePartitionPath(true)
            .withRecordIndexUpdatePartitionPath(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4).build())
        .withSchema(SCHEMA_STR)
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withRecordMergeStrategyId(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .fromProperties(getPayloadProps(payloadClass)).build())
        .build();
  }
}
