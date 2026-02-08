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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
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
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getInserts;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getKeyGenProps;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getUpdates;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_SIMPLE;
import static org.apache.hudi.index.HoodieIndex.IndexType.RECORD_INDEX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for global index with COMMIT_TIME_ORDERING merge mode.
 * This validates that the optimization to skip merging with older record versions
 * works correctly for COMMIT_TIME_ORDERING on MOR tables.
 */
public class TestGlobalIndexCommitTimeOrdering extends SparkClientFunctionalTestHarness {

  @Override
  public SparkConf conf() {
    return conf(SparkClientFunctionalTestHarness.getSparkSqlConf());
  }

  private static Stream<Arguments> getTableTypeAndIndexType() {
    return Stream.of(
        Arguments.of(COPY_ON_WRITE, RECORD_INDEX),
        Arguments.of(MERGE_ON_READ, RECORD_INDEX),
        Arguments.of(COPY_ON_WRITE, GLOBAL_SIMPLE),
        Arguments.of(MERGE_ON_READ, GLOBAL_SIMPLE)
    );
  }

  /**
   * Tests basic upserts with COMMIT_TIME_ORDERING.
   * The newer commit should always win regardless of the ts field value.
   */
  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testBasicUpsertsWithCommitTimeOrdering(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = OverwriteWithLatestAvroPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";

      // 1st batch: inserts with ts=100
      String commitTime1 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtTs100 = getInserts(totalRecords, p1, 100, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<WriteStatus> writeStatusesList = client.upsert(jsc().parallelize(insertsAtTs100, 2), commitTime1).collect();
      client.commit(commitTime1, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 100);

      // 2nd batch: updates with LOWER ts=50 (should still be visible due to COMMIT_TIME_ORDERING)
      String commitTime2 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtTs50 = getUpdates(insertsAtTs100, 50, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime2);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtTs50, 2), commitTime2).collect();
      client.commit(commitTime2, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      // With COMMIT_TIME_ORDERING, newer commit wins, so ts=50 records should be visible
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 50);

      // 3rd batch: updates with HIGHER ts=200 (should be visible)
      String commitTime3 = getCommitTimeAtUTC(10);
      List<HoodieRecord> updatesAtTs200 = getUpdates(insertsAtTs100, 200, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime3);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtTs200, 2), commitTime3).collect();
      client.commit(commitTime3, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 200);

      // 4th batch: updates with EQUAL ts=200 (should be visible as newer commit wins)
      String commitTime4 = getCommitTimeAtUTC(15);
      List<HoodieRecord> updatesAtTs200Again = getUpdates(insertsAtTs100, 200, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime4);
      writeStatusesList = client.upsert(jsc().parallelize(updatesAtTs200Again, 2), commitTime4).collect();
      client.commit(commitTime4, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 200);
    }
  }

  /**
   * Tests partition path updates with COMMIT_TIME_ORDERING.
   * Records should be moved to new partition regardless of ts field value.
   */
  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testPartitionUpdatesWithCommitTimeOrdering(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = OverwriteWithLatestAvroPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";
      final String p2 = "p2";
      final String p3 = "p3";

      // 1st batch: inserts to p1 with ts=100
      String commitTime1 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtTs100 = getInserts(totalRecords, p1, 100, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<WriteStatus> writeStatusesList = client.upsert(jsc().parallelize(insertsAtTs100, 2), commitTime1).collect();
      client.commit(commitTime1, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 100);

      // 2nd batch: move all from p1 to p2 with LOWER ts=50 (should still move due to COMMIT_TIME_ORDERING)
      String commitTime2 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesToP2AtTs50 = getUpdates(insertsAtTs100, p2, 50, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime2);
      writeStatusesList = client.upsert(jsc().parallelize(updatesToP2AtTs50, 2), commitTime2).collect();
      client.commit(commitTime2, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      // Records should be in p2 with ts=50
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 50);

      // 3rd batch: move all from p2 to p3 with ts=150
      String commitTime3 = getCommitTimeAtUTC(10);
      List<HoodieRecord> updatesToP3AtTs150 = getUpdates(updatesToP2AtTs50, p3, 150, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime3);
      writeStatusesList = client.upsert(jsc().parallelize(updatesToP3AtTs150, 2), commitTime3).collect();
      client.commit(commitTime3, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p3, 150);

      // 4th batch: move all back to p1 with ts=25 (lower than current 150, but should still take effect)
      String commitTime4 = getCommitTimeAtUTC(15);
      List<HoodieRecord> updatesToP1AtTs25 = getUpdates(updatesToP3AtTs150, p1, 25, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime4);
      writeStatusesList = client.upsert(jsc().parallelize(updatesToP1AtTs25, 2), commitTime4).collect();
      client.commit(commitTime4, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 25);
    }
  }

  /**
   * Tests delete operations with COMMIT_TIME_ORDERING.
   * Deletes should always succeed regardless of ts field value.
   */
  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testDeletesWithCommitTimeOrdering(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = OverwriteWithLatestAvroPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";

      // 1st batch: inserts with ts=100
      String commitTime1 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtTs100 = getInserts(totalRecords, p1, 100, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<WriteStatus> writeStatusesList = client.upsert(jsc().parallelize(insertsAtTs100, 2), commitTime1).collect();
      client.commit(commitTime1, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 100);

      // 2nd batch: delete records 0, 1 using delete API
      String commitTime2 = getCommitTimeAtUTC(5);
      WriteClientTestUtils.startCommitWithTime(client, commitTime2);
      List<WriteStatus> deleteStatuses = client.delete(
          jsc().parallelize(insertsAtTs100.subList(0, 2).stream()
              .map(HoodieRecord::getKey).collect(Collectors.toList()), 2),
          commitTime2).collect();
      client.commit(commitTime2, jsc().parallelize(deleteStatuses));
      assertNoWriteErrors(deleteStatuses);
      // Only records 2, 3 should remain
      readTableAndValidate(metaClient, new int[] {2, 3}, p1, 100);

      // 3rd batch: re-insert deleted records with ts=50 (lower than original, should still insert)
      String commitTime3 = getCommitTimeAtUTC(10);
      List<HoodieRecord> reInsertsAtTs50 = getInserts(2, p1, 50, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime3);
      writeStatusesList = client.upsert(jsc().parallelize(reInsertsAtTs50, 2), commitTime3).collect();
      client.commit(commitTime3, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      // Records 0, 1 should be back with ts=50, records 2, 3 still have ts=100
      Map<String, Long> expectedTsMap = new HashMap<>();
      expectedTsMap.put("0", 50L);
      expectedTsMap.put("1", 50L);
      expectedTsMap.put("2", 100L);
      expectedTsMap.put("3", 100L);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, expectedTsMap);
    }
  }

  /**
   * Tests deletes using empty payload with partition path different from actual location.
   * The global index should find the record and delete it.
   */
  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testDeleteWithUnknownPartitionAndCommitTimeOrdering(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = OverwriteWithLatestAvroPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";

      // 1st batch: inserts with ts=100
      String commitTime1 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtTs100 = getInserts(totalRecords, p1, 100, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<WriteStatus> writeStatusesList = client.upsert(jsc().parallelize(insertsAtTs100, 2), commitTime1).collect();
      client.commit(commitTime1, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 100);

      // 2nd batch: delete record 0 with unknown partition (global index should find it)
      String commitTime2 = getCommitTimeAtUTC(5);
      List<HoodieRecord> deletesWithUnknownPartition = getDeletesWithEmptyPayloadAndNewPartition(
          insertsAtTs100.subList(0, 1), "unknown_partition");
      WriteClientTestUtils.startCommitWithTime(client, commitTime2);
      writeStatusesList = client.upsert(jsc().parallelize(deletesWithUnknownPartition, 1), commitTime2).collect();
      client.commit(commitTime2, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      // Record 0 should be deleted, records 1, 2, 3 should remain
      readTableAndValidate(metaClient, new int[] {1, 2, 3}, p1, 100);
    }
  }

  /**
   * Tests mixed operations: inserts, updates with partition changes, and deletes.
   */
  @ParameterizedTest
  @MethodSource("getTableTypeAndIndexType")
  public void testMixedOperationsWithCommitTimeOrdering(HoodieTableType tableType, IndexType indexType) throws IOException {
    final Class<?> payloadClass = OverwriteWithLatestAvroPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass, indexType);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 8;
      final String p1 = "p1";
      final String p2 = "p2";

      // 1st batch: insert 8 records to p1
      String commitTime1 = getCommitTimeAtUTC(0);
      List<HoodieRecord> inserts = getInserts(totalRecords, p1, 100, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<WriteStatus> writeStatusesList = client.upsert(jsc().parallelize(inserts, 2), commitTime1).collect();
      client.commit(commitTime1, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3, 4, 5, 6, 7}, p1, 100);

      // 2nd batch: move records 0-3 to p2 with lower ts=50
      String commitTime2 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesToP2 = getUpdates(inserts.subList(0, 4), p2, 50, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime2);
      writeStatusesList = client.upsert(jsc().parallelize(updatesToP2, 2), commitTime2).collect();
      client.commit(commitTime2, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, 100);
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 50);

      // 3rd batch: delete records 0, 1 from p2
      String commitTime3 = getCommitTimeAtUTC(10);
      WriteClientTestUtils.startCommitWithTime(client, commitTime3);
      List<WriteStatus> deleteStatuses = client.delete(
          jsc().parallelize(inserts.subList(0, 2).stream()
              .map(HoodieRecord::getKey).collect(Collectors.toList()), 2),
          commitTime3).collect();
      client.commit(commitTime3, jsc().parallelize(deleteStatuses));
      assertNoWriteErrors(deleteStatuses);
      readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, 100);
      readTableAndValidate(metaClient, new int[] {2, 3}, p2, 50);

      // 4th batch: update records 4, 5 in p1 with higher ts=200
      String commitTime4 = getCommitTimeAtUTC(15);
      List<HoodieRecord> updatesInP1 = getUpdates(inserts.subList(4, 6), 200, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime4);
      writeStatusesList = client.upsert(jsc().parallelize(updatesInP1, 2), commitTime4).collect();
      client.commit(commitTime4, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);
      Map<String, Long> expectedP1TsMap = new HashMap<>();
      expectedP1TsMap.put("4", 200L);
      expectedP1TsMap.put("5", 200L);
      expectedP1TsMap.put("6", 100L);
      expectedP1TsMap.put("7", 100L);
      readTableAndValidate(metaClient, new int[] {4, 5, 6, 7}, p1, expectedP1TsMap);
      readTableAndValidate(metaClient, new int[] {2, 3}, p2, 50);
    }
  }

  /**
   * Tests that records are properly handled when compaction occurs on MOR tables.
   */
  @Test
  public void testCompactionWithCommitTimeOrderingMOR() throws IOException {
    final Class<?> payloadClass = OverwriteWithLatestAvroPayload.class;
    HoodieWriteConfig writeConfig = getWriteConfigWithInlineCompaction(payloadClass, RECORD_INDEX);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(MERGE_ON_READ, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String p1 = "p1";

      // 1st batch: inserts
      String commitTime1 = getCommitTimeAtUTC(0);
      List<HoodieRecord> inserts = getInserts(totalRecords, p1, 100, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<WriteStatus> writeStatusesList = client.upsert(jsc().parallelize(inserts, 2), commitTime1).collect();
      client.commit(commitTime1, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);

      // 2nd batch: updates with lower ts
      String commitTime2 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updates1 = getUpdates(inserts, 50, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime2);
      writeStatusesList = client.upsert(jsc().parallelize(updates1, 2), commitTime2).collect();
      client.commit(commitTime2, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);

      // 3rd batch: more updates - this should trigger inline compaction (configured for 2 delta commits)
      String commitTime3 = getCommitTimeAtUTC(10);
      List<HoodieRecord> updates2 = getUpdates(inserts, 75, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTime3);
      writeStatusesList = client.upsert(jsc().parallelize(updates2, 2), commitTime3).collect();
      client.commit(commitTime3, jsc().parallelize(writeStatusesList));
      assertNoWriteErrors(writeStatusesList);

      // Verify the latest values are visible (ts=75 from the 3rd commit)
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 75);
    }
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
    assertEquals(expectedCount, allRows.length, "Expected " + expectedCount + " records in partition " + expectedPartition);
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
      metadataConfigBuilder.enable(true).withEnableGlobalRecordLevelIndex(true);
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
        .withRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING)
        .build();
  }

  private HoodieWriteConfig getWriteConfigWithInlineCompaction(Class<?> payloadClass, IndexType indexType) {
    HoodieMetadataConfig.Builder metadataConfigBuilder = HoodieMetadataConfig.newBuilder();
    if (indexType == IndexType.RECORD_INDEX) {
      metadataConfigBuilder.enable(true).withEnableGlobalRecordLevelIndex(true);
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
            .withInlineCompaction(true)
            .withMaxNumDeltaCommitsBeforeCompaction(2).build())
        .withSchema(SCHEMA_STR)
        .withRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING)
        .build();
  }
}