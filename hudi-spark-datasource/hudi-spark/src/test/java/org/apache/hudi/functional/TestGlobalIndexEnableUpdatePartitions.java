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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
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
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_BLOOM;
import static org.apache.hudi.index.HoodieIndex.IndexType.GLOBAL_SIMPLE;
import static org.apache.hudi.index.HoodieIndex.IndexType.RECORD_INDEX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGlobalIndexEnableUpdatePartitions extends SparkClientFunctionalTestHarness {

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
      client.startCommitWithTime(commitTimeAtEpoch0);
      assertNoWriteErrors(client.upsert(jsc().parallelize(insertsAtEpoch0, 2), commitTimeAtEpoch0).collect());

      // 2nd batch: normal updates same partition
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = getUpdates(insertsAtEpoch0, 5, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch5);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch5, 2), commitTimeAtEpoch5).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 5);

      // 3rd batch: update all from p1 to p2
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      List<HoodieRecord> updatesAtEpoch6 = getUpdates(updatesAtEpoch5, p2, 6, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch6);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch6, 2), commitTimeAtEpoch6).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 6);

      // 4th batch: update all from p2 to p3
      String commitTimeAtEpoch7 = getCommitTimeAtUTC(7);
      List<HoodieRecord> updatesAtEpoch7 = getUpdates(updatesAtEpoch6, p3, 7, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch7);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch7, 2), commitTimeAtEpoch7).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p3, 7);

      // 5th batch: late update all to p4; discarded
      String commitTimeAtEpoch8 = getCommitTimeAtUTC(8);
      List<HoodieRecord> updatesAtEpoch2 = getUpdates(insertsAtEpoch0, p4, 2, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch8);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch2, 2), commitTimeAtEpoch8).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p3, 7);

      // 6th batch: update all from p3 to p1
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = getUpdates(updatesAtEpoch7, p1, 9, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch9);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch9, 2), commitTimeAtEpoch9).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 9);
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
      client.startCommitWithTime(commitTimeAtEpoch0);
      assertNoWriteErrors(client.upsert(jsc().parallelize(insertsAtEpoch0, 2), commitTimeAtEpoch0).collect());

      // 2nd batch: normal updates same partition
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = getUpdates(insertsAtEpoch0, 5, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch5);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch5, 2), commitTimeAtEpoch5).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 5);

      // 3rd batch: update all from p1 to p2
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      List<HoodieRecord> updatesAtEpoch6 = getUpdates(updatesAtEpoch5, p2, 6, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch6);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch6, 2), commitTimeAtEpoch6).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p2, 6);

      // 4th batch: delete records with id=0,1
      String commitTimeAtEpoch7 = getCommitTimeAtUTC(7);
      List<HoodieRecord> deletesAtEpoch7 = getDeletesWithNewPartition(insertsAtEpoch0.subList(0, 2), p2, 7, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch7);
      assertNoWriteErrors(client.upsert(jsc().parallelize(deletesAtEpoch7, 2), commitTimeAtEpoch7).collect());
      readTableAndValidate(metaClient, new int[] {2, 3}, p2, 6);

      // 5th batch: delete records with id=2 (set to unknown partition but still matched)
      String commitTimeAtEpoch8 = getCommitTimeAtUTC(8);
      List<HoodieRecord> deletesAtEpoch8 = getDeletesWithEmptyPayloadAndNewPartition(insertsAtEpoch0.subList(2, 3), "unknown_pt");
      client.startCommitWithTime(commitTimeAtEpoch8);
      assertNoWriteErrors(client.upsert(jsc().parallelize(deletesAtEpoch8, 1), commitTimeAtEpoch8).collect());
      readTableAndValidate(metaClient, new int[] {3}, p2, 6);

      // 6th batch: update all to p1
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = getUpdates(insertsAtEpoch0, p1, 9, payloadClass);
      client.startCommitWithTime(commitTimeAtEpoch9);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch9, 2), commitTimeAtEpoch9).collect());
      readTableAndValidate(metaClient, new int[] {0, 1, 2, 3}, p1, 9);
    }
  }

  private void readTableAndValidate(HoodieTableMetaClient metaClient, int[] expectedIds, String expectedPartition, long expectedTs) {
    Dataset<Row> df = spark().read().format("hudi")
        .load(metaClient.getBasePathV2().toString())
        .sort("id")
        .select("_hoodie_record_key", "_hoodie_partition_path", "id", "pt", "ts")
        .cache();
    int expectedCount = expectedIds.length;
    assertEquals(expectedCount, df.count());
    assertEquals(expectedCount, df.filter(String.format("pt = '%s'", expectedPartition)).count());
    Row[] allRows = (Row[]) df.collect();
    for (int i = 0; i < expectedCount; i++) {
      int expectedId = expectedIds[i];
      Row r = allRows[i];
      assertEquals(String.valueOf(expectedId), r.getString(0));
      assertEquals(expectedPartition, r.getString(1));
      assertEquals(expectedId, r.getInt(2));
      assertEquals(expectedPartition, r.getString(3));
      assertEquals(expectedTs, r.getLong(4));
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
    return getConfigBuilder(true)
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
        .withSchema(SCHEMA_STR)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .fromProperties(getPayloadProps(payloadClass)).build())
        .build();
  }
}
