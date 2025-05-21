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

package org.apache.hudi.io;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.AWSDmsAvroPayload;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.SCHEMA_STR;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.SCHEMA_WITH_METAFIELDS;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getDeletes;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getDeletesWithEmptyPayload;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getInserts;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getKeyGenProps;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getPayloadProps;
import static org.apache.hudi.common.testutils.HoodieAdaptablePayloadDataGenerator.getUpdates;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergedReadHandle extends SparkClientFunctionalTestHarness {

  private static Stream<Arguments> avroPayloadClasses() {
    return Stream.of(
        Arguments.of(COPY_ON_WRITE, OverwriteWithLatestAvroPayload.class),
        Arguments.of(COPY_ON_WRITE, OverwriteNonDefaultsWithLatestAvroPayload.class),
        Arguments.of(COPY_ON_WRITE, PartialUpdateAvroPayload.class),
        Arguments.of(COPY_ON_WRITE, DefaultHoodieRecordPayload.class),
        Arguments.of(COPY_ON_WRITE, AWSDmsAvroPayload.class),
        Arguments.of(COPY_ON_WRITE, MySqlDebeziumAvroPayload.class),
        Arguments.of(COPY_ON_WRITE, PostgresDebeziumAvroPayload.class),
        Arguments.of(MERGE_ON_READ, OverwriteWithLatestAvroPayload.class),
        Arguments.of(MERGE_ON_READ, OverwriteNonDefaultsWithLatestAvroPayload.class),
        Arguments.of(MERGE_ON_READ, PartialUpdateAvroPayload.class),
        Arguments.of(MERGE_ON_READ, DefaultHoodieRecordPayload.class),
        Arguments.of(MERGE_ON_READ, AWSDmsAvroPayload.class),
        Arguments.of(MERGE_ON_READ, MySqlDebeziumAvroPayload.class),
        Arguments.of(MERGE_ON_READ, PostgresDebeziumAvroPayload.class)
    );
  }

  @ParameterizedTest
  @MethodSource("avroPayloadClasses")
  public void testReadLatestRecordsWithDeletes(HoodieTableType tableType, Class<?> payloadClass) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String partition = "foo";

      // 1st batch: inserts
      String commitTimeAtEpoch0 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtEpoch0 = getInserts(totalRecords, partition, 0, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch0);
      assertNoWriteErrors(client.upsert(jsc().parallelize(insertsAtEpoch0, 1), commitTimeAtEpoch0).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords, partition, 0, payloadClass);

      // 2nd batch: normal updates
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = getUpdates(insertsAtEpoch0, 5, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch5);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch5, 1), commitTimeAtEpoch5).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords, partition, 5, payloadClass);

      // 3rd batch: delete the record with id 3 (the last one)
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch6);
      List<HoodieRecord> deletesAtEpoch6 = getDeletes(updatesAtEpoch5.subList(totalRecords - 1, totalRecords), 6, payloadClass);
      assertNoWriteErrors(client.upsert(jsc().parallelize(deletesAtEpoch6, 1), commitTimeAtEpoch6).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords - 1, partition, 5, payloadClass);

      // 4th batch: delete the record with id 2 (the 2nd last one) using EmptyHoodieRecordPayload
      String commitTimeAtEpoch7 = getCommitTimeAtUTC(7);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch7);
      List<HoodieRecord> deletesAtEpoch7 = getDeletesWithEmptyPayload(updatesAtEpoch5.subList(totalRecords - 2, totalRecords - 1));
      assertNoWriteErrors(client.upsert(jsc().parallelize(deletesAtEpoch7, 1), commitTimeAtEpoch7).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords - 2, partition, 5, payloadClass);

      // 5th batch: normal updates
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = getUpdates(updatesAtEpoch5, 9, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch9);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch9, 1), commitTimeAtEpoch9).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords, partition, 9, payloadClass);
    }
  }

  private static Stream<Arguments> avroPayloadClassesThatHonorOrdering() {
    return Stream.of(
        Arguments.of(COPY_ON_WRITE, PartialUpdateAvroPayload.class),
        Arguments.of(COPY_ON_WRITE, DefaultHoodieRecordPayload.class),
        Arguments.of(COPY_ON_WRITE, MySqlDebeziumAvroPayload.class),
        Arguments.of(COPY_ON_WRITE, PostgresDebeziumAvroPayload.class),
        Arguments.of(MERGE_ON_READ, PartialUpdateAvroPayload.class),
        Arguments.of(MERGE_ON_READ, DefaultHoodieRecordPayload.class),
        Arguments.of(MERGE_ON_READ, MySqlDebeziumAvroPayload.class),
        Arguments.of(MERGE_ON_READ, PostgresDebeziumAvroPayload.class)
    );
  }

  @ParameterizedTest
  @MethodSource("avroPayloadClassesThatHonorOrdering")
  public void testReadLatestRecordsWithLateArrivedRecords(HoodieTableType tableType, Class<?> payloadClass) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig(payloadClass);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      final String partition = "foo";

      // 1st batch: inserts
      String commitTimeAtEpoch0 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtEpoch0 = getInserts(totalRecords, partition, 0, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch0);
      assertNoWriteErrors(client.upsert(jsc().parallelize(insertsAtEpoch0, 1), commitTimeAtEpoch0).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords, partition, 0, payloadClass);

      // 2nd batch: normal updates
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = getUpdates(insertsAtEpoch0, 5, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch5);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch5, 1), commitTimeAtEpoch5).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords, partition, 5, payloadClass);

      // 3rd batch: updates with old timestamp will be discarded
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      List<HoodieRecord> updatesAtEpoch1 = getUpdates(insertsAtEpoch0, 1, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch6);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch1, 1), commitTimeAtEpoch6).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords, partition, 5, payloadClass);

      // 4th batch: normal updates
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = getUpdates(updatesAtEpoch5, 9, payloadClass);
      WriteClientTestUtils.startCommitWithTime(client, commitTimeAtEpoch9);
      assertNoWriteErrors(client.upsert(jsc().parallelize(updatesAtEpoch9, 1), commitTimeAtEpoch9).collect());
      doMergedReadAndValidate(metaClient, writeConfig, totalRecords, partition, 9, payloadClass);
    }
  }

  private void doMergedReadAndValidate(HoodieTableMetaClient metaClient, HoodieWriteConfig writeConfig,
      int totalRecords, String partition, long timestamp, Class<?> payloadClass) throws IOException {
    String orderingField = new HoodieAdaptablePayloadDataGenerator.RecordGen(payloadClass).getOrderingField();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(writeConfig, context(), metaClient);
    List<Pair<String, String>> partitionPathAndFileIDPairs = table.getHoodieView()
        .getLatestBaseFiles(partition)
        .map(baseFile -> Pair.of(partition, baseFile.getFileId()))
        .collect(Collectors.toList());
    assertEquals(1, partitionPathAndFileIDPairs.size());
    String latestCommitTime = table.getActiveTimeline().lastInstant().get().requestedTime();
    HoodieMergedReadHandle mergedReadHandle = new HoodieMergedReadHandle<>(writeConfig, Option.of(latestCommitTime), table, partitionPathAndFileIDPairs.get(0));
    List<HoodieRecord> mergedRecords = mergedReadHandle.getMergedRecords();
    assertEquals(totalRecords, mergedRecords.size());
    List<HoodieRecord> sortedMergedRecords = mergedRecords.stream()
        .sorted(Comparator.comparing(HoodieRecord::getRecordKey)).collect(Collectors.toList());
    for (int i = 0; i < sortedMergedRecords.size(); i++) {
      HoodieRecord r = sortedMergedRecords.get(i);
      assertEquals(i, Integer.parseInt(r.getRecordKey()));
      assertEquals(partition, r.getPartitionPath());
      assertEquals(payloadClass.getName(), r.getData().getClass().getName());
      Option<IndexedRecord> valueOpt = ((HoodieRecordPayload) r.getData()).getInsertValue(SCHEMA_WITH_METAFIELDS);
      assertTrue(valueOpt.isPresent());
      GenericRecord avroValue = (GenericRecord) valueOpt.get();
      assertEquals(i, Integer.parseInt(avroValue.get("id").toString()));
      assertEquals(partition, avroValue.get("pt").toString());
      assertEquals(timestamp, Long.parseLong(avroValue.get(orderingField).toString()));
    }
  }

  private HoodieWriteConfig getWriteConfig(Class<?> payloadClass) {
    return getConfigBuilder(true)
        .withProperties(getKeyGenProps(payloadClass))
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(1)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withSchema(SCHEMA_STR)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .fromProperties(getPayloadProps(payloadClass)).build())
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .build();
  }
}
