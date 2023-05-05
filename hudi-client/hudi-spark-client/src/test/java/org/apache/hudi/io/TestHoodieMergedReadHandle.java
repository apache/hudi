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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
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
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.avro.HoodieAvroUtils.createHoodieRecordFromAvro;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.getCommitTimeAtUTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergedReadHandle extends SparkClientFunctionalTestHarness {

  private HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testReadLatestRecordsWithDeletes(HoodieTableType tableType) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      // 1st batch: inserts
      String commitTimeAtEpoch0 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtEpoch0 = convertPayload(dataGen
          .generateInsertsForPartition(commitTimeAtEpoch0, totalRecords, DEFAULT_FIRST_PARTITION_PATH), DefaultHoodieRecordPayload.class);
      client.startCommitWithTime(commitTimeAtEpoch0);
      client.upsert(jsc().parallelize(insertsAtEpoch0, 1), commitTimeAtEpoch0);
      validate(metaClient, writeConfig, totalRecords, commitTimeAtEpoch0, DEFAULT_FIRST_PARTITION_PATH, 0);

      // 2nd batch: normal updates
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = convertPayload(dataGen
          .generateUpdatesWithTimestamp(commitTimeAtEpoch5, insertsAtEpoch0, 5), DefaultHoodieRecordPayload.class);
      client.startCommitWithTime(commitTimeAtEpoch5);
      client.upsert(jsc().parallelize(updatesAtEpoch5, 1), commitTimeAtEpoch5);
      validate(metaClient, writeConfig, totalRecords, commitTimeAtEpoch5, DEFAULT_FIRST_PARTITION_PATH, 5);

      // 3rd batch: delete a record
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      client.startCommitWithTime(commitTimeAtEpoch6);
      List<HoodieRecord> deletesAtEpoch6 = convertPayload(dataGen
          .generateUpdatesWithTimestamp(commitTimeAtEpoch6, Collections.singletonList(insertsAtEpoch0.get(0)), 6), DefaultHoodieRecordPayload.class, true);
      client.upsert(jsc().parallelize(deletesAtEpoch6, 1), commitTimeAtEpoch6);
      validate(metaClient, writeConfig, totalRecords - 1, commitTimeAtEpoch5, DEFAULT_FIRST_PARTITION_PATH, 5);

      // 4th batch: normal updates
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = convertPayload(dataGen
          .generateUpdatesWithTimestamp(commitTimeAtEpoch9, insertsAtEpoch0, 9), DefaultHoodieRecordPayload.class);
      client.startCommitWithTime(commitTimeAtEpoch9);
      client.upsert(jsc().parallelize(updatesAtEpoch9, 1), commitTimeAtEpoch9);
      validate(metaClient, writeConfig, totalRecords, commitTimeAtEpoch9, DEFAULT_FIRST_PARTITION_PATH, 9);

    }
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testReadLatestRecordsWithLateArrivedRecords(HoodieTableType tableType) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    HoodieTableMetaClient metaClient = getHoodieMetaClient(tableType, writeConfig.getProps());
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      final int totalRecords = 4;
      // 1st batch: inserts
      String commitTimeAtEpoch0 = getCommitTimeAtUTC(0);
      List<HoodieRecord> insertsAtEpoch0 = convertPayload(dataGen
          .generateInsertsForPartition(commitTimeAtEpoch0, totalRecords, DEFAULT_FIRST_PARTITION_PATH), DefaultHoodieRecordPayload.class);
      client.startCommitWithTime(commitTimeAtEpoch0);
      client.upsert(jsc().parallelize(insertsAtEpoch0, 1), commitTimeAtEpoch0);
      validate(metaClient, writeConfig, totalRecords, commitTimeAtEpoch0, DEFAULT_FIRST_PARTITION_PATH, 0);

      // 2nd batch: normal updates
      String commitTimeAtEpoch5 = getCommitTimeAtUTC(5);
      List<HoodieRecord> updatesAtEpoch5 = convertPayload(dataGen
          .generateUpdatesWithTimestamp(commitTimeAtEpoch5, insertsAtEpoch0, 5), DefaultHoodieRecordPayload.class);
      client.startCommitWithTime(commitTimeAtEpoch5);
      client.upsert(jsc().parallelize(updatesAtEpoch5, 1), commitTimeAtEpoch5);
      validate(metaClient, writeConfig, totalRecords, commitTimeAtEpoch5, DEFAULT_FIRST_PARTITION_PATH, 5);

      // 3rd batch: updates with old timestamp will be discarded
      String commitTimeAtEpoch6 = getCommitTimeAtUTC(6);
      List<HoodieRecord> updatesAtEpoch1 = convertPayload(dataGen
          .generateUpdatesWithTimestamp(commitTimeAtEpoch6, insertsAtEpoch0, 1), DefaultHoodieRecordPayload.class);
      client.startCommitWithTime(commitTimeAtEpoch6);
      client.upsert(jsc().parallelize(updatesAtEpoch1, 1), commitTimeAtEpoch6);
      validate(metaClient, writeConfig, totalRecords, commitTimeAtEpoch5, DEFAULT_FIRST_PARTITION_PATH, 5);

      // 4th batch: normal updates
      String commitTimeAtEpoch9 = getCommitTimeAtUTC(9);
      List<HoodieRecord> updatesAtEpoch9 = convertPayload(dataGen
          .generateUpdatesWithTimestamp(commitTimeAtEpoch9, insertsAtEpoch0, 9), DefaultHoodieRecordPayload.class);
      client.startCommitWithTime(commitTimeAtEpoch9);
      client.upsert(jsc().parallelize(updatesAtEpoch9, 1), commitTimeAtEpoch9);
      validate(metaClient, writeConfig, totalRecords, commitTimeAtEpoch9, DEFAULT_FIRST_PARTITION_PATH, 9);
    }
  }

  private void validate(HoodieTableMetaClient metaClient, HoodieWriteConfig writeConfig,
      int totalRecords, String commitTime, String partition, long timestamp) throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(writeConfig, context(), metaClient);
    List<Pair<String, String>> partitionPathAndFileIDPairs = table.getHoodieView()
        .getLatestBaseFiles(partition)
        .map(baseFile -> Pair.of(partition, baseFile.getFileId()))
        .collect(Collectors.toList());
    assertEquals(1, partitionPathAndFileIDPairs.size());
    String latestCommitTime = table.getActiveTimeline().lastInstant().get().getTimestamp();
    HoodieMergedReadHandle mergedReadHandle = new HoodieMergedReadHandle<>(writeConfig, Option.of(latestCommitTime), table, partitionPathAndFileIDPairs.get(0));
    List<HoodieRecord> mergedRecords = mergedReadHandle.getMergedRecords();
    assertEquals(totalRecords, mergedRecords.size());
    for (HoodieRecord rec : mergedRecords) {
      HoodieRecord r = rec.wrapIntoHoodieRecordPayloadWithParams(
          addMetadataFields(AVRO_SCHEMA, writeConfig.allowOperationMetadataField()),
          writeConfig.getProps(), Option.empty(), writeConfig.allowOperationMetadataField(), Option.empty(), false, Option.of(AVRO_SCHEMA));
      assertEquals(partition, r.getPartitionPath());
      assertEquals(DefaultHoodieRecordPayload.class, r.getData().getClass());
      DefaultHoodieRecordPayload data = (DefaultHoodieRecordPayload) r.getData();
      assertEquals(timestamp, data.getOrderingVal());
      Option<IndexedRecord> valueOpt = data.getInsertValue(AVRO_SCHEMA);
      assertTrue(valueOpt.isPresent());
      GenericRecord avroValue = (GenericRecord) valueOpt.get();
      assertEquals("rider-" + commitTime, avroValue.get("rider").toString());
      assertEquals("driver-" + commitTime, avroValue.get("driver").toString());
      assertEquals(timestamp, Long.parseLong(avroValue.get("timestamp").toString()));
    }
  }

  private List<HoodieRecord> convertPayload(List<HoodieRecord> records, Class<?> payloadClazz) throws IOException {
    return convertPayload(records, payloadClazz, false);
  }

  private List<HoodieRecord> convertPayload(List<HoodieRecord> records, Class<?> payloadClazz, boolean isDeleted) throws IOException {
    List<HoodieRecord> convertedRecords = new ArrayList<>();
    for (HoodieRecord r : records) {
      GenericRecord avroData = (GenericRecord) ((RawTripTestPayload) r.getData()).getRecordToInsert(AVRO_SCHEMA);
      avroData.put("_hoodie_is_deleted", isDeleted);
      convertedRecords.add(
          createHoodieRecordFromAvro(avroData, payloadClazz.getName(),
              "timestamp", Option.of(Pair.of("_row_key", "partition_path")),
              false, Option.empty(), false, Option.of(AVRO_SCHEMA)));
    }
    return convertedRecords;
  }

  private HoodieWriteConfig getWriteConfig() {
    Properties properties = new Properties();
    properties.put("hoodie.datasource.write.recordkey.field", "_row_key");
    properties.put("hoodie.datasource.write.partitionpath.field", "partition_path");
    properties.put("hoodie.datasource.write.precombine.field", "timestamp");
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    properties.put(HoodieTableConfig.PRECOMBINE_FIELD.key(), "timestamp");
    return getConfigBuilder(true)
        .withProperties(properties)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withDeleteParallelism(1)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .withPayloadClass(DefaultHoodieRecordPayload.class.getName())
            .withPayloadOrderingField("timestamp").build())
        .build();
  }
}
