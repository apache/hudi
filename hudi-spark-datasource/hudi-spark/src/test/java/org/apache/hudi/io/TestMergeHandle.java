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

package org.apache.hudi.io;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.commit.HoodieMergeHelper;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link HoodieMergeHandle}.
 */
public class TestMergeHandle extends BaseTestHandle {

  private static final String ORDERING_FIELD = "timestamp";

  @Test
  public void testMergeHandleRLIAndSIStatsWithUpdatesAndDeletes() throws Exception {

    // delete and recreate
    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());
    Properties properties = new Properties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    properties.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), ORDERING_FIELD);
    initMetaClient(getTableType(), properties);

    // init config and table
    HoodieWriteConfig config = getHoodieWriteConfigBuilder().build();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instantTime = client.startCommit();
    List<HoodieRecord> records1 = dataGenerator.generateInserts(instantTime, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records1, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    client.commit(instantTime, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    instantTime = "001";
    int numUpdates = 10;
    List<HoodieRecord> newRecords = dataGenerator.generateUniqueUpdates(instantTime, numUpdates);
    int numDeletes = generateDeleteRecords(newRecords, dataGenerator, instantTime);
    assertTrue(numDeletes > 0);
    HoodieWriteMergeHandle mergeHandle = new HoodieWriteMergeHandle(config, instantTime, table, newRecords.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        new HoodieBaseFile(fileGroup.getAllBaseFiles().findFirst().get()), Option.empty());
    HoodieMergeHelper.newInstance().runMerge(table, mergeHandle);
    WriteStatus writeStatus = mergeHandle.writeStatus;
    // verify stats after merge
    assertEquals(100 - numDeletes, writeStatus.getStat().getNumWrites());
    assertEquals(numUpdates, writeStatus.getStat().getNumUpdateWrites());
    assertEquals(numDeletes, writeStatus.getStat().getNumDeletes());

    // verify record index stats
    // numUpdates + numDeletes - new record index updates
    assertEquals(numUpdates + numDeletes, writeStatus.getIndexStats().getWrittenRecordDelegates().size());
    int numDeletedRecordDelegates = 0;
    for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
      if (!recordDelegate.getNewLocation().isPresent()) {
        numDeletedRecordDelegates++;
      } else {
        assertTrue(recordDelegate.getNewLocation().isPresent());
        assertEquals(fileId, recordDelegate.getNewLocation().get().getFileId());
        assertEquals(instantTime, recordDelegate.getNewLocation().get().getInstantTime());
      }
    }
    assertEquals(numDeletes, numDeletedRecordDelegates);

    // verify secondary index stats
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    // 2 * numUpdates si records for old secondary keys and new secondary keys related to updates
    // numDeletes secondary keys related to deletes
    assertEquals(2 * numUpdates + numDeletes, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());
    validateSecondaryIndexStatsContent(writeStatus, numUpdates, numDeletes);
  }

  @Test
  public void testFGReaderBasedMergeHandleCommitTimeOrdering() throws IOException {

    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    properties.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "timestamp");
    initMetaClient(getTableType(), properties);

    HoodieWriteConfig config = getHoodieWriteConfigBuilder().build();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instantTime = client.startCommit();
    List<HoodieRecord> records1 = dataGenerator.generateInserts(instantTime, 10);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records1, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    client.commit(instantTime, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    instantTime = "001";
    int numUpdates = 10;
    List<HoodieRecord> newRecords = dataGenerator.generateUniqueUpdates(instantTime, numUpdates);
    GenericRecord genericRecord1 = getGenRecord(newRecords.get(0), config);
    GenericRecord genericRecord2 = getGenRecord(newRecords.get(1), config);
    GenericRecord genericRecord3 = getGenRecord(newRecords.get(2), config);

    genericRecord1.put(ORDERING_FIELD, 10L);
    genericRecord2.put(ORDERING_FIELD, 10L);
    genericRecord3.put(ORDERING_FIELD, 10L);

    List<GenericRecord> toUpdate = new ArrayList<>();
    toUpdate.add(genericRecord1);
    toUpdate.add(genericRecord2);
    toUpdate.add(genericRecord3);

    Map<String, GenericRecord> expectedRecordsMap = toUpdate.stream()
        .map(genRec -> Pair.of(genRec.get("_row_key"), genRec))
        .collect(Collectors.toMap(pair -> pair.getKey().toString(), pair -> pair.getValue()));

    List<HoodieRecord> recordsToUpdate = getHoodieRecords(OverwriteWithLatestAvroPayload.class.getName(), toUpdate, partitionPath);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(metaClient.getStorageConf(), metaClient.getTableConfig(), Option.empty(), Option.empty());
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.put(HoodieTableConfig.RECORD_MERGE_MODE.key(), "COMMIT_TIME");
    readerContext.initRecordMerger(typedProperties);
    readerContext.getRecordContext().updateRecordKeyExtractor(metaClient.getTableConfig(), false);

    FileGroupReaderBasedMergeHandle fileGroupReaderBasedMergeHandle = new FileGroupReaderBasedMergeHandle(
        config, instantTime, table, recordsToUpdate.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        Option.empty(), readerContext, HoodieRecord.HoodieRecordType.AVRO);

    fileGroupReaderBasedMergeHandle.doMerge();
    List<WriteStatus> writeStatuses = fileGroupReaderBasedMergeHandle.close();

    // read the file and validate values.
    String filePath = writeStatuses.get(0).getStat().getPath();
    String fullPath = metaClient.getBasePath() +"/" + filePath;

    List<GenericRecord> actualRecords = new ParquetUtils().readAvroRecords(metaClient.getStorage(), new StoragePath(fullPath));
    Map<String, GenericRecord> actualRecordsMap = actualRecords.stream()
        .map(genRec -> Pair.of(genRec.get("_row_key"), genRec))
        .collect(Collectors.toMap(pair -> pair.getKey().toString(), pair -> pair.getValue()));

    for (Map.Entry<String, GenericRecord> entry: expectedRecordsMap.entrySet()) {
      assertTrue(actualRecordsMap.containsKey(entry.getKey()));
      assertEquals(entry.getValue().get(ORDERING_FIELD), actualRecordsMap.get(entry.getKey()).get(ORDERING_FIELD));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING", "CUSTOM"})
  public void testFGReaderBasedMergeHandleInsertUpsertDelete(String mergeMode) throws IOException {
    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    properties.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "timestamp");
    properties.put(HoodieTableConfig.RECORD_MERGE_MODE.key(), mergeMode);
    if (mergeMode.equals(RecordMergeMode.CUSTOM.name())) {
      properties.put(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), RawTripTestPayload.class.getName());
    }
    initMetaClient(getTableType(), properties);

    HoodieWriteConfig config = getHoodieWriteConfigBuilder().build();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);

    // one round per partition
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    // init some args
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String instantTime = client.startCommit();
    List<HoodieRecord> records1 = dataGenerator.generateInserts(instantTime, 10);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records1, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
    client.commit(instantTime, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    // Generate records to delete
    instantTime = "001";
    List<HoodieRecord> newRecords = dataGenerator.generateUniqueUpdates(instantTime, 5);
    HoodieRecord record = newRecords.get(2);
    HoodieRecord deleteRecordSameOrderingValue = dataGenerator.generateDeleteRecord(record);
    record = newRecords.get(3);
    HoodieRecord deleteRecordHigherOrderingValue = dataGenerator.generateDeleteRecord(record, 1);
    record = newRecords.get(4);
    HoodieRecord deleteRecordLowerOrderingValue = dataGenerator.generateDeleteRecord(record, -1);
    List<HoodieRecord> recordsToDelete = new ArrayList<>();
    recordsToDelete.add(deleteRecordSameOrderingValue);
    recordsToDelete.add(deleteRecordLowerOrderingValue);
    recordsToDelete.add(deleteRecordHigherOrderingValue);

    // Generate records to update
    GenericRecord genericRecord1 = getGenRecord(newRecords.get(0), config);
    GenericRecord genericRecord2 = getGenRecord(newRecords.get(1), config);
    genericRecord1.put(ORDERING_FIELD, 10);
    genericRecord2.put(ORDERING_FIELD, -1);
    List<GenericRecord> toUpdate = new ArrayList<>();
    toUpdate.add(genericRecord1);
    toUpdate.add(genericRecord2);
    List<HoodieRecord> recordsToUpdate = getHoodieRecords(OverwriteWithLatestAvroPayload.class.getName(), toUpdate, partitionPath);

    List<HoodieRecord> recordsToMerge = recordsToUpdate;
    recordsToMerge.addAll(recordsToDelete);
    // Generate records to insert
    List<HoodieRecord> recordsToInsert = dataGenerator.generateInserts(instantTime, 2);
    recordsToMerge.addAll(recordsToInsert);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(metaClient.getStorageConf(), metaClient.getTableConfig(), Option.empty(), Option.empty());
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.put(HoodieTableConfig.RECORD_MERGE_MODE.key(), mergeMode);
    readerContext.initRecordMerger(typedProperties);
    readerContext.getRecordContext().updateRecordKeyExtractor(metaClient.getTableConfig(), false);

    FileGroupReaderBasedMergeHandle fileGroupReaderBasedMergeHandle = new FileGroupReaderBasedMergeHandle(
        config, instantTime, table, recordsToMerge.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        Option.empty(), readerContext, HoodieRecord.HoodieRecordType.AVRO);

    fileGroupReaderBasedMergeHandle.doMerge();
    List<WriteStatus> writeStatuses = fileGroupReaderBasedMergeHandle.close();

    // read the file and validate values.
    String filePath = writeStatuses.get(0).getStat().getPath();
    String fullPath = metaClient.getBasePath() +"/" + filePath;

    List<GenericRecord> actualRecords = new ParquetUtils().readAvroRecords(metaClient.getStorage(), new StoragePath(fullPath));
    Map<String, GenericRecord> actualRecordsMap = actualRecords.stream()
        .map(genRec -> Pair.of(genRec.get("_row_key"), genRec))
        .collect(Collectors.toMap(pair -> pair.getKey().toString(), pair -> pair.getValue()));

    Map<String, GenericRecord> expectedRecordsMap = toUpdate.stream()
        .map(genRec -> Pair.of(genRec.get("_row_key"), genRec))
        .collect(Collectors.toMap(pair -> pair.getKey().toString(), pair -> pair.getValue()));
    for (Map.Entry<String, GenericRecord> entry: expectedRecordsMap.entrySet()) {
      assertTrue(actualRecordsMap.containsKey(entry.getKey()));
      assertEquals(entry.getValue().get(ORDERING_FIELD).toString(), actualRecordsMap.get(entry.getKey()).get(ORDERING_FIELD).toString());
    }

    for (HoodieRecord rec: recordsToInsert) {
      // validate record is inserted
      assertTrue(actualRecordsMap.containsKey(rec.getRecordKey()));
    }

    int numDeletes = 3;
    for (HoodieRecord rec: recordsToDelete) {
      // validate record is deleted
      if (rec.equals(deleteRecordLowerOrderingValue) && mergeMode.equals(RecordMergeMode.EVENT_TIME_ORDERING.name())) {
        numDeletes--;
        assertTrue(actualRecordsMap.containsKey(rec.getRecordKey()));
      } else {
        assertFalse(actualRecordsMap.containsKey(rec.getRecordKey()));
      }
    }

    HoodieWriteStat stat = writeStatuses.get(0).getStat();
    assertEquals(2, stat.getNumUpdateWrites());
    assertEquals(numDeletes, stat.getNumDeletes());
    assertEquals(2, stat.getNumInserts());
  }

  HoodieWriteConfig.Builder getHoodieWriteConfigBuilder() {
    return getConfigBuilder(basePath)
        .withPopulateMetaFields(true)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withRemoteServerPort(timelineServicePort).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withEnableRecordIndex(true)
            .withStreamingWriteEnabled(true)
            .withSecondaryIndexEnabled(true)
            .withSecondaryIndexName("sec-rider")
            .withSecondaryIndexForColumn("rider")
            .build())
        .withKeyGenerator(KeyGeneratorForDataGeneratorRecords.class.getCanonicalName())
        .withSchema(TRIP_EXAMPLE_SCHEMA);
  }

  private GenericRecord getGenRecord(HoodieRecord hoodieRecord, HoodieWriteConfig config) {
    try {
     return (GenericRecord) ((HoodieRecordPayload)hoodieRecord.getData()).getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA, config.getProps()).get();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser record ", e);
    }
  }

  private List<HoodieRecord> getHoodieRecords(String payloadClass, List<GenericRecord> genericRecords, String partitionPath) {
    return genericRecords.stream().map(genericRecord -> {
      return (HoodieRecord)new HoodieAvroRecord<>(new HoodieKey(genericRecord.get("_row_key").toString(), partitionPath),
          HoodieRecordUtils.loadPayload(payloadClass, genericRecord, ORDERING_FIELD));
    }).collect(Collectors.toList());
  }
}
