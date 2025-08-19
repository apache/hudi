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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.cdc.HoodieCDCUtils.schemaBySupplementalLoggingMode;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.AssertionsKt.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests {@link HoodieMergeHandle}.
 */
public class TestMergeHandle extends BaseTestHandle {

  private static final String ORDERING_FIELD = "timestamp";

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeHandleRLIAndSIStatsWithUpdatesAndDeletes(boolean useFileGroupReader) throws Exception {
    // delete and recreate
    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());
    Properties properties = new Properties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    properties.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), ORDERING_FIELD);
    initMetaClient(getTableType(), properties);

    // init config and table
    HoodieWriteConfig config = getHoodieWriteConfigBuilder().build();
    HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);

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
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    instantTime = "001";
    int numUpdates = 10;
    List<HoodieRecord> newRecords = dataGenerator.generateUniqueUpdates(instantTime, numUpdates);
    int numDeletes = generateDeleteRecords(newRecords, dataGenerator, instantTime);
    assertTrue(numDeletes > 0);
    HoodieWriteMergeHandle mergeHandle;
    if (useFileGroupReader) {
      mergeHandle = new FileGroupReaderBasedMergeHandle(config, instantTime, table, newRecords.iterator(), partitionPath, fileId,
          new LocalTaskContextSupplier(), Option.empty());
    } else {
      mergeHandle = new HoodieWriteMergeHandle(config, instantTime, table, newRecords.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
          new HoodieBaseFile(fileGroup.getAllBaseFiles().findFirst().get()), Option.empty());
    }
    mergeHandle.doMerge();
    WriteStatus writeStatus = (WriteStatus) mergeHandle.close().get(0);
    // verify stats after merge
    int deletesWithIgnoreIndexUpdate = 5;
    int expectedNumDelegatesWithIgnoreIndexUpdate = useFileGroupReader ? 0 : deletesWithIgnoreIndexUpdate;
    assertEquals(100 - numDeletes, writeStatus.getStat().getNumWrites());
    assertEquals(numUpdates, writeStatus.getStat().getNumUpdateWrites());
    assertEquals(numDeletes, writeStatus.getStat().getNumDeletes());

    // verify record index stats
    // numUpdates + numDeletes - new record index updates
    assertEquals(numUpdates + numDeletes - (deletesWithIgnoreIndexUpdate - expectedNumDelegatesWithIgnoreIndexUpdate), writeStatus.getIndexStats().getWrittenRecordDelegates().size());
    int numDeletedRecordDelegates = 0;
    int numDeletedRecordDelegatesWithIgnoreIndexUpdate = 0;
    for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
      if (!recordDelegate.getNewLocation().isPresent()) {
        numDeletedRecordDelegates++;
        if (recordDelegate.getIgnoreIndexUpdate()) {
          numDeletedRecordDelegatesWithIgnoreIndexUpdate++;
        }
      } else {
        assertTrue(recordDelegate.getNewLocation().isPresent());
        assertEquals(fileId, recordDelegate.getNewLocation().get().getFileId());
        assertEquals(instantTime, recordDelegate.getNewLocation().get().getInstantTime());
      }
    }
    // 5 of the deletes are marked with ignoreIndexUpdate in generateDeleteRecords
    assertEquals(expectedNumDelegatesWithIgnoreIndexUpdate, numDeletedRecordDelegatesWithIgnoreIndexUpdate);
    assertEquals(numDeletes - (deletesWithIgnoreIndexUpdate - expectedNumDelegatesWithIgnoreIndexUpdate), numDeletedRecordDelegates);

    // verify secondary index stats
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    // 2 * numUpdates si records for old secondary keys and new secondary keys related to updates
    // numDeletes secondary keys related to deletes
    assertEquals(2 * numUpdates + numDeletes, writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().findFirst().get().size());
    validateSecondaryIndexStatsContent(writeStatus, numUpdates, numDeletes);
  }

  @Test
  void testWriteFailures() throws Exception {
    // delete and recreate
    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());
    Properties properties = new Properties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    properties.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), ORDERING_FIELD);
    properties.put(HoodieTableConfig.CDC_ENABLED.key(), "true");
    properties.put(HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key(), HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY.name());
    initMetaClient(getTableType(), properties);

    // init config and table
    HoodieWriteConfig config = getHoodieWriteConfigBuilder().build();
    HoodieSparkTable.create(config, new HoodieLocalEngineContext(storageConf), metaClient);

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
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    instantTime = client.startCommit();
    List<HoodieRecord> updates = dataGenerator.generateUniqueUpdates(instantTime, 10);
    FileGroupReaderBasedMergeHandle fileGroupReaderBasedMergeHandle = new FileGroupReaderBasedMergeHandle(
        config, instantTime, table, updates.iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        Option.empty());

    String recordKeyForFailure = updates.get(5).getRecordKey();
    try (MockedStatic<HoodieAvroUtils> mockedStatic = mockStatic(HoodieAvroUtils.class, Mockito.withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS))) {
      int position = AVRO_SCHEMA.getField("_row_key").pos();
      mockedStatic.when(() -> HoodieAvroUtils.rewriteRecordWithNewSchema(any(), any())).thenAnswer(invocationOnMock -> {
        IndexedRecord record = invocationOnMock.getArgument(0);
        if (record.get(position).toString().equals(recordKeyForFailure)) {
          throw new HoodieIOException("Simulated write failure for record key: " + recordKeyForFailure);
        }
        return HoodieAvroUtils.rewriteRecordWithNewSchema((IndexedRecord) invocationOnMock.getArgument(0), invocationOnMock.getArgument(1), Collections.emptyMap());
      });
      fileGroupReaderBasedMergeHandle.doMerge();
    }

    List<WriteStatus> writeStatuses = fileGroupReaderBasedMergeHandle.close();
    WriteStatus writeStatus = writeStatuses.get(0);
    assertEquals(1, writeStatus.getErrors().size());
    // check that record and secondary index stats are non-empty
    assertFalse(writeStatus.getWrittenRecordDelegates().isEmpty());
    assertFalse(writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().flatMap(Collection::stream).count() == 0L);

    writeStatus.getWrittenRecordDelegates().forEach(recordDelegate -> assertNotEquals(recordKeyForFailure, recordDelegate.getRecordKey()));
    writeStatus.getIndexStats().getSecondaryIndexStats().values().stream().flatMap(Collection::stream)
        .forEach(secondaryIndexStats -> assertNotEquals(recordKeyForFailure, secondaryIndexStats.getRecordKey()));

    AtomicBoolean cdcRecordsFound = new AtomicBoolean(false);
    String cdcFilePath = metaClient.getBasePath().toString() + "/" + writeStatus.getStat().getCdcStats().keySet().stream().findFirst().get();
    Schema cdcSchema = schemaBySupplementalLoggingMode(HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY, AVRO_SCHEMA);
    int recordKeyFieldIndex = cdcSchema.getField("record_key").pos();
    try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(storage, new HoodieLogFile(cdcFilePath), cdcSchema)) {
      while (reader.hasNext()) {
        HoodieLogBlock logBlock = reader.next();
        if (logBlock instanceof HoodieDataBlock) {
          cdcRecordsFound.set(true);
          try (ClosableIterator<HoodieRecord<IndexedRecord>> itr = ((HoodieDataBlock) logBlock).getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
            itr.forEachRemaining(record -> assertNotEquals(recordKeyForFailure, record.getData().get(recordKeyFieldIndex)));
          }
        }
      }
    }
    assertTrue(cdcRecordsFound.get(), "No CDC records were processed, validate test setup");
  }

  @ParameterizedTest
  @ValueSource(strings = {"EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING", "CUSTOM", "CUSTOM_MERGER"})
  public void testFGReaderBasedMergeHandleInsertUpsertDelete(String mergeMode) throws IOException {
    testFGReaderBasedMergeHandleInsertUpsertDeleteInternal(mergeMode, new Properties(), false);
  }

  @Test
  public void testFGReaderBasedMergeHandleEventTimeMetadata() throws IOException {
    Properties properties = new Properties();
    properties.put("hoodie.write.track.event.time.watermark", "true");
    properties.put("hoodie.payload.event.time.field", "current_ts");
    testFGReaderBasedMergeHandleInsertUpsertDeleteInternal("EVENT_TIME_ORDERING", properties, true);
  }

  private void testFGReaderBasedMergeHandleInsertUpsertDeleteInternal(String mergeMode, Properties writerProps, boolean validateEventTimeMetadata) throws IOException {
    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());

    HoodieWriteConfig config = getHoodieWriteConfigBuilder().withProperties(writerProps).build();
    TypedProperties properties = new TypedProperties();
    writerProps.keySet().forEach((key -> properties.put(key, writerProps.get(key))));

    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    properties.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    properties.put(HoodieTableConfig.PRECOMBINE_FIELDS.key(), ORDERING_FIELD);
    properties.put(HoodieTableConfig.RECORD_MERGE_MODE.key(), mergeMode);
    if (mergeMode.equals("CUSTOM_MERGER")) {
      config.setValue(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES, CustomMerger.class.getName());
      properties.put(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), CustomMerger.getStrategyId());
      properties.put(HoodieTableConfig.RECORD_MERGE_MODE.key(), "CUSTOM");
    }
    String payloadClass = null;
    if (mergeMode.equals(RecordMergeMode.CUSTOM.name()) || mergeMode.equals("CUSTOM_MERGER")) {
      // set payload class as part of table properties.
      properties.put(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), CustomPayload.class.getName());
      payloadClass = CustomPayload.class.getName();
    } else if (mergeMode.equals(RecordMergeMode.EVENT_TIME_ORDERING.name())) {
      payloadClass = DefaultHoodieRecordPayload.class.getName();
    } else if (mergeMode.equals(RecordMergeMode.COMMIT_TIME_ORDERING.name())) {
      payloadClass = OverwriteWithLatestAvroPayload.class.getName();
    }
    initMetaClient(getTableType(), properties);

    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(new String[] {partitionPath});
    // initial write
    List<HoodieRecord> recordsBatch1 = initialWrite(config, dataGenerator, payloadClass, partitionPath);
    Map<String, HoodieRecord> recordsBatch1Map = recordsBatch1.stream().map(record -> Pair.of(record.getRecordKey(), record))
        .collect(Collectors.toMap(pair -> pair.getKey(), pair -> pair.getValue()));

    metaClient = HoodieTableMetaClient.reload(metaClient);
    String commit1 = metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().getInstants().get(0).requestedTime();
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkCopyOnWriteTable.create(config, context, metaClient);
    HoodieFileGroup fileGroup = table.getFileSystemView().getAllFileGroups(partitionPath).collect(Collectors.toList()).get(0);
    String fileId = fileGroup.getFileGroupId().getFileId();

    String instantTime = "001";
    InputAndExpectedDataSet inputAndExpectedDataSet = prepareInputFor2ndBatch(config, dataGenerator, payloadClass, partitionPath, mergeMode, recordsBatch1, instantTime,
        fileGroup);

    Map<String, HoodieRecord> newInsertRecordsMap = inputAndExpectedDataSet.getNewInserts().stream().map(record -> Pair.of(record.getRecordKey(), record))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    setCurLocation(inputAndExpectedDataSet.getRecordsToMerge().stream().filter(record -> !newInsertRecordsMap.containsKey(record.getRecordKey())).collect(Collectors.toList()),
        fileId, commit1);
    Map<String, HoodieRecord> validUpdatesRecordsMap = inputAndExpectedDataSet.getValidUpdates().stream().map(record -> Pair.of(record.getRecordKey(), record))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    Map<String, HoodieRecord> validDeletesMap = inputAndExpectedDataSet.getValidDeletes();
    Map<String, HoodieRecord> untouchedRecordsFromBatch1 = recordsBatch1Map.entrySet().stream().filter(kv -> {
      return (!validUpdatesRecordsMap.containsKey(kv.getKey()) && !validDeletesMap.containsKey(kv.getKey()));
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    FileGroupReaderBasedMergeHandle fileGroupReaderBasedMergeHandle = new FileGroupReaderBasedMergeHandle(
        config, instantTime, table, inputAndExpectedDataSet.getRecordsToMerge().iterator(), partitionPath, fileId, new LocalTaskContextSupplier(),
        Option.empty());

    fileGroupReaderBasedMergeHandle.doMerge();
    List<WriteStatus> writeStatuses = fileGroupReaderBasedMergeHandle.close();
    WriteStatus writeStatus = writeStatuses.get(0);

    // read the file and validate values.
    String filePath = writeStatus.getStat().getPath();
    String fullPath = metaClient.getBasePath() + "/" + filePath;

    List<GenericRecord> actualRecords = new ParquetUtils().readAvroRecords(metaClient.getStorage(), new StoragePath(fullPath));
    Map<String, GenericRecord> actualRecordsMap = actualRecords.stream()
        .map(genRec -> Pair.of(genRec.get("_row_key"), genRec))
        .collect(Collectors.toMap(pair -> pair.getKey().toString(), pair -> pair.getValue()));

    for (Map.Entry<String, HoodieRecord> entry : inputAndExpectedDataSet.getExpectedRecordsMap().entrySet()) {
      assertTrue(actualRecordsMap.containsKey(entry.getKey()));
      GenericRecord genericRecord = (GenericRecord) ((HoodieRecordPayload) entry.getValue().getData()).getInsertValue(AVRO_SCHEMA, properties).get();
      assertEquals(genericRecord.get(ORDERING_FIELD).toString(), actualRecordsMap.get(entry.getKey()).get(ORDERING_FIELD).toString());
    }

    // validate that deleted records are not part of actual list
    inputAndExpectedDataSet.getValidDeletes().keySet().forEach(deletedKey -> {
      assertTrue(!actualRecordsMap.containsKey(deletedKey));
    });

    HoodieWriteStat stat = writeStatus.getStat();
    assertEquals(inputAndExpectedDataSet.getExpectedUpdates(), stat.getNumUpdateWrites());
    assertEquals(inputAndExpectedDataSet.getExpectedDeletes(), stat.getNumDeletes());
    assertEquals(2, stat.getNumInserts());

    validateWriteStatus(writeStatus, commit1, 10 - inputAndExpectedDataSet.getExpectedDeletes() + 2,
        inputAndExpectedDataSet.getExpectedUpdates(), 2, inputAndExpectedDataSet.getExpectedDeletes());

    // validate event time metadata if enabled
    if (validateEventTimeMetadata) {
      List<HoodieRecord> records = new ArrayList<>(inputAndExpectedDataSet.getExpectedRecordsMap().values());
      validateEventTimeMetadata(writeStatus, writerProps.get("hoodie.payload.event.time.field").toString(), AVRO_SCHEMA, config, properties, records);
    } else {
      validateEventTimeMetadataNotSet(writeStatus);
    }

    // validate RLI stats
    List<HoodieRecordDelegate> recordDelegates = writeStatus.getIndexStats().getWrittenRecordDelegates();
    recordDelegates.forEach(recordDelegate -> {
      if (recordDelegate.getNewLocation().isPresent() && recordDelegate.getCurrentLocation().isPresent()) {
        // updates
        // inserts are also tagged as updates. To be fixed.
        assertTrue(validUpdatesRecordsMap.containsKey(recordDelegate.getRecordKey()) || untouchedRecordsFromBatch1.containsKey(recordDelegate.getRecordKey()));
      } else if (recordDelegate.getNewLocation().isPresent() && recordDelegate.getCurrentLocation().isEmpty()) {
        // inserts
        assertTrue(newInsertRecordsMap.containsKey(recordDelegate.getRecordKey()));
      } else if (recordDelegate.getCurrentLocation().isPresent() && recordDelegate.getNewLocation().isEmpty()) {
        // deletes
        assertTrue(validDeletesMap.containsKey(recordDelegate.getRecordKey()));
      }
    });

    // validate SI stats.
    assertEquals(1, writeStatus.getIndexStats().getSecondaryIndexStats().size());
    assertEquals(inputAndExpectedDataSet.expectedDeletes + 2 * inputAndExpectedDataSet.expectedUpdates + inputAndExpectedDataSet.newInserts.size(),
        writeStatus.getIndexStats().getSecondaryIndexStats().get("secondary_index_sec-rider").size());
    for (SecondaryIndexStats secondaryIndexStat : writeStatus.getIndexStats().getSecondaryIndexStats().get("secondary_index_sec-rider")) {
      if (secondaryIndexStat.isDeleted()) {
        // Either the record is deleted or record is updated. For updated record there are two SI entries
        // one for older SI record deletion and another for new SI record creation
        assertTrue(inputAndExpectedDataSet.validDeletes.containsKey(secondaryIndexStat.getRecordKey())
            || inputAndExpectedDataSet.getValidUpdates().stream().anyMatch(rec -> rec.getRecordKey().equals(secondaryIndexStat.getRecordKey())));
      } else {
        HoodieRecord record = inputAndExpectedDataSet.expectedRecordsMap.get(secondaryIndexStat.getRecordKey());
        assertEquals(record.getColumnValueAsJava(AVRO_SCHEMA, "rider", properties).toString(),
            secondaryIndexStat.getSecondaryKeyValue().toString());
      }
    }
  }

  private void validateEventTimeMetadataNotSet(WriteStatus writeStatus) {
    assertNull(writeStatus.getStat().getMinEventTime());
    assertNull(writeStatus.getStat().getMaxEventTime());
  }

  private void validateEventTimeMetadata(WriteStatus writeStatus, String eventTimeFieldName, Schema schema, HoodieWriteConfig config,
                                         TypedProperties props, List<HoodieRecord> records) {
    long actualMinEventTime = writeStatus.getStat().getMinEventTime();
    long actualMaxEventTime = writeStatus.getStat().getMaxEventTime();
    boolean keepConsistentLogicalTimestamp = ConfigUtils.shouldKeepConsistentLogicalTimestamp(config.getProps());

    AtomicLong expectedMinValue = new AtomicLong(Long.MAX_VALUE);
    AtomicLong expectedMaxValue = new AtomicLong(Long.MIN_VALUE);

    // Append event_time.
    records.forEach(record -> {
      Object eventTimeValue = record.getColumnValueAsJava(schema, eventTimeFieldName, props);
      if (eventTimeValue != null) {
        // Append event_time.
        Option<Schema.Field> field = AvroSchemaUtils.findNestedField(schema, eventTimeFieldName);
        // Field should definitely exist.
        eventTimeValue = record.convertColumnValueForLogicalType(
            field.get().schema(), eventTimeValue, keepConsistentLogicalTimestamp);
        int length = eventTimeValue.toString().length();
        Long millisEventTime = null;
        if (length == 10) {
          millisEventTime = Long.parseLong(eventTimeValue.toString()) * 1000;
        } else if (length == 13) {
          // eventTimeVal in millis unit
          millisEventTime = Long.parseLong(eventTimeValue.toString());
        }
        long eventTime = DateTimeUtils.parseDateTime(Long.toString(millisEventTime)).toEpochMilli();
        expectedMinValue.set(Math.min(expectedMinValue.get(), eventTime));
        expectedMaxValue.set(Math.max(expectedMaxValue.get(), eventTime));
      }
    });
    assertEquals(expectedMinValue.get(), actualMinEventTime, "Min event time does not match");
    assertEquals(expectedMaxValue.get(), actualMaxEventTime, "Max event time does not match");
  }

  private List<HoodieRecord> initialWrite(HoodieWriteConfig config, HoodieTestDataGenerator dataGenerator, String payloadClass, String partitionPath) {
    List<HoodieRecord> insertRecords = null;
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String instantTime = client.startCommit();
      insertRecords = dataGenerator.generateInserts(instantTime, 10);
      insertRecords = overrideOrderingValue(insertRecords, config, payloadClass, partitionPath, 5L);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(insertRecords, 1);
      JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, instantTime);
      client.commit(instantTime, statuses, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    }
    return insertRecords;
  }

  private InputAndExpectedDataSet prepareInputFor2ndBatch(HoodieWriteConfig config, HoodieTestDataGenerator dataGenerator, String payloadClass,
                                                          String partitionPath, String mergeMode, List<HoodieRecord> recordsBatch1,
                                                          String instantTime, HoodieFileGroup fileGroup) {
    List<HoodieRecord> recordsToDelete = new ArrayList<>();
    Map<String, HoodieRecord> validDeletes = new HashMap<>();
    List<GenericRecord> recordsToUpdate = new ArrayList<>();
    List<HoodieRecord> validUpdates = new ArrayList<>();
    List<HoodieRecord> newInserts = new ArrayList<>();
    int expectedUpdates = 0;
    int expectedDeletes = 0;

    // Generate records to delete
    List<HoodieRecord> newRecords = dataGenerator.generateUniqueUpdates(instantTime, 5);
    HoodieRecord deleteRecordSameOrderingValue = generateDeletes(Collections.singletonList(newRecords.get(2)), config, payloadClass, partitionPath, 10L).get(0);
    HoodieRecord deleteRecordHigherOrderingValue = generateDeletes(Collections.singletonList(newRecords.get(3)), config, payloadClass, partitionPath, 20L).get(0);
    HoodieRecord deleteRecordLowerOrderingValue = generateDeletes(Collections.singletonList(newRecords.get(4)), config, payloadClass, partitionPath, 2L).get(0);
    recordsToDelete.add(deleteRecordSameOrderingValue);
    recordsToDelete.add(deleteRecordLowerOrderingValue);
    recordsToDelete.add(deleteRecordHigherOrderingValue);

    // Known Gap HUDI-9715: Currently the ordering provided by the custom mergers does not apply deletes.
    validDeletes.put(deleteRecordSameOrderingValue.getRecordKey(), deleteRecordSameOrderingValue);
    validDeletes.put(deleteRecordHigherOrderingValue.getRecordKey(), deleteRecordHigherOrderingValue);
    expectedDeletes += 2;

    if (mergeMode.equals(RecordMergeMode.COMMIT_TIME_ORDERING.name())) {
      validDeletes.put(deleteRecordLowerOrderingValue.getRecordKey(), deleteRecordLowerOrderingValue);
      expectedDeletes += 1;
    }

    // Generate records to update
    GenericRecord genericRecord1 = getGenRecord(newRecords.get(0), config);
    GenericRecord genericRecord2 = getGenRecord(newRecords.get(1), config);
    genericRecord1.put(ORDERING_FIELD, 20L);
    genericRecord2.put(ORDERING_FIELD, 2L);
    recordsToUpdate.add(genericRecord1);
    recordsToUpdate.add(genericRecord2);
    List<HoodieRecord> hoodieRecordsToUpdate = getHoodieRecords(payloadClass, recordsToUpdate, partitionPath);
    if (!mergeMode.equals("CUSTOM_MERGER") && !mergeMode.equals("CUSTOM")) {
      // Custom merger chooses record with lower ordering value
      validUpdates.add(hoodieRecordsToUpdate.get(0));
      expectedUpdates += 1;
    }
    if (!mergeMode.equals(RecordMergeMode.EVENT_TIME_ORDERING.name())) {
      validUpdates.add(hoodieRecordsToUpdate.get(1));
      expectedUpdates += 1;
    }

    List<HoodieRecord> recordsToMerge = hoodieRecordsToUpdate;
    recordsToMerge.addAll(recordsToDelete);
    // Generate records to insert
    List<HoodieRecord> recordsToInsert2 = dataGenerator.generateInserts(instantTime, 2);
    recordsToInsert2 = overrideOrderingValue(recordsToInsert2, config, payloadClass, partitionPath, 15L);
    recordsToMerge.addAll(recordsToInsert2);
    newInserts.addAll(recordsToInsert2);

    // let's compute the expected record list
    Map<String, HoodieRecord> expectedRecordsMap = new HashMap<>();
    validUpdates.forEach(rec -> {
      expectedRecordsMap.put(rec.getRecordKey(), rec);
    });
    recordsBatch1.forEach(record -> {
      // if not part of new update, if not valid delete, add records from 1st batch.
      String recKey = record.getRecordKey();
      if (!expectedRecordsMap.containsKey(recKey) && !validDeletes.containsKey(recKey)) {
        expectedRecordsMap.put(recKey, record);
      }
    });
    // add new inserts.
    newInserts.forEach(record -> {
      expectedRecordsMap.put(record.getRecordKey(), record);
    });

    return new InputAndExpectedDataSet(expectedRecordsMap, expectedUpdates, expectedDeletes, recordsToMerge, newInserts, validUpdates, validDeletes);
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

  private List<HoodieRecord> overrideOrderingValue(List<HoodieRecord> hoodieRecords, HoodieWriteConfig config, String payloadClass, String partitionPath, long orderingValue) {

    List<GenericRecord> genericRecords = hoodieRecords.stream().map(insertRecord -> {
      try {
        GenericRecord genericRecord = (GenericRecord) ((HoodieRecordPayload) insertRecord.getData()).getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA, config.getProps()).get();
        genericRecord.put(ORDERING_FIELD, orderingValue);
        return genericRecord;
      } catch (IOException e) {
        throw new HoodieIOException("Failed to deser ", e);
      }
    }).collect(Collectors.toList());

    return getHoodieRecords(payloadClass, genericRecords, partitionPath);
  }

  private List<HoodieRecord> generateDeletes(List<HoodieRecord> hoodieRecords, HoodieWriteConfig config, String payloadClass, String partitionPath, long orderingValue) {
    List<GenericRecord> genericRecords = hoodieRecords.stream().map(deleteRecord -> {
      try {
        GenericRecord genericRecord = (GenericRecord) ((HoodieRecordPayload) deleteRecord.getData()).getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA, config.getProps()).get();
        genericRecord.put(ORDERING_FIELD, orderingValue);
        genericRecord.put(HoodieRecord.HOODIE_IS_DELETED_FIELD, true);
        return genericRecord;
      } catch (IOException e) {
        throw new HoodieIOException("Failed to deser ", e);
      }
    }).collect(Collectors.toList());
    return getHoodieRecords(payloadClass, genericRecords, partitionPath);
  }

  private GenericRecord getGenRecord(HoodieRecord hoodieRecord, HoodieWriteConfig config) {
    try {
      return (GenericRecord) ((HoodieRecordPayload) hoodieRecord.getData()).getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA, config.getProps()).get();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deser record ", e);
    }
  }

  private List<HoodieRecord> getHoodieRecords(String payloadClass, List<GenericRecord> genericRecords, String partitionPath) {
    return genericRecords.stream().map(genericRecord -> {
      return (HoodieRecord) new HoodieAvroRecord<>(new HoodieKey(genericRecord.get("_row_key").toString(), partitionPath),
          HoodieRecordUtils.loadPayload(payloadClass, genericRecord, (Comparable) genericRecord.get(ORDERING_FIELD)));
    }).collect(Collectors.toList());
  }

  private void setCurLocation(List<HoodieRecord> records, String fileId, String instantTime) {
    records.forEach(record -> record.setCurrentLocation(new HoodieRecordLocation(instantTime, fileId)));
  }

  private static void validateWriteStatus(WriteStatus writeStatus, String previousCommit, long expectedTotalRecordsWritten, long expectedTotalUpdatedRecords,
                                          long expectedTotalInsertedRecords, long expectedTotalDeletedRecords) {
    HoodieWriteStat writeStat = writeStatus.getStat();
    assertEquals(previousCommit, writeStat.getPrevCommit());
    assertNotNull(writeStat.getFileId());
    assertNotNull(writeStat.getPath());
    assertTrue(writeStat.getFileSizeInBytes() > 0);
    assertTrue(writeStat.getTotalWriteBytes() > 0);
    assertTrue(writeStat.getTotalLogBlocks() == 0);
    assertTrue(writeStat.getTotalLogSizeCompacted() == 0);
    assertTrue(writeStat.getTotalLogFilesCompacted() == 0);
    assertTrue(writeStat.getTotalLogRecords() == 0);
    assertEquals(expectedTotalRecordsWritten, writeStat.getNumWrites());
    assertEquals(expectedTotalUpdatedRecords, writeStat.getNumUpdateWrites());
    assertEquals(expectedTotalInsertedRecords, writeStat.getNumInserts());
    assertEquals(expectedTotalDeletedRecords, writeStat.getNumDeletes());
  }

  class InputAndExpectedDataSet {
    private final Map<String, HoodieRecord> expectedRecordsMap;
    private final int expectedUpdates;
    private final int expectedDeletes;
    private final List<HoodieRecord> recordsToMerge;
    private final List<HoodieRecord> newInserts;
    private final List<HoodieRecord> validUpdates;
    private final Map<String, HoodieRecord> validDeletes;

    public InputAndExpectedDataSet(Map<String, HoodieRecord> expectedRecordsMap, int expectedUpdates, int expectedDeletes,
                                   List<HoodieRecord> recordsToMerge, List<HoodieRecord> newInserts, List<HoodieRecord> validUpdates,
                                   Map<String, HoodieRecord> validDeletes) {
      this.expectedRecordsMap = expectedRecordsMap;
      this.expectedUpdates = expectedUpdates;
      this.expectedDeletes = expectedDeletes;
      this.recordsToMerge = recordsToMerge;
      this.validUpdates = validUpdates;
      this.newInserts = newInserts;
      this.validDeletes = validDeletes;
    }

    public Map<String, HoodieRecord> getExpectedRecordsMap() {
      return expectedRecordsMap;
    }

    public int getExpectedUpdates() {
      return expectedUpdates;
    }

    public int getExpectedDeletes() {
      return expectedDeletes;
    }

    public List<HoodieRecord> getRecordsToMerge() {
      return recordsToMerge;
    }

    public List<HoodieRecord> getNewInserts() {
      return newInserts;
    }

    public List<HoodieRecord> getValidUpdates() {
      return validUpdates;
    }

    public Map<String, HoodieRecord> getValidDeletes() {
      return validDeletes;
    }
  }

  public static class CustomMerger implements HoodieRecordMerger {
    private static final String STRATEGY_ID = UUID.randomUUID().toString();

    public static String getStrategyId() {
      return STRATEGY_ID;
    }

    @Override
    public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
      GenericRecord olderData = (GenericRecord) older.getData();
      GenericRecord newerData = (GenericRecord) newer.getData();
      Long olderTimestamp = (Long) olderData.get("timestamp");
      Long newerTimestamp = (Long) newerData.get("timestamp");
      if (olderTimestamp.equals(newerTimestamp)) {
        // If the timestamps are the same, we do not update
        return Option.of(Pair.of(older, oldSchema));
      } else if (olderTimestamp < newerTimestamp) {
        // Custom merger chooses record with lower ordering value
        return Option.of(Pair.of(older, oldSchema));
      } else {
        // Custom merger chooses record with lower ordering value
        return Option.of(Pair.of(newer, newSchema));
      }
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return HoodieRecord.HoodieRecordType.AVRO;
    }

    @Override
    public String getMergingStrategy() {
      return STRATEGY_ID;
    }
  }

  public static class CustomPayload implements HoodieRecordPayload<CustomPayload> {
    private final GenericRecord record;

    public CustomPayload(GenericRecord record, Comparable orderingValue) {
      this.record = record;
    }

    @Override
    public CustomPayload preCombine(CustomPayload other) {
      return this; // No-op for this test
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
      Long olderTimestamp = (Long) ((GenericRecord) currentValue).get("timestamp");
      Long newerTimestamp = (Long) record.get("timestamp");
      if (olderTimestamp.equals(newerTimestamp)) {
        // If the timestamps are the same, we do not update
        return Option.of(currentValue);
      } else if (olderTimestamp < newerTimestamp) {
        // Custom merger chooses record with lower ordering value
        return Option.of(currentValue);
      } else {
        // Custom merger chooses record with lower ordering value
        return Option.of(record);
      }
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
      return Option.of(record);
    }
  }
}
