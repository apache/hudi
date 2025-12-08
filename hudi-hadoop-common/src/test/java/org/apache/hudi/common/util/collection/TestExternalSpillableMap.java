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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.SpillableMapTestUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.Alphanumeric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests external spillable map {@link ExternalSpillableMap}.
 */
@TestMethodOrder(Alphanumeric.class)
public class TestExternalSpillableMap extends HoodieCommonTestHarness {

  private static final String TEST_LOGGING_CONTEXT = "test_logging_context";
  private static String failureOutputPath;

  @BeforeEach
  public void setUp() {
    initPath();
    failureOutputPath = basePath + "/test_fail";
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void simpleInsertTest(ExternalSpillableMap.DiskMapType diskMapType, boolean isCompressionEnabled) throws IOException, URISyntaxException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) { // 16B

      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);
      List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
      assert (recordKeys.size() == 100);

      // Test iterator
      Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
      int cntSize = 0;
      while (itr.hasNext()) {
        HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
        cntSize++;
        assert recordKeys.contains(rec.getRecordKey());
      }
      assertEquals(recordKeys.size(), cntSize);

      // Test value stream
      List<HoodieRecord<? extends HoodieRecordPayload>> values = records.valueStream().collect(Collectors.toList());
      cntSize = 0;
      for (HoodieRecord value : values) {
        assert recordKeys.contains(value.getRecordKey());
        cntSize++;
      }
      assertEquals(recordKeys.size(), cntSize);
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testSimpleUpsert(ExternalSpillableMap.DiskMapType diskMapType, boolean isCompressionEnabled) throws IOException, URISyntaxException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) { // 16B

      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);
      List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
      assert (recordKeys.size() == 100);
      Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
      while (itr.hasNext()) {
        HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
        assert recordKeys.contains(rec.getRecordKey());
      }

      List<IndexedRecord> updatedRecords = SchemaTestUtil.updateHoodieTestRecords(recordKeys,
          testUtil.generateHoodieTestRecords(0, 100), InProcessTimeGenerator.createNewInstantTime());

      // update records already inserted
      SpillableMapTestUtils.upsertRecords(updatedRecords, records);

      // make sure we have records spilled to disk
      assertTrue(records.getDiskBasedMapNumEntries() > 0);

      // iterate over the updated records and compare the value from Map
      updatedRecords.forEach(record -> {
        HoodieRecord rec = records.get(((GenericRecord) record).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        try {
          assertEquals(((HoodieAvroRecord) rec).getData().getInsertValue(schema.toAvroSchema()).get(), record);
        } catch (IOException io) {
          throw new UncheckedIOException(io);
        }
      });
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testAllMapOperations(ExternalSpillableMap.DiskMapType diskMapType, boolean isCompressionEnabled) throws IOException, URISyntaxException {

    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    String payloadClazz = HoodieAvroPayload.class.getName();

    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) { // 16B

      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);
      // insert a bunch of records so that values spill to disk too
      List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
      IndexedRecord inMemoryRecord = iRecords.get(0);
      String ikey = ((GenericRecord) inMemoryRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String iPartitionPath = ((GenericRecord) inMemoryRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
      HoodieRecord inMemoryHoodieRecord = new HoodieAvroRecord<>(new HoodieKey(ikey, iPartitionPath),
          new HoodieAvroPayload(Option.of((GenericRecord) inMemoryRecord)));

      IndexedRecord onDiskRecord = iRecords.get(99);
      String dkey = ((GenericRecord) onDiskRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String dPartitionPath = ((GenericRecord) onDiskRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
      HoodieRecord onDiskHoodieRecord = new HoodieAvroRecord<>(new HoodieKey(dkey, dPartitionPath),
          new HoodieAvroPayload(Option.of((GenericRecord) onDiskRecord)));
      // assert size
      assert records.size() == 100;
      // get should return the same HoodieKey, same location and same value
      assert inMemoryHoodieRecord.getKey().equals(records.get(ikey).getKey());
      assert onDiskHoodieRecord.getKey().equals(records.get(dkey).getKey());
      // compare the member variables of HoodieRecord not set by the constructor
      assert records.get(ikey).getCurrentLocation().getFileId().equals(SpillableMapTestUtils.DUMMY_FILE_ID);
      assert records.get(ikey).getCurrentLocation().getInstantTime().equals(SpillableMapTestUtils.DUMMY_COMMIT_TIME);

      // test contains
      assertTrue(records.containsKey(ikey));
      assertTrue(records.containsKey(dkey));

      // test isEmpty
      assertFalse(records.isEmpty());

      // test containsAll
      assertTrue(records.keySet().containsAll(recordKeys));

      // remove (from inMemory and onDisk)
      HoodieRecord removedRecord = records.remove(ikey);
      assertTrue(removedRecord != null);
      assertFalse(records.containsKey(ikey));

      removedRecord = records.remove(dkey);
      assertTrue(removedRecord != null);
      assertFalse(records.containsKey(dkey));

      // test clear
      records.clear();
      assertTrue(records.size() == 0);
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void simpleTestWithException(ExternalSpillableMap.DiskMapType diskMapType, boolean isCompressionEnabled) throws IOException, URISyntaxException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records = new ExternalSpillableMap<>(16L,
        failureOutputPath, new DefaultSizeEstimator(),
        new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) { // 16B

      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);
      List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
      assert (recordKeys.size() == 100);
      Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
      assertThrows(IOException.class, () -> {
        while (itr.hasNext()) {
          throw new IOException("Testing failures...");
        }
      });
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testDataCorrectnessWithUpsertsToDataInMapAndOnDisk(ExternalSpillableMap.DiskMapType diskMapType,
                                                                 boolean isCompressionEnabled) throws IOException,
      URISyntaxException {

    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) { // 16B

      List<String> recordKeys = new ArrayList<>();
      // Ensure we spill to disk
      while (records.getDiskBasedMapNumEntries() < 1) {
        SchemaTestUtil testUtil = new SchemaTestUtil();
        List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);
        recordKeys.addAll(SpillableMapTestUtils.upsertRecords(iRecords, records));
      }

      // Get a record from the in-Memory map
      String key = recordKeys.get(0);
      HoodieAvroRecord record = (HoodieAvroRecord) records.get(key);
      List<IndexedRecord> recordsToUpdate = new ArrayList<>();
      recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema.toAvroSchema()).get());

      String newCommitTime = InProcessTimeGenerator.createNewInstantTime();
      List<String> keysToBeUpdated = new ArrayList<>();
      keysToBeUpdated.add(key);
      // Update the instantTime for this record
      List<IndexedRecord> updatedRecords =
          SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
      // Upsert this updated record
      SpillableMapTestUtils.upsertRecords(updatedRecords, records);
      GenericRecord gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema.toAvroSchema()).get();
      // The record returned for this key should have the updated commitTime
      assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());

      // Get a record from the disk based map
      key = recordKeys.get(recordKeys.size() - 1);
      record = (HoodieAvroRecord) records.get(key);
      recordsToUpdate = new ArrayList<>();
      recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema.toAvroSchema()).get());

      newCommitTime = InProcessTimeGenerator.createNewInstantTime();
      keysToBeUpdated = new ArrayList<>();
      keysToBeUpdated.add(key);
      // Update the commitTime for this record
      updatedRecords = SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
      // Upsert this updated record
      SpillableMapTestUtils.upsertRecords(updatedRecords, records);
      gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema.toAvroSchema()).get();
      // The record returned for this key should have the updated instantTime
      assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testDataCorrectnessWithoutHoodieMetadata(ExternalSpillableMap.DiskMapType diskMapType,
                                                       boolean isCompressionEnabled) throws IOException,
      URISyntaxException {

    HoodieSchema schema = SchemaTestUtil.getSimpleSchema();

    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) { // 16B

      SchemaTestUtil testUtil = new SchemaTestUtil();
      List<String> recordKeys = new ArrayList<>();

      // Ensure we spill to disk
      while (records.getDiskBasedMapNumEntries() < 1) {
        List<HoodieRecord> hoodieRecords = testUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 100);
        hoodieRecords.stream().forEach(r -> {
          records.put(r.getRecordKey(), r);
          recordKeys.add(r.getRecordKey());
        });
      }

      // Get a record from the in-Memory map
      String key = recordKeys.get(0);
      HoodieRecord record = records.get(key);
      // Get the field we want to update
      String fieldName = schema.getFields().stream().filter(field -> field.schema().getType() == HoodieSchemaType.STRING)
          .findAny().get().name();
      // Use a new value to update this field
      String newValue = "update1";
      List<HoodieRecord> recordsToUpdate = new ArrayList<>();
      recordsToUpdate.add(record);

      List<HoodieRecord> updatedRecords =
          SchemaTestUtil.updateHoodieTestRecordsWithoutHoodieMetadata(recordsToUpdate, schema, fieldName, newValue);

      // Upsert this updated record
      updatedRecords.forEach(r -> {
        records.put(r.getRecordKey(), r);
      });
      GenericRecord gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema.toAvroSchema()).get();
      // The record returned for this key should have the updated value for the field name
      assertEquals(gRecord.get(fieldName).toString(), newValue);

      // Get a record from the disk based map
      key = recordKeys.get(recordKeys.size() - 1);
      record = records.get(key);
      // Get the field we want to update
      fieldName = schema.getFields().stream().filter(field -> field.schema().getType() == HoodieSchemaType.STRING).findAny()
          .get().name();
      // Use a new value to update this field
      newValue = "update2";
      recordsToUpdate = new ArrayList<>();
      recordsToUpdate.add(record);

      updatedRecords =
          SchemaTestUtil.updateHoodieTestRecordsWithoutHoodieMetadata(recordsToUpdate, schema, fieldName, newValue);

      // Upsert this updated record
      updatedRecords.forEach(r -> {
        records.put(r.getRecordKey(), r);
      });
      gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema.toAvroSchema()).get();
      // The record returned for this key should have the updated value for the field name
      assertEquals(gRecord.get(fieldName).toString(), newValue);
    }
  }

  @Test
  public void testEstimationWithEmptyMap() throws IOException, URISyntaxException {
    final ExternalSpillableMap.DiskMapType diskMapType = ExternalSpillableMap.DiskMapType.BITCASK;
    final boolean isCompressionEnabled = false;
    final HoodieSchema schema = SchemaTestUtil.getSimpleSchema();

    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) {

      List<String> recordKeys = new ArrayList<>();
      SchemaTestUtil testUtil = new SchemaTestUtil();

      // Put a single record. Payload size estimation happens as part of this initial put.
      HoodieRecord seedRecord = testUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 1).get(0);
      records.put(seedRecord.getRecordKey(), seedRecord);

      // Remove the key immediately to make the map empty again.
      records.remove(seedRecord.getRecordKey());

      // Verify payload size re-estimation does not throw exception
      SchemaTestUtil testUtilx = new SchemaTestUtil();
      List<HoodieRecord> hoodieRecords = testUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 250);
      hoodieRecords.stream().forEach(hoodieRecord -> {
        assertDoesNotThrow(() -> {
          records.put(hoodieRecord.getRecordKey(), hoodieRecord);
        }, "ExternalSpillableMap put() should not throw exception!");
        recordKeys.add(hoodieRecord.getRecordKey());
      });
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testDataCorrectnessWithRecordExistsInDiskMapAndThenUpsertToMem(ExternalSpillableMap.DiskMapType diskMapType,
                                                  boolean isCompressionEnabled) throws IOException, URISyntaxException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    SizeEstimator keyEstimator = new DefaultSizeEstimator();
    SizeEstimator valEstimator = new HoodieRecordSizeEstimator(schema.toAvroSchema());
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> iRecords = testUtil.generateHoodieTestRecords(0, 100);

    // Get the first record
    IndexedRecord firstRecord = iRecords.get(0);
    String key = ((GenericRecord) firstRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String partitionPath = ((GenericRecord) firstRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieRecord record =
        new HoodieAvroRecord<>(new HoodieKey(key, partitionPath), new HoodieAvroPayload(Option.of((GenericRecord) firstRecord)));
    record.setCurrentLocation(new HoodieRecordLocation(SpillableMapTestUtils.DUMMY_COMMIT_TIME, SpillableMapTestUtils.DUMMY_FILE_ID));
    record.seal();

    // Estimate the first record size and calculate the total memory size that the in-memory map can only contain 100 records.
    long estimatedPayloadSize = keyEstimator.sizeEstimate(key) + valEstimator.sizeEstimate(record);
    long totalEstimatedSizeWith100Records = (long) ((estimatedPayloadSize * 100) / 0.8);
    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(totalEstimatedSizeWith100Records, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema.toAvroSchema()), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, TEST_LOGGING_CONTEXT)) {

      // Insert 100 records and then in-memory map will contain 100 records.
      SpillableMapTestUtils.upsertRecords(iRecords, records);

      // Generate one record and it will be spilled to disk
      List<IndexedRecord> singleRecord = testUtil.generateHoodieTestRecords(0, 1);
      List<String> singleRecordKey = SpillableMapTestUtils.upsertRecords(singleRecord, records);

      // Get the field we want to update
      String fieldName = schema.getFields().stream().filter(field -> field.schema().getType() == HoodieSchemaType.STRING).findAny()
          .get().name();
      HoodieRecord hoodieRecord = records.get(singleRecordKey.get(0));
      // Use a new value to update this field, the estimate size of this record will be less than the first record.
      String newValue = "";
      HoodieRecord updatedRecord =
          SchemaTestUtil.updateHoodieTestRecordsWithoutHoodieMetadata(Arrays.asList(hoodieRecord), schema, fieldName, newValue).get(0);
      records.put(updatedRecord.getRecordKey(), updatedRecord);

      assertEquals(records.size(), 101);
    }
  }

  @ParameterizedTest
  @EnumSource(value = ExternalSpillableMap.DiskMapType.class)
  void assertEmptyMapOperations(ExternalSpillableMap.DiskMapType diskMapType) throws IOException {
    // validate that operations on an empty map work as expected
    try (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
             new ExternalSpillableMap<>(10, basePath, new DefaultSizeEstimator(),
                 new DefaultSizeEstimator<>(), diskMapType, new DefaultSerializer<>(),false, TEST_LOGGING_CONTEXT)) {
      assertTrue(records.isEmpty());
      assertFalse(records.containsKey("key"));
      assertFalse(records.containsValue("value"));
      assertTrue(records.keySet().isEmpty());
      assertTrue(records.values().isEmpty());
      assertTrue(records.entrySet().isEmpty());
      assertEquals(0, records.valueStream().count());
      assertEquals(0, records.size());
      assertFalse(records.iterator().hasNext());
    }
  }

  private static Stream<Arguments> testArguments() {
    // Arguments : 1. Disk Map Type 2. isCompressionEnabled for BitCaskMap
    return Stream.of(
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, false),
        arguments(ExternalSpillableMap.DiskMapType.ROCKS_DB, false),
        arguments(ExternalSpillableMap.DiskMapType.UNKNOWN, false),
        arguments(ExternalSpillableMap.DiskMapType.BITCASK, true)
    );
  }
}
