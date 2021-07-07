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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.inline.InLineFSUtils;
import org.apache.hudi.common.fs.inline.InLineFileSystem;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieLogFileReader;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.SpillableMapTestUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.Alphanumeric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.getPhantomFile;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests external spillable map {@link ExternalSpillableMap}.
 */
@TestMethodOrder(Alphanumeric.class)
public class TestExternalSpillableMap extends HoodieCommonTestHarness {

  private static String failureOutputPath;

  @BeforeEach
  public void setUp() {
    initPath();
    failureOutputPath = basePath + "/test_fail";
  }

  @ParameterizedTest
  @EnumSource(ExternalSpillableMap.DiskMapType.class)
  public void simpleInsertTest(ExternalSpillableMap.DiskMapType diskMapType) throws IOException, URISyntaxException {
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), diskMapType); // 16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
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

  @ParameterizedTest
  @EnumSource(ExternalSpillableMap.DiskMapType.class)
  public void testSimpleUpsert(ExternalSpillableMap.DiskMapType diskMapType) throws IOException, URISyntaxException {

    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), diskMapType); // 16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    while (itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      assert recordKeys.contains(rec.getRecordKey());
    }
    List<IndexedRecord> updatedRecords = SchemaTestUtil.updateHoodieTestRecords(recordKeys,
        SchemaTestUtil.generateHoodieTestRecords(0, 100), HoodieActiveTimeline.createNewInstantTime());

    // update records already inserted
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);

    // make sure we have records spilled to disk
    assertTrue(records.getDiskBasedMapNumEntries() > 0);

    // iterate over the updated records and compare the value from Map
    updatedRecords.forEach(record -> {
      HoodieRecord rec = records.get(((GenericRecord) record).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      try {
        assertEquals(rec.getData().getInsertValue(schema).get(), record);
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
  }

  @ParameterizedTest
  @EnumSource(ExternalSpillableMap.DiskMapType.class)
  public void testAllMapOperations(ExternalSpillableMap.DiskMapType diskMapType) throws IOException, URISyntaxException {

    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    String payloadClazz = HoodieAvroPayload.class.getName();

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), diskMapType); // 16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    // insert a bunch of records so that values spill to disk too
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    IndexedRecord inMemoryRecord = iRecords.get(0);
    String ikey = ((GenericRecord) inMemoryRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String iPartitionPath = ((GenericRecord) inMemoryRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieRecord inMemoryHoodieRecord = new HoodieRecord<>(new HoodieKey(ikey, iPartitionPath),
        new HoodieAvroPayload(Option.of((GenericRecord) inMemoryRecord)));

    IndexedRecord onDiskRecord = iRecords.get(99);
    String dkey = ((GenericRecord) onDiskRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String dPartitionPath = ((GenericRecord) onDiskRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieRecord onDiskHoodieRecord = new HoodieRecord<>(new HoodieKey(dkey, dPartitionPath),
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

  @ParameterizedTest
  @EnumSource(ExternalSpillableMap.DiskMapType.class)
  public void simpleTestWithException(ExternalSpillableMap.DiskMapType diskMapType) throws IOException, URISyntaxException {
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records = new ExternalSpillableMap<>(16L,
        failureOutputPath, new DefaultSizeEstimator(),
        new HoodieRecordSizeEstimator(schema), diskMapType); // 16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    assertThrows(IOException.class, () -> {
      while (itr.hasNext()) {
        throw new IOException("Testing failures...");
      }
    });
  }

  @ParameterizedTest
  @EnumSource(ExternalSpillableMap.DiskMapType.class)
  public void testDataCorrectnessWithUpsertsToDataInMapAndOnDisk(ExternalSpillableMap.DiskMapType diskMapType) throws IOException, URISyntaxException {

    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), diskMapType); // 16B

    List<String> recordKeys = new ArrayList<>();
    // Ensure we spill to disk
    while (records.getDiskBasedMapNumEntries() < 1) {
      List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
      recordKeys.addAll(SpillableMapTestUtils.upsertRecords(iRecords, records));
    }

    // Get a record from the in-Memory map
    String key = recordKeys.get(0);
    HoodieRecord record = records.get(key);
    List<IndexedRecord> recordsToUpdate = new ArrayList<>();
    recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema).get());

    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    List<String> keysToBeUpdated = new ArrayList<>();
    keysToBeUpdated.add(key);
    // Update the instantTime for this record
    List<IndexedRecord> updatedRecords =
        SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
    // Upsert this updated record
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);
    GenericRecord gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated commitTime
    assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());

    // Get a record from the disk based map
    key = recordKeys.get(recordKeys.size() - 1);
    record = records.get(key);
    recordsToUpdate = new ArrayList<>();
    recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema).get());

    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    keysToBeUpdated = new ArrayList<>();
    keysToBeUpdated.add(key);
    // Update the commitTime for this record
    updatedRecords = SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
    // Upsert this updated record
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);
    gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated instantTime
    assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
  }

  @ParameterizedTest
  @EnumSource(ExternalSpillableMap.DiskMapType.class)
  public void testDataCorrectnessWithoutHoodieMetadata(ExternalSpillableMap.DiskMapType diskMapType) throws IOException, URISyntaxException {

    Schema schema = SchemaTestUtil.getSimpleSchema();

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), diskMapType); // 16B

    List<String> recordKeys = new ArrayList<>();
    // Ensure we spill to disk
    while (records.getDiskBasedMapNumEntries() < 1) {
      List<HoodieRecord> hoodieRecords = SchemaTestUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 100);
      hoodieRecords.stream().forEach(r -> {
        records.put(r.getRecordKey(), r);
        recordKeys.add(r.getRecordKey());
      });
    }

    // Get a record from the in-Memory map
    String key = recordKeys.get(0);
    HoodieRecord record = records.get(key);
    // Get the field we want to update
    String fieldName = schema.getFields().stream().filter(field -> field.schema().getType() == Schema.Type.STRING)
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
    GenericRecord gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated value for the field name
    assertEquals(gRecord.get(fieldName).toString(), newValue);

    // Get a record from the disk based map
    key = recordKeys.get(recordKeys.size() - 1);
    record = records.get(key);
    // Get the field we want to update
    fieldName = schema.getFields().stream().filter(field -> field.schema().getType() == Schema.Type.STRING).findAny()
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
    gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated value for the field name
    assertEquals(gRecord.get(fieldName).toString(), newValue);
  }

  @Test
  public void tempTestParquetLog() throws IOException, URISyntaxException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    String benchmarkRoot = "file:///tmp";
    FileSystem localFileSystem = new Path(benchmarkRoot).getFileSystem(conf);

    String runBasePath = new Path("/tmp", "rm_base_path").toString();
    java.nio.file.Files.createDirectories(java.nio.file.Paths.get(runBasePath));
    org.apache.hadoop.fs.Path runPartitionPath = FSUtils.getPartitionPath(runBasePath, "05-20-2021");

    System.out.println("WNI WNI IMP " + runPartitionPath.toString());

    Schema schema = getSimpleSchema();
    List<IndexedRecord> rawRecords = SchemaTestUtil.generateTestRecords(0, 10);
    Schema writerSchema = HoodieAvroUtils.addMetadataFields(schema);
    List<IndexedRecord> records = rawRecords.stream().map(s -> HoodieAvroUtils.rewriteRecord((GenericRecord) s,
        writerSchema)).peek(p -> {
      p.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, UUID.randomUUID().toString());
      p.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, runPartitionPath.toString());
      p.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "001");
    }).collect(Collectors.toList());

    HoodieLogFormat.Writer logWriter =
        HoodieLogFormat.newWriterBuilder().onParentPath(runPartitionPath)
            .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("rm_test_file")
            .overBaseCommit("001")
            .withFs(localFileSystem).build();
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "002");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA,
        writerSchema.toString());

    HoodieDataBlock dataBlock = new HoodieParquetDataBlock(records, header);
    //HoodieDataBlock dataBlock = new HoodieAvroDataBlock(records, header);

    logWriter.appendBlock(dataBlock);
    //FileCreateUtils.createDeltaCommit(basePath.toString(), "002", localFileSystem);
    logWriter.close();

    List<HoodieLogFile> allLogFiles =
        FSUtils.getAllLogFiles(localFileSystem, runPartitionPath, "rm_test_file",
            HoodieLogFile.DELTA_EXTENSION, "001").collect(Collectors.toList());

    // read
    HoodieLogFileReader reader = new HoodieLogFileReader(localFileSystem,
        allLogFiles.get(0), writerSchema, (100*1024*1024), true, false);

    System.out.println("WNI HasNext " + reader.hasNext());
    HoodieParquetDataBlock block = (HoodieParquetDataBlock) reader.next();

    System.out.println("WNI VIMP "
        + " " + block.getBlockContentLocation().get().getContentPositionInLogFile()
    + " " + block.getBlockContentLocation().get().getBlockSize()
    + " " + block.getBlockContentLocation().get().getLogFile().getPath()
        + " " + block.getBlockContentLocation().get().getLogFile().getPath().getFileSystem(conf).getScheme()
    + " " + block.getBlockContentLocation().get().getLogFile().toString());

    Path inlinePath = InLineFSUtils.getInlineFilePath(
        block.getBlockContentLocation().get().getLogFile().getPath(),
        block.getBlockContentLocation().get().getLogFile().getPath().getFileSystem(conf).getScheme(),
        block.getBlockContentLocation().get().getContentPositionInLogFile(),
        block.getBlockContentLocation().get().getBlockSize());

    Configuration inlineConf = new Configuration();
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());

    ParquetReader inLineReader = AvroParquetReader.builder(inlinePath).withConf(inlineConf).build();
    List<GenericRecord> readRecords = readParquetGenericRecords(inLineReader);

    for (IndexedRecord readRecord : readRecords) {
      System.out.println("READ RECORD " + readRecord.toString());
    }

    inLineReader.close();
    reader.close();

  }

  static List<GenericRecord> readParquetGenericRecords(ParquetReader reader) throws IOException {
    List<GenericRecord> toReturn = new ArrayList<>();
    Object obj = reader.read();
    while (obj instanceof GenericRecord) {
      toReturn.add((GenericRecord) obj);
      obj = reader.read();
    }
    return toReturn;
  }

  // TODO : come up with a performance eval test for spillableMap
  @Test
  public void testLargeInsertUpsert() {}
}