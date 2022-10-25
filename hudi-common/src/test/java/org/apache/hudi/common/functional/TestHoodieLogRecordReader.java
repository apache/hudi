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

package org.apache.hudi.common.functional;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.apache.hudi.common.util.collection.ExternalSpillableMap.DiskMapType.BITCASK;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieLogRecordReader {

  private FileSystem fs;
  private Path tmpDir;
  private String basePath;

  @BeforeEach
  public void setUp() throws Exception {
    this.tmpDir = new Path("/tmp/" + UUID.randomUUID());
    this.basePath = tmpDir.toString();

    fs = new Path(basePath).getFileSystem(new Configuration());
  }

  @AfterEach
  public void tearDown() throws IOException {
    fs.delete(this.tmpDir, true);
  }

  @Test
  public void testLogRecordReaderExtractPartitionFromPath() throws IOException, URISyntaxException, InterruptedException {
    String partitionValue1 = "str_partition";
    int partitionValue2 = 100;
    String partitionField1 = "partition1";
    String partitionField2 = "partition2";
    Path partitionPath = new Path(tmpDir.toString(), partitionValue1 + "/" + partitionValue2);

    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), "true");
    properties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), partitionField1 + "," + partitionField2);
    HoodieTestUtils.init(fs.getConf(), basePath, HoodieTableType.MERGE_ON_READ, properties);

    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    HoodieLogFormat.Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).withSizeThreshold(500).build();
    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());

    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = new HoodieAvroDataBlock(records1, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = new HoodieAvroDataBlock(records2, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    FileCreateUtils.createDeltaCommit(basePath, "100", fs);

    // generate reader schema

    List<Schema.Field> newFields = new ArrayList<>();
    newFields.add(new Schema.Field(partitionField1, Schema.create(Schema.Type.STRING), null, ""));
    newFields.add(new Schema.Field(partitionField2, Schema.create(Schema.Type.INT), null, 0));
    Schema readerSchema = AvroSchemaUtils.appendFieldsToSchema(schema, newFields);

    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime("102")
        .withMaxMemorySizeInBytes(10240L)
        .withReverseReader(false)
        .withBufferSize(4096)
        .withDiskMapType(BITCASK)
        .withPartition(partitionValue1 + "/" + partitionValue2)
        .build();
    assertEquals(200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> {
      readKeys.add(s.getKey().getRecordKey());
      try {
        IndexedRecord record = ((HoodieAvroPayload)s.getData()).getInsertValue(readerSchema, new Properties()).get();
        String partition1 = record.get(record.getSchema().getField(partitionField1).pos()).toString();
        assertEquals(partitionValue1, partition1);

        int partition2 = Integer.parseInt(record.get(record.getSchema().getField(partitionField2).pos()).toString());
        assertEquals(partitionValue2, partition2);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    copyOfRecords1.addAll(copyOfRecords2);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals(originalKeys, readKeys, "CompositeAvroLogReader should return 200 records from 2 versions");
  }

  @Test
  public void testLogRecordReaderExtractHiveStylePartitionFromPath() throws IOException, URISyntaxException, InterruptedException {
    String partitionValue1 = "str_partition";
    int partitionValue2 = 100;
    String partitionField1 = "partition1";
    String partitionField2 = "partition2";

    String hiveStylePartitionValue = partitionField1 + "=" + partitionValue1 + "/" + partitionField2 + "=" + partitionValue2;
    Path partitionPath = new Path(tmpDir.toString(), hiveStylePartitionValue);

    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), "true");
    properties.setProperty(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    properties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), partitionField1 + "," + partitionField2);
    HoodieTestUtils.init(fs.getConf(), basePath, HoodieTableType.MERGE_ON_READ, properties);

    Schema schema = HoodieAvroUtils.addMetadataFields(getSimpleSchema());
    HoodieLogFormat.Writer writer =
        HoodieLogFormat.newWriterBuilder().onParentPath(partitionPath).withFileExtension(HoodieLogFile.DELTA_EXTENSION)
            .withFileId("test-fileid1").overBaseCommit("100").withFs(fs).withSizeThreshold(500).build();
    // Write 1
    List<IndexedRecord> records1 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords1 = records1.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());

    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieDataBlock dataBlock = new HoodieAvroDataBlock(records1, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    writer.appendBlock(dataBlock);

    // Write 2
    List<IndexedRecord> records2 = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<IndexedRecord> copyOfRecords2 = records2.stream()
        .map(record -> HoodieAvroUtils.rewriteRecord((GenericRecord) record, schema)).collect(Collectors.toList());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    dataBlock = new HoodieAvroDataBlock(records2, header, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    writer.appendBlock(dataBlock);
    writer.close();

    List<String> allLogFiles =
        FSUtils.getAllLogFiles(fs, partitionPath, "test-fileid1", HoodieLogFile.DELTA_EXTENSION, "100")
            .map(s -> s.getPath().toString()).collect(Collectors.toList());

    FileCreateUtils.createDeltaCommit(basePath, "100", fs);

    // generate reader schema
    List<Schema.Field> newFields = new ArrayList<>();
    newFields.add(new Schema.Field(partitionField1, Schema.create(Schema.Type.STRING), null, ""));
    newFields.add(new Schema.Field(partitionField2, Schema.create(Schema.Type.INT), null, 0));
    Schema readerSchema = AvroSchemaUtils.appendFieldsToSchema(schema, newFields);

    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(basePath)
        .withLogFilePaths(allLogFiles)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime("102")
        .withMaxMemorySizeInBytes(10240L)
        .withReverseReader(false)
        .withBufferSize(4096)
        .withDiskMapType(BITCASK)
        .withPartition(hiveStylePartitionValue)
        .build();
    assertEquals(200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> {
      readKeys.add(s.getKey().getRecordKey());
      try {
        IndexedRecord record = ((HoodieAvroPayload)s.getData()).getInsertValue(readerSchema, new Properties()).get();
        String partition1 = record.get(record.getSchema().getField(partitionField1).pos()).toString();
        assertEquals(partitionValue1, partition1);

        int partition2 = Integer.parseInt(record.get(record.getSchema().getField(partitionField2).pos()).toString());
        assertEquals(partitionValue2, partition2);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    assertEquals(200, readKeys.size(), "Stream collect should return all 200 records");
    copyOfRecords1.addAll(copyOfRecords2);
    Set<String> originalKeys =
        copyOfRecords1.stream().map(s -> ((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString())
            .collect(Collectors.toSet());
    assertEquals(originalKeys, readKeys, "CompositeAvroLogReader should return 200 records from 2 versions");
  }
}
