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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
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

  private static File dfsBaseDir;
  private static MiniDFSCluster cluster;

  private FileSystem fs;
  private String basePath;
  private Path partitionPath;
  private String partitionValue = "my_partition";
  private String partitionName = "partition";

  @BeforeAll
  public static void setUpClass() throws IOException, InterruptedException {
    dfsBaseDir = new File("/tmp/" + UUID.randomUUID().toString());
    FileUtil.fullyDelete(dfsBaseDir);
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    Configuration conf = new Configuration();
    // lower heartbeat interval for fast recognition of DN
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsBaseDir.getAbsolutePath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 3000);
    cluster = new MiniDFSCluster.Builder(conf).checkExitOnShutdown(true).numDataNodes(4).build();
  }

  @AfterAll
  public static void tearDownClass() {
    cluster.shutdown(true);
    FileUtil.fullyDelete(dfsBaseDir);
  }

  @BeforeEach
  public void setUp() throws IOException, InterruptedException {
    this.fs = cluster.getFileSystem();

    this.partitionPath = new Path(dfsBaseDir.toString(), partitionValue);
    this.basePath = dfsBaseDir.toString();
  }

  @AfterEach
  public void tearDown() throws IOException {
    fs.delete(partitionPath, true);
  }

  @Test
  public void testLogRecordReaderExtractPartitionFromPath() throws IOException, URISyntaxException, InterruptedException {

    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), "true");
    properties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), partitionName);
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
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field(partitionName, Schema.create(Schema.Type.STRING), null, ""));
    for (Schema.Field field : schema.getFields()) {
      Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
      for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
        newField.addProp(prop.getKey(), prop.getValue());
      }
      fields.add(newField);
    }
    Schema readerSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    readerSchema.setFields(fields);

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
        .withPartition(partitionValue)
        .build();
    assertEquals(200, scanner.getTotalLogRecords());
    Set<String> readKeys = new HashSet<>(200);
    scanner.forEach(s -> {
      readKeys.add(s.getKey().getRecordKey());
      try {
        IndexedRecord record = ((HoodieAvroPayload)s.getData()).getInsertValue(readerSchema, new Properties()).get();
        String partition = record.get(record.getSchema().getField(partitionName).pos()).toString();
        // The value of field "partition" should be "my_partition" passed to scanner
        assertEquals(partitionValue, partition);
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
