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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieCommonTestHarness.getDataBlock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestHoodieMergedLogRecordScanner {

  @TempDir
  java.nio.file.Path tempDir;

  /**
   * A later delete wins over a stored log record whose ordering field is null, and a delete without an ordering value
   * wins by natural order.
   */
  @Test
  void testDeleteOverNullOrderingValue() throws IOException, InterruptedException {
    String basePath = tempDir.toString();
    Properties tableProps = new Properties();
    tableProps.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath, HoodieTableType.MERGE_ON_READ, tableProps);
    HoodieStorage storage = HoodieTestUtils.getStorage(basePath);
    StoragePath partitionPath = new StoragePath(basePath, "partition_path");
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(
        "{\"type\":\"record\",\"name\":\"rec\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"ts\",\"type\":[\"null\",\"long\"],\"default\":null}]}"));
    List<IndexedRecord> records = new ArrayList<>();
    for (String key : Arrays.asList("key0", "key1", "key2")) {
      GenericRecord record = new GenericData.Record(schema.toAvroSchema());
      record.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, key);
      record.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, "partition_path");
      record.put("id", key);
      record.put("ts", key.equals("key0") ? null : 9L);
      records.add(record);
    }
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder().withParentPath(partitionPath)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withLogFileId("fileid0").withInstantTime("100").withStorage(storage).build()) {
      writer.appendBlock(getDataBlock(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK, records, header));
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
      List<Pair<DeleteRecord, Long>> deletes = Arrays.asList(
          Pair.of(DeleteRecord.create("key0", "partition_path", 5L), -1L),
          Pair.of(DeleteRecord.create("key1", "partition_path", 5L), -1L),
          Pair.of(DeleteRecord.create("key2", "partition_path", null), -1L));
      writer.appendBlock(new HoodieDeleteBlock(deletes, header));
    }
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "101", storage);
    List<String> logFiles = FSUtils.getAllLogFiles(storage, partitionPath, "fileid0", HoodieLogFile.DELTA_EXTENSION, "100")
        .map(s -> s.getPath().toString()).collect(Collectors.toList());

    try (HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(logFiles)
        .withReaderSchema(schema)
        .withLatestInstantTime("101")
        .withMaxMemorySizeInBytes(1024000L)
        .withBufferSize(4096)
        .withSpillableMapBasePath(new StoragePath(basePath, ".spillable").toString())
        .build()) {
      Map<String, Option<IndexedRecord>> merged = new HashMap<>();
      for (HoodieRecord<?> record : scanner) {
        merged.put(record.getRecordKey(), ((HoodieRecordPayload) record.getData()).getInsertValue(schema.toAvroSchema()));
      }
      assertEquals(new HashSet<>(Arrays.asList("key0", "key1", "key2")), merged.keySet());
      assertFalse(merged.get("key0").isPresent(), "The delete wins over a stored null ordering value");
      assertFalse(merged.get("key2").isPresent(), "A delete without an ordering value wins by natural order");
      IndexedRecord kept = merged.get("key1").get();
      for (HoodieSchemaField field : schema.getFields()) {
        assertEquals(String.valueOf(records.get(1).get(field.pos())), String.valueOf(kept.get(field.pos())),
            "The stored record with the higher ordering value wins");
      }
    }
  }
}
