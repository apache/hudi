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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that a files-partition entry carrying a source partition path resolves outside the table.
 */
public class TestHoodieMetadataPayloadSourcePartitionPath {

  private static final StoragePath PARTITION_PATH = new StoragePath("/tmp/hudi_table/datestr=2022-06-15");
  private static final String SOURCE_PARTITION_PATH = "/data/hive_table/datestr=2022-06-15";

  /**
   * The files-partition schema as it was before {@code sourcePartitionPath} was added, so a record
   * written by an older writer can be read back here.
   */
  private static final String SCHEMA_WITHOUT_SOURCE_PARTITION_PATH =
      "{\"type\":\"record\",\"name\":\"HoodieMetadataRecord\",\"fields\":["
          + "{\"name\":\"key\",\"type\":\"string\"},"
          + "{\"name\":\"type\",\"type\":\"int\"},"
          + "{\"name\":\"filesystemMetadata\",\"type\":[\"null\",{\"type\":\"map\",\"values\":"
          + "{\"type\":\"record\",\"name\":\"HoodieMetadataFileInfo\",\"fields\":["
          + "{\"name\":\"size\",\"type\":\"long\"},"
          + "{\"name\":\"isDeleted\",\"type\":\"boolean\"}]}}],\"default\":null}]}";

  /** The same, for the file-info record on its own. */
  private static final String FILE_INFO_SCHEMA_WITHOUT_SOURCE_PARTITION_PATH =
      "{\"type\":\"record\",\"name\":\"HoodieMetadataFileInfo\",\"namespace\":\"org.apache.hudi.avro.model\","
          + "\"fields\":[{\"name\":\"size\",\"type\":\"long\"},"
          + "{\"name\":\"isDeleted\",\"type\":\"boolean\"}]}";

  private HoodieStorage storage() {
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getDefaultBlockSize(any(StoragePath.class))).thenReturn(1024);
    return storage;
  }

  private HoodieMetadataPayload payloadFor(Map<String, HoodieMetadataFileInfo> fileInfo) {
    return new HoodieMetadataPayload("datestr=2022-06-15", MetadataPartitionType.FILES.getRecordType(), fileInfo);
  }

  @Test
  public void testFileWithoutSourcePartitionPathResolvesUnderThePartition() {
    HoodieMetadataPayload payload = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(128L, false, null)));

    List<StoragePathInfo> files = payload.getFileList(storage(), PARTITION_PATH);

    assertEquals(1, files.size());
    assertEquals(new StoragePath(PARTITION_PATH, "f1.parquet"), files.get(0).getPath());
    assertEquals(128L, files.get(0).getLength());
  }

  @Test
  public void testFileWithSourcePartitionPathResolvesOutsideTheTable() {
    HoodieMetadataPayload payload = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(128L, false, SOURCE_PARTITION_PATH)));

    List<StoragePathInfo> files = payload.getFileList(storage(), PARTITION_PATH);

    assertEquals(1, files.size());
    assertEquals(new StoragePath(SOURCE_PARTITION_PATH, "f1.parquet"), files.get(0).getPath());
  }

  @Test
  public void testRegisteredAndOwnedFilesCoexistInOnePartition() {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    fileInfo.put("owned.parquet", new HoodieMetadataFileInfo(64L, false, null));
    fileInfo.put("registered.parquet", new HoodieMetadataFileInfo(64L, false, SOURCE_PARTITION_PATH));

    List<StoragePathInfo> files = payloadFor(fileInfo).getFileList(storage(), PARTITION_PATH);

    assertEquals(2, files.size());
    Map<String, String> byName = new HashMap<>();
    files.forEach(f -> byName.put(f.getPath().getName(), f.getPath().getParent().toString()));
    assertEquals(PARTITION_PATH.toString(), byName.get("owned.parquet"));
    assertEquals(SOURCE_PARTITION_PATH, byName.get("registered.parquet"));
  }

  @Test
  public void testMergePreservesSourcePartitionPath() {
    HoodieMetadataPayload older = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(64L, false, SOURCE_PARTITION_PATH)));
    HoodieMetadataPayload newer = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(128L, false, SOURCE_PARTITION_PATH)));

    Map<String, HoodieMetadataFileInfo> combined =
        HoodieTableMetadataUtil.combineFileSystemMetadata(older, newer);

    assertEquals(128L, combined.get("f1.parquet").getSize());
    assertEquals(SOURCE_PARTITION_PATH, combined.get("f1.parquet").getSourcePartitionPath());
  }

  @Test
  public void testMergeIsCommutativeWhenOnlyOneSideNamesASourcePartitionPath() {
    HoodieMetadataPayload owned = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(128L, false, null)));
    HoodieMetadataPayload registered = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(64L, false, SOURCE_PARTITION_PATH)));

    Map<String, HoodieMetadataFileInfo> ownedFirst =
        HoodieTableMetadataUtil.combineFileSystemMetadata(owned, registered);
    Map<String, HoodieMetadataFileInfo> registeredFirst =
        HoodieTableMetadataUtil.combineFileSystemMetadata(registered, owned);

    assertEquals(registeredFirst.get("f1.parquet").getSourcePartitionPath(),
        ownedFirst.get("f1.parquet").getSourcePartitionPath());
    assertEquals(registeredFirst.get("f1.parquet").getSize(), ownedFirst.get("f1.parquet").getSize());
  }

  @Test
  public void testDeletingARegisteredFileDropsItRatherThanKeepingItsSourcePartitionPath() {
    HoodieMetadataPayload registered = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(64L, false, SOURCE_PARTITION_PATH)));
    HoodieMetadataPayload deleted = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(0L, true, null)));

    Map<String, HoodieMetadataFileInfo> combined =
        HoodieTableMetadataUtil.combineFileSystemMetadata(registered, deleted);

    assertNull(combined.get("f1.parquet"));
  }

  @Test
  public void testConflictingSourcePartitionPathsResolveTheSameWayInEitherOrder() {
    HoodieMetadataPayload here = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(64L, false, "/data/a/datestr=2022-06-15")));
    HoodieMetadataPayload there = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(64L, false, "/data/b/datestr=2022-06-15")));

    Map<String, HoodieMetadataFileInfo> oneWay = HoodieTableMetadataUtil.combineFileSystemMetadata(here, there);
    Map<String, HoodieMetadataFileInfo> otherWay = HoodieTableMetadataUtil.combineFileSystemMetadata(there, here);

    assertEquals(otherWay.get("f1.parquet").getSourcePartitionPath(),
        oneWay.get("f1.parquet").getSourcePartitionPath());
  }

  @Test
  public void testASourcePartitionPathThatIsNotAnAbsoluteDirectoryIsRejected() {
    Map<String, Long> filesAdded = Collections.singletonMap("f1.parquet", 128L);

    assertThrows(IllegalStateException.class, () -> HoodieMetadataPayload.createPartitionFilesRecord(
        "datestr=2022-06-15", filesAdded, Collections.singletonMap("f1.parquet", ""),
        Collections.emptyList(), false));
    assertThrows(IllegalStateException.class, () -> HoodieMetadataPayload.createPartitionFilesRecord(
        "datestr=2022-06-15", filesAdded, Collections.singletonMap("f1.parquet", "data/hive_table"),
        Collections.emptyList(), false));
  }

  /**
   * Reads bytes written against the old schema back through the current one, so Avro's own writer
   * /reader resolution is what fills the absent field in, rather than a hand-built record.
   */
  @Test
  public void testFileInfoWrittenAgainstTheOldSchemaResolvesToANullSourcePartitionPath() throws Exception {
    Schema oldFileInfoSchema = new Schema.Parser().parse(FILE_INFO_SCHEMA_WITHOUT_SOURCE_PARTITION_PATH);

    GenericRecord oldFileInfo = new GenericData.Record(oldFileInfoSchema);
    oldFileInfo.put("size", 128L);
    oldFileInfo.put("isDeleted", false);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(oldFileInfoSchema).write(oldFileInfo, encoder);
    encoder.flush();

    GenericRecord resolved = new GenericDatumReader<GenericRecord>(oldFileInfoSchema, HoodieMetadataFileInfo.SCHEMA$)
        .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

    assertEquals(128L, resolved.get("size"));
    assertNull(resolved.get("sourcePartitionPath"));
  }

  @Test
  public void testRecordWrittenBeforeTheFieldExistedStillReads() {
    Schema oldSchema = new Schema.Parser().parse(SCHEMA_WITHOUT_SOURCE_PARTITION_PATH);
    Schema fileInfoSchema = oldSchema.getField("filesystemMetadata").schema()
        .getTypes().get(1).getValueType();

    GenericRecord oldFileInfo = new GenericData.Record(fileInfoSchema);
    oldFileInfo.put("size", 128L);
    oldFileInfo.put("isDeleted", false);

    // A mutable map: the payload rewrites the entries of this map in place as it reads them.
    Map<String, GenericRecord> fileInfoByName = new HashMap<>();
    fileInfoByName.put("f1.parquet", oldFileInfo);

    GenericRecord oldRecord = new GenericData.Record(oldSchema);
    oldRecord.put("key", "datestr=2022-06-15");
    oldRecord.put("type", MetadataPartitionType.FILES.getRecordType());
    oldRecord.put("filesystemMetadata", fileInfoByName);

    HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(oldRecord));

    List<StoragePathInfo> files = payload.getFileList(storage(), PARTITION_PATH);
    assertEquals(1, files.size());
    assertEquals(new StoragePath(PARTITION_PATH, "f1.parquet"), files.get(0).getPath());
    assertNull(payload.filesystemMetadata.get("f1.parquet").getSourcePartitionPath());
  }
}
