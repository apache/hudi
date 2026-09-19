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
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that a files-partition entry carrying a source base path resolves outside the table.
 */
public class TestHoodieMetadataPayloadSourceBasePath {

  private static final StoragePath PARTITION_PATH = new StoragePath("/tmp/hudi_table/datestr=2022-06-15");

  private HoodieStorage storage() {
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getDefaultBlockSize(any(StoragePath.class))).thenReturn(1024);
    return storage;
  }

  private HoodieMetadataPayload payloadFor(Map<String, HoodieMetadataFileInfo> fileInfo) {
    return new HoodieMetadataPayload("datestr=2022-06-15", MetadataPartitionType.FILES.getRecordType(), fileInfo);
  }

  @Test
  public void testFileWithoutSourceBasePathResolvesUnderThePartition() {
    HoodieMetadataPayload payload = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(128L, false, null)));

    List<StoragePathInfo> files = payload.getFileList(storage(), PARTITION_PATH);

    assertEquals(1, files.size());
    assertEquals(new StoragePath(PARTITION_PATH, "f1.parquet"), files.get(0).getPath());
    assertEquals(128L, files.get(0).getLength());
  }

  @Test
  public void testFileWithSourceBasePathResolvesOutsideTheTable() {
    String sourceBasePath = "/data/hive_table/datestr=2022-06-15";
    HoodieMetadataPayload payload = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(128L, false, sourceBasePath)));

    List<StoragePathInfo> files = payload.getFileList(storage(), PARTITION_PATH);

    assertEquals(1, files.size());
    assertEquals(new StoragePath(sourceBasePath, "f1.parquet"), files.get(0).getPath());
  }

  @Test
  public void testRegisteredAndOwnedFilesCoexistInOnePartition() {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    fileInfo.put("owned.parquet", new HoodieMetadataFileInfo(64L, false, null));
    fileInfo.put("registered.parquet", new HoodieMetadataFileInfo(64L, false, "/data/hive_table/datestr=2022-06-15"));

    List<StoragePathInfo> files = payloadFor(fileInfo).getFileList(storage(), PARTITION_PATH);

    assertEquals(2, files.size());
    Map<String, String> byName = new HashMap<>();
    files.forEach(f -> byName.put(f.getPath().getName(), f.getPath().getParent().toString()));
    assertEquals(PARTITION_PATH.toString(), byName.get("owned.parquet"));
    assertEquals("/data/hive_table/datestr=2022-06-15", byName.get("registered.parquet"));
  }

  @Test
  public void testMergePreservesSourceBasePath() {
    HoodieMetadataPayload older = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(64L, false, "/data/hive_table/datestr=2022-06-15")));
    HoodieMetadataPayload newer = payloadFor(
        Collections.singletonMap("f1.parquet", new HoodieMetadataFileInfo(128L, false, "/data/hive_table/datestr=2022-06-15")));

    Map<String, HoodieMetadataFileInfo> combined =
        HoodieTableMetadataUtil.combineFileSystemMetadata(older, newer);

    assertEquals(128L, combined.get("f1.parquet").getSize());
    assertEquals("/data/hive_table/datestr=2022-06-15", combined.get("f1.parquet").getSourceBasePath());
  }
}
