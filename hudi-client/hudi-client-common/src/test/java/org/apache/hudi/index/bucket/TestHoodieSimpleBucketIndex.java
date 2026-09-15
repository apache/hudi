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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestHoodieSimpleBucketIndex {

  @Test
  void testPendingDataFileDetectionHandlesBucketAndNonBucketFiles() {
    HoodieSimpleBucketIndex index = new HoodieSimpleBucketIndex(config(8));
    List<StoragePathInfo> files = Arrays.asList(
        file("/table/p/00000003-file_002.parquet", true),
        file("/table/p/not-a-bucket-file", true),
        file("/table/p/00000003-file_001.parquet", false));

    assertTrue(index.hasPendingDataFilesForInstant(files, "002", 3));
    assertFalse(index.hasPendingDataFilesForInstant(files, "999", 3));
    assertFalse(index.hasPendingDataFilesForInstant(files, "002", 4));
    assertFalse(index.canIndexLogFiles());
  }

  @Test
  void testFindConflictInstantsUsesInflightSlicesAndToleratesListingFailure() throws Exception {
    HoodieSimpleBucketIndex index = new HoodieSimpleBucketIndex(config(8));
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    TableFileSystemView.SliceView sliceView = mock(TableFileSystemView.SliceView.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
    when(metaClient.getStorage()).thenReturn(storage);
    when(table.getSliceView()).thenReturn(sliceView);
    when(storage.listFiles(new StoragePath("/table/p"))).thenReturn(Collections.singletonList(
        file("/table/p/00000003-file_002.parquet", true)));
    when(sliceView.getLatestFileSlicesIncludingInflight("p")).thenAnswer(ignored -> Stream.of(
        new FileSlice("p", "002", "00000003-file"),
        new FileSlice("p", "003", "00000003-file")));

    assertEquals(Collections.singletonList("002"),
        index.findConflictInstantsInPartition(table, "p", 3, Set.of("002")));

    when(storage.listFiles(any(StoragePath.class))).thenThrow(new IOException("listing failure"));
    assertEquals(Collections.emptyList(),
        index.findConflictInstantsInPartition(table, "p", 3, Set.of("002")));
  }

  @Test
  void testLoadBucketMappingAndDuplicateDetection() throws Exception {
    HoodieSimpleBucketIndex index = new HoodieSimpleBucketIndex(config(8));
    HoodieTable table = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline timeline = mock(HoodieActiveTimeline.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    TableFileSystemView.SliceView sliceView = mock(TableFileSystemView.SliceView.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.reloadActiveTimeline()).thenReturn(timeline);
    when(timeline.filterInflights()).thenReturn(timeline);
    when(timeline.getInstantsAsStream()).thenAnswer(ignored -> Stream.empty());
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/table"));
    when(metaClient.getStorage()).thenReturn(storage);
    when(storage.listFiles(any(StoragePath.class))).thenReturn(Collections.emptyList());
    when(table.getSliceView()).thenReturn(sliceView);
    when(sliceView.getLatestFileSlicesIncludingInflight("p")).thenAnswer(ignored -> Stream.empty());
    FileSlice first = new FileSlice("p", "001", "00000003-file-a");
    FileSlice secondBucket = new FileSlice("p", "001", "00000004-file-b");

    try (MockedStatic<HoodieIndexUtils> indexUtils = mockStatic(HoodieIndexUtils.class)) {
      indexUtils.when(() -> HoodieIndexUtils.getLatestFileSlicesForPartition("p", table))
          .thenReturn(Arrays.asList(first, secondBucket));
      Map<Integer, HoodieRecordLocation> mapping = index.loadBucketIdToFileIdMappingForPartition(table, "p");
      assertEquals(Set.of(3, 4), mapping.keySet());
      assertEquals("00000003-file-a", mapping.get(3).getFileId());

      indexUtils.when(() -> HoodieIndexUtils.getLatestFileSlicesForPartition("p", table))
          .thenReturn(Arrays.asList(first, new FileSlice("p", "002", "00000003-file-conflict")));
      assertThrows(HoodieIOException.class,
          () -> index.loadBucketIdToFileIdMappingForPartition(table, "p"));
    }
  }

  @Test
  void testIndexLocationFunctionAssignsAndLooksUpBucket() {
    HoodieWriteConfig config = config(8);
    HoodieKey key = new HoodieKey("record-key", "p");
    int bucket = BucketIdentifier.getBucketId(key.getRecordKey(), "_hoodie_record_key", 8);
    HoodieRecordLocation expected = new HoodieRecordLocation("001", BucketIdentifier.bucketIdStr(bucket) + "-file");
    TestableSimpleBucketIndex index = new TestableSimpleBucketIndex(config, Collections.singletonMap(bucket, expected));
    HoodieTable table = mock(HoodieTable.class);
    when(table.getConfig()).thenReturn(config);
    HoodieRecord record = mock(HoodieRecord.class);
    when(record.getKey()).thenReturn(key);
    when(record.getPartitionPath()).thenReturn("p");

    Function<HoodieRecord, Option<HoodieRecordLocation>> locationFunction = index.locationFunction(table, "p");
    assertEquals(expected, locationFunction.apply(record).get());

    HoodieKey missingKey = new HoodieKey("different-key", "p");
    while (BucketIdentifier.getBucketId(missingKey.getRecordKey(), "_hoodie_record_key", 8) == bucket) {
      missingKey = new HoodieKey(missingKey.getRecordKey() + "x", "p");
    }
    when(record.getKey()).thenReturn(missingKey);
    assertFalse(locationFunction.apply(record).isPresent());
    assertEquals(BucketIdentifier.getBucketId(missingKey.getRecordKey(), "_hoodie_record_key", 8),
        index.getBucketID(missingKey, 8));
  }

  private static HoodieWriteConfig config(int buckets) {
    return HoodieWriteConfig.newBuilder()
        .withPath("/table")
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withBucketNum(String.valueOf(buckets))
            .withIndexKeyField("_hoodie_record_key")
            .build())
        .build();
  }

  private static StoragePathInfo file(String path, boolean isFile) {
    return new StoragePathInfo(new StoragePath(path), 1L, !isFile, (short) 1, 1L, 1L);
  }

  private static class TestableSimpleBucketIndex extends HoodieSimpleBucketIndex {
    private final Map<Integer, HoodieRecordLocation> mapping;

    TestableSimpleBucketIndex(HoodieWriteConfig config, Map<Integer, HoodieRecordLocation> mapping) {
      super(config);
      this.mapping = mapping;
    }

    @Override
    public Map<Integer, HoodieRecordLocation> loadBucketIdToFileIdMappingForPartition(HoodieTable table, String partition) {
      return mapping;
    }

    Function<HoodieRecord, Option<HoodieRecordLocation>> locationFunction(HoodieTable table, String partition) {
      return getIndexLocationFunctionForPartition(table, partition);
    }
  }
}
