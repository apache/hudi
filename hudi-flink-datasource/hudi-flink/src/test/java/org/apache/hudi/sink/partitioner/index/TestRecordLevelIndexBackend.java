/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link RecordLevelIndexBackend}.
 */
public class TestRecordLevelIndexBackend {

  private static final long ONE_MB = 1024 * 1024;

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString("hadoop.fs.defaultFS", "file:///");
    conf.set(FlinkOptions.INDEX_RLI_CACHE_SIZE, 1L);
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  public void testCheckpointCompleteDoesNotEagerEvict() throws Exception {
    try (RecordLevelIndexBackend backend = createBackend()) {
      backend.getPartitionBucketCaches().put("par1", cacheWithHeapSize(backend, 2 * ONE_MB, 1L));

      backend.onCheckpointComplete(new TestCorrespondent(Collections.emptyMap()), 2L);

      assertTrue(backend.getPartitionBucketCaches().containsKey("par1"));
    }
  }

  @Test
  public void testLazyEvictOldPartitionsWhenHeapExceedsLimit() throws Exception {
    try (RecordLevelIndexBackend backend = createBackend()) {
      backend.getPartitionBucketCaches().put("par1", cacheWithHeapSize(backend, 800 * 1024, 1L));
      backend.getPartitionBucketCaches().put("par2", cacheWithHeapSize(backend, 800 * 1024, 1L));
      backend.onCheckpointComplete(new TestCorrespondent(Collections.emptyMap()), 2L);
      backend.getPartitionBucketCaches().put("par3", cacheWithHeapSize(backend, 1L, 2L));

      backend.cleanIfNecessary(0L, "par3");

      assertFalse(backend.getPartitionBucketCaches().containsKey("par1"));
      assertTrue(backend.getPartitionBucketCaches().containsKey("par2"));
      assertTrue(backend.getPartitionBucketCaches().containsKey("par3"));
    }
  }

  @Test
  public void testLazyEvictKeepsRecentPartition() throws Exception {
    try (RecordLevelIndexBackend backend = createBackend()) {
      backend.getPartitionBucketCaches().put("old", cacheWithHeapSize(backend, 800 * 1024, 1L));
      backend.getPartitionBucketCaches().put("recent", cacheWithHeapSize(backend, 800 * 1024, 2L));
      backend.onCheckpointComplete(new TestCorrespondent(Collections.emptyMap()), 2L);

      backend.cleanIfNecessary(0L, null);

      assertFalse(backend.getPartitionBucketCaches().containsKey("old"));
      assertTrue(backend.getPartitionBucketCaches().containsKey("recent"));
    }
  }

  @Test
  public void testGetDoesNotRefreshLastUpdatedCheckpoint() throws Exception {
    try (RecordLevelIndexBackend backend = createBackend()) {
      backend.getPartitionBucketCaches().put("old", cacheWithHeapSize(backend, 2 * ONE_MB, 1L));
      backend.onCheckpoint(2L);

      backend.get("old", "key1");
      backend.onCheckpointComplete(new TestCorrespondent(Collections.emptyMap()), 2L);
      backend.cleanIfNecessary(0L, null);

      assertFalse(backend.getPartitionBucketCaches().containsKey("old"));
    }
  }

  @Test
  public void testPartitionCacheDictionaryEncodesFileGroupId() throws Exception {
    try (RecordLevelIndexBackend backend = createBackend()) {
      ExternalSpillableMap<String, Integer> recordKeyToFileGroupIdCode = mapWithStorage(0L);
      RecordLevelIndexBackend.BucketCache cache = backend.newBucketCache(recordKeyToFileGroupIdCode, 1L);

      cache.putRecordKey("key1", "file-group-id-000000000000000000000001");
      cache.putRecordKey("key2", "file-group-id-000000000000000000000001");
      cache.putRecordKey("key3", "file-group-id-000000000000000000000002");

      assertEquals("file-group-id-000000000000000000000001", cache.getFileGroupId("key1"));
      assertEquals("file-group-id-000000000000000000000001", cache.getFileGroupId("key2"));
      assertEquals("file-group-id-000000000000000000000002", cache.getFileGroupId("key3"));
      assertEquals(Integer.valueOf(0), recordKeyToFileGroupIdCode.get("key1"));
      assertEquals(Integer.valueOf(0), recordKeyToFileGroupIdCode.get("key2"));
      assertEquals(Integer.valueOf(1), recordKeyToFileGroupIdCode.get("key3"));
    }
  }

  @Test
  public void testLazyEvictUsesAccessOrder() throws Exception {
    try (RecordLevelIndexBackend backend = createBackend()) {
      backend.getPartitionBucketCaches().put("par1", cacheWithHeapSize(backend, 500 * 1024, 1L));
      backend.getPartitionBucketCaches().put("par2", cacheWithHeapSize(backend, 500 * 1024, 1L));
      backend.getPartitionBucketCaches().get("par1");
      backend.getPartitionBucketCaches().put("par3", cacheWithHeapSize(backend, 100 * 1024, 2L));
      backend.onCheckpointComplete(new TestCorrespondent(Collections.emptyMap()), 2L);

      backend.cleanIfNecessary(0L, "par3");

      assertTrue(backend.getPartitionBucketCaches().containsKey("par1"));
      assertFalse(backend.getPartitionBucketCaches().containsKey("par2"));
      assertTrue(backend.getPartitionBucketCaches().containsKey("par3"));
    }
  }

  private RecordLevelIndexBackend createBackend() {
    return new RecordLevelIndexBackend(conf, (partitionPath, recordKey, fileId) -> true);
  }

  private RecordLevelIndexBackend.BucketCache cacheWithHeapSize(
      RecordLevelIndexBackend indexBackend,
      long heapSize,
      long lastUpdatedCheckpoint) {
    return indexBackend.newBucketCache(mapWithStorage(heapSize), lastUpdatedCheckpoint);
  }

  private ExternalSpillableMap<String, Integer> mapWithStorage(long heapSize) {
    Map<String, Integer> storage = new HashMap<>();
    ExternalSpillableMap<String, Integer> recordKeyToFileGroupIdCode = Mockito.mock(ExternalSpillableMap.class);
    when(recordKeyToFileGroupIdCode.getCurrentInMemoryMapSize()).thenReturn(heapSize);
    when(recordKeyToFileGroupIdCode.size()).thenAnswer(invocation -> storage.size());
    when(recordKeyToFileGroupIdCode.get(Mockito.anyString()))
        .thenAnswer(invocation -> storage.get(invocation.getArgument(0)));
    doAnswer(invocation -> storage.put(invocation.getArgument(0), invocation.getArgument(1)))
        .when(recordKeyToFileGroupIdCode).put(Mockito.anyString(), Mockito.anyInt());
    return recordKeyToFileGroupIdCode;
  }

  private static class TestCorrespondent extends Correspondent {
    private final Map<Long, String> inflightInstants;

    TestCorrespondent(Map<Long, String> inflightInstants) {
      this.inflightInstants = inflightInstants;
    }

    @Override
    public Map<Long, String> requestInflightInstants() {
      return inflightInstants;
    }
  }
}
