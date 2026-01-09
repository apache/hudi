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

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link RecordIndexCache}.
 */
public class TestRecordIndexCache {
  @TempDir
  File tempDir;
  private RecordIndexCache cache;

  @BeforeEach
  void setUp() {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    conf.set(FlinkOptions.INDEX_RLI_CACHE_SIZE, 100L); // 100MB cache size
    this.cache = new RecordIndexCache(conf, 1L);
  }

  @AfterEach
  void clean() throws IOException {
    this.cache.close();
  }

  @Test
  void testConstructor() {
    // Test constructor with initial checkpoint ID
    assertNotNull(cache.getCaches());
    assertEquals(1, cache.getCaches().size());
    assertTrue(cache.getCaches().containsKey(1L));
  }

  @Test
  void testAddCheckpointCache() {
    // Add another checkpoint cache
    cache.addCheckpointCache(2L);
    
    assertEquals(2, cache.getCaches().size());
    assertTrue(cache.getCaches().containsKey(1L));
    assertTrue(cache.getCaches().containsKey(2L));
    
    // Check that checkpoints are stored in reverse order (2L should be first in reverse-ordered TreeMap)
    assertEquals(Long.valueOf(2L), cache.getCaches().firstKey());
    assertEquals(Long.valueOf(1L), cache.getCaches().lastKey());
  }

  @Test
  void testUpdateAndGet() {
    String recordKey = "key1";
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partition1", "1001", "file_id1");
    
    // Initially should return null
    assertNull(cache.get(recordKey));
    
    // Update the cache with a record location
    cache.update(recordKey, location);
    
    // Should now return the location
    HoodieRecordGlobalLocation retrievedLocation = cache.get(recordKey);
    assertEquals(location, retrievedLocation);
  }

  @Test
  void testUpdateWithMultipleCheckpoints() {
    cache.addCheckpointCache(2L);

    String recordKey = "key1";
    HoodieRecordGlobalLocation location1 = new HoodieRecordGlobalLocation("partition1", "1001", "file_id1");
    HoodieRecordGlobalLocation location2 = new HoodieRecordGlobalLocation("partition2", "1002", "file_id2");
    
    // Update in checkpoint 2
    cache.update(recordKey, location1);
    
    // Check that it's in the highest checkpoint cache (2L)
    HoodieRecordGlobalLocation retrieved = cache.get(recordKey);
    assertEquals(location1, retrieved);
    
    // Add to checkpoint 3 and update again
    cache.addCheckpointCache(3L);
    cache.update(recordKey, location2);
    
    // Should now return the updated location
    retrieved = cache.get(recordKey);
    assertEquals(location2, retrieved);
  }

  @Test
  void testGetFromMultipleCheckpoints() {
    cache.addCheckpointCache(2L);
    
    String recordKey1 = "key1";
    String recordKey2 = "key2";
    HoodieRecordGlobalLocation location1 = new HoodieRecordGlobalLocation("partition1", "1001", "file_id1");
    HoodieRecordGlobalLocation location2 = new HoodieRecordGlobalLocation("partition2", "1002", "file_id2");
    
    // Add to checkpoint 1
    cache.getCaches().get(1L).put(recordKey1, location1);

    cache.getCaches().get(1L).put(recordKey2, location1);

    // Add to checkpoint 2 (higher checkpoint should take precedence)
    cache.getCaches().get(2L).put(recordKey1, location2);
    
    // Should return from higher checkpoint (2L) first
    HoodieRecordGlobalLocation retrieved = cache.get(recordKey1);
    assertEquals(location2, retrieved);

    // Should return from previous checkpoint (1L)
    retrieved = cache.get(recordKey2);
    assertEquals(location1, retrieved);
  }

  @Test
  void testClean() {
    cache.addCheckpointCache(2L);
    cache.addCheckpointCache(3L);
    cache.addCheckpointCache(4L);
    
    String recordKey1 = "key1";
    String recordKey2 = "key2";
    HoodieRecordGlobalLocation location1 = new HoodieRecordGlobalLocation("partition1", "1001", "file_id1");
    HoodieRecordGlobalLocation location2 = new HoodieRecordGlobalLocation("partition2", "1002", "file_id2");
    
    // Add records to different checkpoints
    cache.getCaches().get(1L).put(recordKey1, location1);
    cache.getCaches().get(2L).put(recordKey2, location2);
    
    // Verify records exist before cleaning
    assertNotNull(cache.get(recordKey1));
    assertNotNull(cache.get(recordKey2));
    
    // Clean checkpoints up to and including 2
    cache.clean(3L);
    
    // Check that checkpoints 1 and 2 are removed
    assertEquals(2, cache.getCaches().size()); // Should have checkpoints 3 and 4
    assertFalse(cache.getCaches().containsKey(1L));
    assertFalse(cache.getCaches().containsKey(2L));
    assertTrue(cache.getCaches().containsKey(3L));
    assertTrue(cache.getCaches().containsKey(4L));
    
    // Records from cleaned checkpoints should no longer be accessible
    assertNull(cache.get(recordKey1));
    assertNull(cache.get(recordKey2));
  }

  @Test
  void testClose() throws IOException {
    cache.addCheckpointCache(2L);
    
    String recordKey = "key1";
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partition1", "1001", "file_id1");
    cache.update(recordKey, location);
    
    // Verify cache has entries before closing
    assertEquals(2, cache.getCaches().size());
    assertNotNull(cache.get(recordKey));
    
    // Close the cache
    cache.close();
    
    // After closing, the cache should be empty
    assertEquals(0, cache.getCaches().size());
  }

  @Test
  void testUpdateWithEmptyCache() {
    // Clear the cache to test error condition
    cache.getCaches().clear();

    String recordKey = "key1";
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partition1", "1001", "file_id");

    // Should throw an exception when trying to update an empty cache
    assertThrows(IllegalArgumentException.class, () -> {
      cache.update(recordKey, location);
    });
  }

  @Test
  void testSpillToDisk() throws IOException {
    // Create a new cache with a very small size to force spilling
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    conf.set(FlinkOptions.INDEX_RLI_CACHE_SIZE, 1L); // 1MB cache size to force spilling

    try (RecordIndexCache smallCache = new RecordIndexCache(conf, 1L)) {
      String recordKeyPrefix = "key";
      List<HoodieRecordGlobalLocation> locations = new ArrayList<>();
      for (int i = 0; i < 5000; i++) {
        HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partition1", "1001", "file_id1", i);
        locations.add(location);
        // Update the cache with a record location
        smallCache.update(recordKeyPrefix + i, location);
      }

      // Verify that the data has been spilled to disk by checking the underlying ExternalSpillableMap
      ExternalSpillableMap<String, HoodieRecordGlobalLocation> spillableMap = smallCache.getCaches().get(1L);
      assertTrue(spillableMap.getDiskBasedMapNumEntries() > 0, "Data should be spilled to disk");

      for (int i = 0; i < 5000; i++) {
        HoodieRecordGlobalLocation retrievedLocation = smallCache.get(recordKeyPrefix + i);
        assertEquals(locations.get(i), retrievedLocation);
      }
    }
  }

  @Test
  void testSpillWithMultipleCheckpoints() throws IOException {
    // Create a new cache with a very small size to force spilling
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    conf.set(FlinkOptions.INDEX_RLI_CACHE_SIZE, 1L); // 1MB cache size to force spilling

    try (RecordIndexCache smallCache = new RecordIndexCache(conf, 1L)) {
      // Add records to multiple checkpoints
      String recordKeyPrefix = "key";
      for (int i = 0; i < 5000; i++) {
        HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partition1", "1001", "file_id1", i);
        // Update the cache with a record location
        smallCache.update(recordKeyPrefix + i, location);
      }
      // Verify that the data has been spilled to disk by checking the underlying ExternalSpillableMap
      ExternalSpillableMap<String, HoodieRecordGlobalLocation> spillableMap = smallCache.getCaches().get(1L);
      assertTrue(spillableMap.getDiskBasedMapNumEntries() > 0, "Data should be spilled to disk");

      // Add another checkpoint
      smallCache.addCheckpointCache(2L);
      List<HoodieRecordGlobalLocation> locations = new ArrayList<>();
      for (int i = 0; i < 5000; i++) {
        HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partition1", "1001", "file_id2", i);
        locations.add(location);
        // Update the cache with a record location
        smallCache.update(recordKeyPrefix + i, location);
      }
      // Verify that the data has been spilled to disk by checking the underlying ExternalSpillableMap
      spillableMap = smallCache.getCaches().get(1L);
      assertTrue(spillableMap.getDiskBasedMapNumEntries() > 0, "Data should be spilled to disk");

      for (int i = 0; i < 5000; i++) {
        HoodieRecordGlobalLocation retrievedLocation = smallCache.get(recordKeyPrefix + i);
        assertEquals(locations.get(i), retrievedLocation);
      }
    }
  }
}