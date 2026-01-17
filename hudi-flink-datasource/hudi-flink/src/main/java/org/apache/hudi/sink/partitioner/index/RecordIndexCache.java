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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.FlinkWriteClients;

import lombok.Getter;
import org.apache.flink.configuration.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Cache to hold the in-flight record level index entries which are not committed to metadata table yet.
 * <p>
 * todo: use map backed by flink managed memory.
 */
public class RecordIndexCache implements Closeable {
  @VisibleForTesting
  @Getter
  private final TreeMap<Long, ExternalSpillableMap<String, HoodieRecordGlobalLocation>> caches;
  private final HoodieWriteConfig writeConfig;
  private final long maxCacheSizeInBytes;

  public RecordIndexCache(Configuration conf, long initCheckpointId) {
    this.caches = new TreeMap<>(Comparator.reverseOrder());
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, false, false);
    this.maxCacheSizeInBytes = conf.get(FlinkOptions.INDEX_RLI_CACHE_SIZE) * 1024 * 1024;
    addCheckpointCache(initCheckpointId);
  }

  /**
   * Add a new checkpoint cache for the given checkpoint ID.
   *
   * @param checkpointId the checkpoint ID
   */
  public void addCheckpointCache(long checkpointId) {
    try {
      // Create a new ExternalSpillableMap for this checkpoint
      ExternalSpillableMap<String, HoodieRecordGlobalLocation> newCache =
          new ExternalSpillableMap<>(
              maxCacheSizeInBytes,
              writeConfig.getSpillableMapBasePath(),
              new DefaultSizeEstimator<>(),
              new DefaultSizeEstimator<>(),
              writeConfig.getCommonConfig().getSpillableDiskMapType(),
              new DefaultSerializer<>(),
              writeConfig.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED),
              "RecordIndexCache-" + checkpointId);
      caches.put(checkpointId, newCache);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create checkpoint cache for checkpoint ID: " + checkpointId, e);
    }
  }

  /**
   * Search the record location from caches with larger checkpoint id to that with smaller checkpoint id,
   * return early if the record location is found for the record key, return null otherwise.
   *
   * @param recordKey the record key for querying the location.
   * @return the record location.
   */
  public HoodieRecordGlobalLocation get(String recordKey) {
    // Iterate through the caches in descending order of checkpoint ID (larger to smaller)
    for (ExternalSpillableMap<String, HoodieRecordGlobalLocation> cache : caches.values()) {
      HoodieRecordGlobalLocation location = cache.get(recordKey);
      if (location != null) {
        return location;
      }
    }
    return null;
  }

  /**
   * Put the updated record location to the sub cache with the largest checkpoint id.
   *
   * @param recordKey the record key for querying the location.
   * @param recordGlobalLocation the record location.
   */
  public void update(String recordKey, HoodieRecordGlobalLocation recordGlobalLocation) {
    ValidationUtils.checkArgument(!caches.isEmpty(), "record index cache should not be empty.");
    // Get the sub cache with the largest checkpoint ID (first entry in the reverse-ordered TreeMap)
    caches.firstEntry().getValue().put(recordKey, recordGlobalLocation);
  }

  /**
   * Clean all the cache entries for checkpoint whose id is less than the given checkpoint id.
   *
   * @param checkpointId the id of checkpoint
   */
  public void clean(long checkpointId) {
    // Get all entries that are less than or equal to the given checkpointId
    NavigableMap<Long, ExternalSpillableMap<String, HoodieRecordGlobalLocation>> subMap = caches.tailMap(checkpointId, false);
    // Close all the ExternalSpillableMap instances before removing them
    subMap.values().forEach(ExternalSpillableMap::close);
    // Remove all the entries from the main cache
    subMap.clear();
  }

  @Override
  public void close() throws IOException {
    // Close all the ExternalSpillableMap instances before removing them
    caches.values().forEach(ExternalSpillableMap::close);
    // Close all ExternalSpillableMap instances before clearing the cache
    caches.clear();
  }
}
