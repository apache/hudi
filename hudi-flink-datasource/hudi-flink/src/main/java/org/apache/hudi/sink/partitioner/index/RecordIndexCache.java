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
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class RecordIndexCache implements Closeable {
  @VisibleForTesting
  @Getter
  private final TreeMap<Long, ExternalSpillableMap<String, HoodieRecordGlobalLocation>> caches;
  private final HoodieWriteConfig writeConfig;
  @VisibleForTesting
  @Getter
  private final long maxCacheSizeInBytes;
  // the minimum checkpoint id for the inflight hoodie instant to be retained
  private long minRetainedCheckpointId;
  private long recordCnt = 0;

  /**
   * Check the total memory size of the cache after inserting 100 records
   */
  private static final int NUMBER_OF_RECORDS_TO_CHECK_MEMORY_SIZE = 100;

  /**
   * Factor for estimating the real size of memory used by a spilled map
   */
  @VisibleForTesting
  public static final double FACTOR_FOR_MEMORY_SIZE_OF_SPILLED_MAP = 0.8;

  public RecordIndexCache(Configuration conf, long initCheckpointId) {
    this.caches = new TreeMap<>(Comparator.reverseOrder());
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, false, false);
    this.maxCacheSizeInBytes = conf.get(FlinkOptions.INDEX_RLI_CACHE_SIZE) * 1024 * 1024;
    this.minRetainedCheckpointId = Integer.MIN_VALUE;
    addCheckpointCache(initCheckpointId);
  }

  /**
   * Add a new checkpoint cache for the given checkpoint ID.
   *
   * @param checkpointId the checkpoint ID
   */
  public void addCheckpointCache(long checkpointId) {
    try {
      long inferredCacheSize = inferMemorySizeForCache();
      // clean the caches for committed instants if there is no enough memory
      cleanIfNecessary(inferredCacheSize);
      // Create a new ExternalSpillableMap for this checkpoint
      ExternalSpillableMap<String, HoodieRecordGlobalLocation> newCache =
          new ExternalSpillableMap<>(
              inferredCacheSize,
              writeConfig.getSpillableMapBasePath(),
              new DefaultSizeEstimator<>(),
              new DefaultSizeEstimator<>(),
              // Using ROCKS_DB disk map always. For BITCASK type, there will be extra memory
              // cost for each key during spilling: key -> ValueMetadata(filePath, valueSize, position, ts)
              // So it's redundant to use BITCASK disk map since we are using the map to store
              // HoodieRecordGlobalLocation which has similar size as ValueMetadata.
              ExternalSpillableMap.DiskMapType.ROCKS_DB,
              new DefaultSerializer<>(),
              writeConfig.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED),
              "RecordIndexCache-" + checkpointId);
      caches.put(checkpointId, newCache);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create checkpoint cache for checkpoint ID: " + checkpointId, e);
    }
  }

  /**
   * Infer the size of memory for the cache:
   * - The memory size for the first spillable map is MAX_MEM / 2
   * - The memory size for the following checkpoint cache is the average used memory of all existed caches.
   * - Clean caches for committed instants if the remaining memory is not enough for the new cache.
   */
  @VisibleForTesting
  long inferMemorySizeForCache() {
    if (caches.isEmpty()) {
      return maxCacheSizeInBytes / 2;
    }

    long totalUsedMemory = caches.values().stream()
        .map(
            // if the cache is spilled, adjust the actual used memory by multiplying a factor
            m -> m.getSizeOfFileOnDiskInBytes() > 0
                ? (long) (m.getCurrentInMemoryMapSize() / FACTOR_FOR_MEMORY_SIZE_OF_SPILLED_MAP)
                : m.getCurrentInMemoryMapSize())
        .reduce(Long::sum).orElse(0L);
    long avgUsedMemorySize = totalUsedMemory / caches.size();

    if (avgUsedMemorySize <= 0) {
      avgUsedMemorySize = maxCacheSizeInBytes / 2;
    }
    return avgUsedMemorySize;
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

    if ((++recordCnt) % NUMBER_OF_RECORDS_TO_CHECK_MEMORY_SIZE == 0) {
      cleanIfNecessary(1L);
      recordCnt = 0;
    }
  }

  /**
   * Mark the cache entries as evictable, whose checkpoint id is less than the given checkpoint id.
   *
   * @param checkpointId The checkpoint id for the minimum inflight instant
   */
  public void markAsEvictable(long checkpointId) {
    ValidationUtils.checkArgument(checkpointId >= minRetainedCheckpointId,
        String.format("The checkpoint id for minium inflight instant should be increased,"
            + " ckpIdForMinInflightInstant: %s, received checkpointId: %s", minRetainedCheckpointId, checkpointId));
    minRetainedCheckpointId = checkpointId;
  }

  /**
   * Perform the actual cleaning of the cache to free up memories for new record index records.
   *
   * @param nextCacheSize the size for the next new cache
   */
  private void cleanIfNecessary(long nextCacheSize) {
    while (!caches.isEmpty() && caches.lastKey() < minRetainedCheckpointId
        && getInMemoryMapSize() + nextCacheSize > this.maxCacheSizeInBytes) {
      NavigableMap.Entry<Long, ExternalSpillableMap<String, HoodieRecordGlobalLocation>> lastEntry = caches.pollLastEntry();
      lastEntry.getValue().close();
      log.info("Clean record index cache for checkpoint: {}", lastEntry.getKey());
    }
  }

  private long getInMemoryMapSize() {
    return caches.values().stream().map(ExternalSpillableMap::getCurrentInMemoryMapSize).reduce(Long::sum).orElse(0L);
  }

  @Override
  public void close() throws IOException {
    // Close all the ExternalSpillableMap instances before removing them
    caches.values().forEach(ExternalSpillableMap::close);
    // Close all ExternalSpillableMap instances before clearing the cache
    caches.clear();
  }
}
