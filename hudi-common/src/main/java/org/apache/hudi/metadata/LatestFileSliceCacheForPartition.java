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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.TableFileSystemView;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A thread-safe singleton cache for storing the latest file slices of a metadata table partition.
 *
 * <p>This cache is primarily used to optimize Record Level Index (RLI) lookups by avoiding
 * repeated computation of file slices for the same partition and instant time. The cache stores
 * file slices sorted by fileId, which is critical for hash-based record key distribution used
 * in RLI lookups (see {@link HoodieTableMetadataUtil#mapRecordKeyToFileGroupIndex}).
 *
 * <h2>Cache Invalidation</h2>
 * <p>The cache is automatically invalidated and rebuilt when:
 * <ul>
 *   <li>The instant time changes (e.g., new commits to the metadata table)</li>
 *   <li>The partition path changes (e.g., switching between RLI partitions)</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>This class uses double-checked locking to ensure thread-safe lazy initialization
 * and cache updates. Only one thread will populate the cache while others wait.
 *
 * <h2>Sorting Requirement</h2>
 * <p><b>IMPORTANT:</b> File slices are sorted by fileId to ensure consistent positional indexing.
 * The hash-based distribution in {@link HoodieTableMetadataUtil#mapRecordKeyToFileGroupIndex}
 * assumes that file slices are in a deterministic order. Without sorting, lookups may search
 * the wrong file group and fail to find records.
 *
 * @see HoodieBackedTableMetadata
 * @see HoodieTableMetadataUtil#mapRecordKeyToFileGroupIndex
 */
public class LatestFileSliceCacheForPartition {
  private static final Logger LOG = LoggerFactory.getLogger(LatestFileSliceCacheForPartition.class);

  private static volatile Cache<CacheKey, List<FileSlice>> LATEST_FILE_SLICE_CACHE_FOR_PARTITION = null;
  private static volatile CacheKey INSTANT_TIME_AND_PARTITION_CACHED = null;

  /**
   * Gets or creates a cache containing the latest file slices for a metadata table partition.
   *
   * <p>If the cache doesn't exist or the cached key (instant time + partition) differs from
   * the requested key, the cache is invalidated and rebuilt with fresh data from the slice view.
   *
   * <p>The returned file slices are sorted by fileId to ensure deterministic ordering for
   * hash-based lookups in RLI.
   *
   * @param sliceView        the file system view to fetch file slices from
   * @param cacheKey         the cache key containing instant time and partition path
   * @param maxCacheSize     maximum number of entries the cache can hold
   * @param expirationInMins time in minutes after which cache entries expire
   * @return the cache instance populated with file slices for the given partition
   */
  public static Cache<CacheKey, List<FileSlice>> getCache(TableFileSystemView.SliceView sliceView,
                                                          CacheKey cacheKey, int maxCacheSize, int expirationInMins) {
    if (LATEST_FILE_SLICE_CACHE_FOR_PARTITION == null || INSTANT_TIME_AND_PARTITION_CACHED == null
        || (!INSTANT_TIME_AND_PARTITION_CACHED.equals(cacheKey))) {
      synchronized (LatestFileSliceCacheForPartition.class) {
        if (LATEST_FILE_SLICE_CACHE_FOR_PARTITION == null || INSTANT_TIME_AND_PARTITION_CACHED == null
            || (!INSTANT_TIME_AND_PARTITION_CACHED.equals(cacheKey))) {
          if (LATEST_FILE_SLICE_CACHE_FOR_PARTITION != null) {
            LATEST_FILE_SLICE_CACHE_FOR_PARTITION.cleanUp();
          }
          LOG.info("Instantiating LatestFileSliceCacheForPartition for instant {} and partition {}",
              cacheKey.getInstantTime(), cacheKey.getPartitionPath());
          LATEST_FILE_SLICE_CACHE_FOR_PARTITION = Caffeine.newBuilder()
              .maximumSize(maxCacheSize)
              .expireAfterWrite(Duration.of(expirationInMins, ChronoUnit.MINUTES))
              .build();
          LOG.info("Populating entries into Latest file slice for partition cache with instant time {} and partition {} : Started ",
              cacheKey.getInstantTime(), cacheKey.getPartitionPath());
          // populate cache w/ latest file slice for all file groups
          // IMPORTANT: File slices MUST be sorted by fileId to ensure consistent positional indexing
          // in RLI lookups. The hash-based distribution (mapRecordKeyToFileGroupIndex) assumes sorted order.
          LATEST_FILE_SLICE_CACHE_FOR_PARTITION.put(cacheKey,
              sliceView.getLatestMergedFileSlicesBeforeOrOn(cacheKey.getPartitionPath(), cacheKey.getInstantTime())
                  .sorted(Comparator.comparing(FileSlice::getFileId))
                  .collect(Collectors.toList()));
          INSTANT_TIME_AND_PARTITION_CACHED = cacheKey;
          LOG.info("Populating entries into Latest file slice for partition cache with instant time {} and partition {} : Completed. Total entries {}",
              cacheKey.getInstantTime(), cacheKey.getPartitionPath(),
              LATEST_FILE_SLICE_CACHE_FOR_PARTITION.estimatedSize());
        } else {
          LOG.info("Already some other concurrent task populated the entries for Latest file slice for partition. "
              + "Hence, skipping to populate entries. Total entries " + LATEST_FILE_SLICE_CACHE_FOR_PARTITION.estimatedSize());
        }
      }
    }
    return LATEST_FILE_SLICE_CACHE_FOR_PARTITION;
  }

  /**
   * Cache key for file slice cache, containing instant time and partition path.
   */
  public static class CacheKey {
    private final String instantTime;
    private final String partitionPath;

    public CacheKey(String instantTime, String partitionPath) {
      this.instantTime = instantTime;
      this.partitionPath = partitionPath;
    }

    public static CacheKey of(String instantTime, String partitionPath) {
      return new CacheKey(instantTime, partitionPath);
    }

    public String getInstantTime() {
      return instantTime;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey that = (CacheKey) o;
      return Objects.equals(instantTime, that.instantTime) && Objects.equals(partitionPath, that.partitionPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(instantTime, partitionPath);
    }

    @Override
    public String toString() {
      return "CacheKey{instantTime='" + instantTime + "', partitionPath='" + partitionPath + "'}";
    }
  }
}
