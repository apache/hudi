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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.utils.SamplingActionExecutor;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Partition-scoped index backend backed by partitioned record level index.
 *
 * <p>The backend keeps a lazy cache per data partition. Each cache maps record keys owned by the
 * current Flink assign subtask to the file group id stored in the partitioned RLI. New records are
 * added to the cache after bucket assignment, while existing records keep their original RLI routing.
 *
 * <p>Partition caches are evicted lazily: checkpoint completion only advances the safe eviction
 * watermark, and cleanup happens when the total in-memory cache size exceeds
 * {@link FlinkOptions#INDEX_RLI_CACHE_SIZE}.
 */
@Slf4j
public class RecordLevelIndexBackend implements PartitionedIndexBackend {

  private final Configuration conf;
  private final HoodieWriteConfig writeConfig;
  private final HoodieTableMetaClient metaClient;
  private final long maxCacheSizeInBytes;
  private final BootstrapFilter bootstrapFilter;
  private HoodieTableMetadata metadataTable;

  @Getter
  private final Map<String, BucketCache> partitionBucketCaches = new LinkedHashMap<>(16, 0.75f, true);
  private long currentCheckpointId = -1L;
  private long minRetainedCheckpointId = Long.MIN_VALUE;
  private final SamplingActionExecutor cleanExecutor = new SamplingActionExecutor();

  /**
   * Creates a partitioned RLI backend with custom ownership filtering.
   *
   * <p>This constructor is used by simple bucket index, whose write ownership is determined by
   * partition and bucket file id rather than Flink record-key key groups.
   *
   * @param conf Flink write configuration
   * @param bootstrapFilter filter for deciding whether a bootstrapped RLI record belongs to this task
   */
  public RecordLevelIndexBackend(Configuration conf, BootstrapFilter bootstrapFilter) {
    this.conf = conf;
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, false, false);
    this.metaClient = StreamerUtil.createMetaClient(conf);
    this.maxCacheSizeInBytes = conf.get(FlinkOptions.INDEX_RLI_CACHE_SIZE) * 1024 * 1024;
    this.bootstrapFilter = bootstrapFilter;
    reloadMetadataTable();
  }

  @Override
  public String get(String partitionPath, String recordKey) {
    BucketCache cache = getOrBootstrapPartition(partitionPath);
    return cache.getFileGroupId(recordKey);
  }

  @Override
  public void update(String partitionPath, String recordKey, String fileId) {
    BucketCache cache = getOrBootstrapPartition(partitionPath);
    cache.putRecordKey(recordKey, fileId);
    cleanExecutor.runIfNecessary(() -> cleanIfNecessary(0L, partitionPath));
  }

  /**
   * Records the latest checkpoint id seen by this assign subtask.
   *
   * <p>New record-key mappings written after this point are tagged with the checkpoint id so lazy
   * eviction can avoid removing partition caches that may still be needed for recovery.
   *
   * @param checkpointId checkpoint id from the bucket assign operator
   */
  @Override
  public void onCheckpoint(long checkpointId) {
    this.currentCheckpointId = checkpointId;
  }

  /**
   * Advances the lazy eviction watermark after a completed checkpoint.
   *
   * <p>The watermark is derived from the minimum inflight checkpoint so caches that may still be
   * needed for recovery are retained. Metadata table state is reloaded after the coordinator has
   * committed or advanced instants for the completed checkpoint.
   *
   * @param correspondent writer coordinator correspondent used to query inflight instants
   * @param completedCheckpointId completed checkpoint id reported by Flink
   */
  @Override
  public void onCheckpointComplete(Correspondent correspondent, long completedCheckpointId) {
    Map<Long, String> inflightInstants = correspondent.requestInflightInstants();
    updateEvictableCkp(inflightInstants.keySet().stream().min(Long::compareTo).orElse(completedCheckpointId));
    metaClient.reloadActiveTimeline();
    reloadMetadataTable();
  }

  private BucketCache getOrBootstrapPartition(String partitionPath) {
    BucketCache cache = partitionBucketCaches.get(partitionPath);
    if (cache != null) {
      return cache;
    }

    cache = bootstrapPartition(partitionPath);
    partitionBucketCaches.put(partitionPath, cache);
    return cache;
  }

  private BucketCache bootstrapPartition(String partitionPath) {
    BucketCache cache = createBucketCache(partitionPath);
    if (!metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX)) {
      log.info("Record index is not available yet. Start partitioned RLI cache empty for partition {}", partitionPath);
      return cache;
    }

    try {
      Map<String, List<FileSlice>> partitionedFileGroups =
          metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX);
      List<FileSlice> fileSlices = partitionedFileGroups.get(partitionPath);
      if (fileSlices == null || fileSlices.isEmpty()) {
        return cache;
      }
      HoodiePairData<String, HoodieRecordGlobalLocation> locations =
          metadataTable.readRecordIndexLocations(fileSlicesToFilter -> fileSlices);
      final AtomicLong totalCnt = new AtomicLong();
      locations.forEach(locationPair -> {
        String recordKey = locationPair.getLeft();
        String fileId = locationPair.getRight().getFileId();
        if (bootstrapFilter.shouldLoad(partitionPath, recordKey, fileId)) {
          cache.bootstrapRecordKey(recordKey, fileId);
        }
        totalCnt.incrementAndGet();
      });
      log.info("Bootstrapped partitioned RLI cache for partition {} with {} owned records from total {} RLI records.",
          partitionPath, cache.size(), totalCnt.get());
      return cache;
    } catch (Exception e) {
      cache.close();
      throw new HoodieException("Failed to bootstrap partitioned RLI cache for partition " + partitionPath, e);
    }
  }

  private BucketCache createBucketCache(String partitionPath) {
    long inferredCacheSize = inferMemorySizeForCache();
    cleanIfNecessary(inferredCacheSize, partitionPath);
    return newBucketCache(createSpillableMap(partitionPath, inferredCacheSize), Long.MIN_VALUE);
  }

  private ExternalSpillableMap<String, Integer> createSpillableMap(String partitionPath, long inferredCacheSize) {
    // preserve some memory for fileId dict, assuming the average file group number as Short.MAX_VALUE, then the size will be about 1MB.
    long maxInMemorySizeInBytes = Math.max(inferredCacheSize - BucketCache.FILE_ID_DICT_ENTRY_SIZE * Short.MAX_VALUE, 1);
    try {
      return new ExternalSpillableMap<>(
          maxInMemorySizeInBytes,
          writeConfig.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(),
          new DefaultSizeEstimator<>(),
          ExternalSpillableMap.DiskMapType.ROCKS_DB,
          new DefaultSerializer<>(),
          writeConfig.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED),
          "PartitionedRLICache-" + partitionPath);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create partitioned RLI cache for partition " + partitionPath, e);
    }
  }

  private void reloadMetadataTable() {
    closeMetadataTable();
    this.metadataTable = metaClient.getTableFormat().getMetadataFactory().create(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH));
  }

  private void updateEvictableCkp(long checkpointId) {
    ValidationUtils.checkArgument(checkpointId >= minRetainedCheckpointId,
        String.format("The minimum retained checkpoint id should be increased, previous: %s, received: %s",
            minRetainedCheckpointId, checkpointId));
    minRetainedCheckpointId = checkpointId;
  }

  private long inferMemorySizeForCache() {
    int concurrentPartitionsNum = conf.get(FlinkOptions.INDEX_RLI_CACHE_CONCURRENT_PARTITIONS_NUM);
    if (partitionBucketCaches.isEmpty()) {
      return maxCacheSizeInBytes / concurrentPartitionsNum;
    }
    long averageMemorySize = getCurrentHeapSize() / partitionBucketCaches.size();
    if (averageMemorySize <= 0) {
      return maxCacheSizeInBytes / concurrentPartitionsNum;
    }
    return averageMemorySize;
  }

  private long getCurrentHeapSize() {
    return partitionBucketCaches.values().stream()
        .map(BucketCache::getHeapSize)
        .reduce(Long::sum)
        .orElse(0L);
  }

  @VisibleForTesting
  void cleanIfNecessary(long nextCacheSize, String protectedPartitionPath) {
    while (getCurrentHeapSize() + nextCacheSize > maxCacheSizeInBytes) {
      boolean cleaned = false;
      Iterator<Map.Entry<String, BucketCache>> iterator = partitionBucketCaches.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, BucketCache> entry = iterator.next();
        if (entry.getKey().equals(protectedPartitionPath)) {
          continue;
        }
        BucketCache cache = entry.getValue();
        if (cache.lastUpdatedCheckpoint < minRetainedCheckpointId) {
          cache.close();
          iterator.remove();
          cleaned = true;
          log.info("Evict partitioned RLI cache for partition {}", entry.getKey());
          break;
        }
      }
      if (!cleaned) {
        // All remaining partition caches are either protected or too recent to evict safely.
        // Returning avoids retrying the same scan without making progress.
        return;
      }
    }
  }

  @Override
  public void close() throws IOException {
    partitionBucketCaches.values().forEach(BucketCache::close);
    partitionBucketCaches.clear();
    closeMetadataTable();
  }

  private void closeMetadataTable() {
    if (metadataTable != null) {
      try {
        metadataTable.close();
      } catch (Exception e) {
        throw new HoodieException("Failed to close metadata table", e);
      } finally {
        metadataTable = null;
      }
    }
  }

  /**
   * Creates a bucket cache with an injected spillable map for tests.
   *
   * @param recordKeyToFileGroupIdCode encoded record-key to file-group-id mapping
   * @param lastUpdatedCheckpoint checkpoint id of the latest non-bootstrap update
   * @return bucket cache for one data partition
   */
  @VisibleForTesting
  public BucketCache newBucketCache(
      ExternalSpillableMap<String, Integer> recordKeyToFileGroupIdCode,
      long lastUpdatedCheckpoint) {
    return new BucketCache(recordKeyToFileGroupIdCode, lastUpdatedCheckpoint);
  }

  @FunctionalInterface
  public interface BootstrapFilter {
    boolean shouldLoad(String partitionPath, String recordKey, String fileId);
  }

  /**
   * Bucket Cache of one data table partition.
   *
   * <p>The spillable map stores {@code recordKey -> fileGroupIdCode}. File group ids are dictionary
   * encoded inside the cache so hot partitions do not repeat UUID-style file group id strings for
   * every record key entry.
   */
  public class BucketCache implements Closeable {
    @Getter
    private final ExternalSpillableMap<String, Integer> recordKeyToFileGroupIdCode;
    // Keep file group ids in a partition-local dictionary so the spillable record map stores
    // compact integer codes instead of repeating long UUID-style file group id strings.
    private final Map<String, Integer> fileGroupIdToDictId;
    private final List<String> dictIdToFileGroupId;
    private long lastUpdatedCheckpoint;
    // File group ids generated by bucket assign are UUID-style 36-character strings.
    // Each dictionary entry keeps one file group id and one 4-byte integer code.
    private static final long FILE_ID_DICT_ENTRY_SIZE = 36L + Integer.BYTES;

    BucketCache(ExternalSpillableMap<String, Integer> recordKeyToFileGroupIdCode, long lastUpdatedCheckpoint) {
      this.recordKeyToFileGroupIdCode = recordKeyToFileGroupIdCode;
      this.fileGroupIdToDictId = new HashMap<>();
      this.dictIdToFileGroupId = new ArrayList<>();
      this.lastUpdatedCheckpoint = lastUpdatedCheckpoint;
    }

    /**
     * Returns the decoded file group id for the record key, or {@code null} when the key is unknown.
     */
    String getFileGroupId(String recordKey) {
      Integer fileGroupIdCode = this.recordKeyToFileGroupIdCode.get(recordKey);
      return fileGroupIdCode == null ? null : dictIdToFileGroupId.get(fileGroupIdCode);
    }

    /**
     * Returns the number of cached record-key mappings in this partition.
     */
    int size() {
      return this.recordKeyToFileGroupIdCode.size();
    }

    private long getHeapSize() {
      return this.recordKeyToFileGroupIdCode.getCurrentInMemoryMapSize() + getFileGroupIdDictHeapSize();
    }

    /**
     * Adds a record key mapping from incoming new records.
     */
    void putRecordKey(String recordKey, String fileGroupId) {
      this.recordKeyToFileGroupIdCode.put(recordKey, getOrCreateFileGroupIdCode(fileGroupId));
      this.lastUpdatedCheckpoint = currentCheckpointId;
    }

    /**
     * Bootstrap a record key mapping from the RLI data.
     */
    void bootstrapRecordKey(String recordKey, String fileGroupId) {
      this.recordKeyToFileGroupIdCode.put(recordKey, getOrCreateFileGroupIdCode(fileGroupId));
    }

    private int getOrCreateFileGroupIdCode(String fileGroupId) {
      // Encode each unique file group id once, and reuse the code for all record keys routed to it.
      Integer existingCode = fileGroupIdToDictId.get(fileGroupId);
      if (existingCode != null) {
        return existingCode;
      }

      int newCode = dictIdToFileGroupId.size();
      fileGroupIdToDictId.put(fileGroupId, newCode);
      dictIdToFileGroupId.add(fileGroupId);
      return newCode;
    }

    private long getFileGroupIdDictHeapSize() {
      return dictIdToFileGroupId.size() * FILE_ID_DICT_ENTRY_SIZE;
    }

    @Override
    public void close() {
      this.recordKeyToFileGroupIdCode.close();
      this.fileGroupIdToDictId.clear();
      this.dictIdToFileGroupId.clear();
    }
  }
}
