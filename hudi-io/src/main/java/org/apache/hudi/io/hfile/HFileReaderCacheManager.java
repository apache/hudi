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

package org.apache.hudi.io.hfile;

import org.apache.hudi.common.util.Option;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Central manager for all shared caches used by {@link CachingHFileReaderImpl}.
 *
 * <p>The manager intentionally keeps block-level cache and load-on-open metadata cache separate,
 * because they have different keys, values, and reuse patterns:
 * <ul>
 *   <li>{@link HFileBlockCache} stores independently addressable blocks keyed by file, offset, and size.</li>
 *   <li>Load-on-open cache stores file-scoped metadata bundles keyed only by file identity.</li>
 * </ul>
 */
@Slf4j
public final class HFileReaderCacheManager {

  private static volatile HFileReaderCacheManager INSTANCE;
  private static final Object INSTANCE_LOCK = new Object();

  private final HFileBlockCache blockCache;
  private final Cache<String, LoadOnOpenBlocks> loadOnOpenDataCache;
  private final int blockCacheSize;
  private final long cacheMaxWeightBytes;
  private final int loadOnOpenDataCacheSize;
  private final int cacheTtlMinutes;

  private HFileReaderCacheManager(int blockCacheSize,
                                  long cacheMaxWeightBytes,
                                  int loadOnOpenDataCacheSize,
                                  int cacheTtlMinutes) {
    this.blockCacheSize = blockCacheSize;
    this.cacheMaxWeightBytes = cacheMaxWeightBytes;
    this.loadOnOpenDataCacheSize = loadOnOpenDataCacheSize;
    this.cacheTtlMinutes = cacheTtlMinutes;
    log.info("Initializing global HFileBlockCache with size: {} blocks, maxWeightBytes: {}, TTL: {} minutes.",
        blockCacheSize, cacheMaxWeightBytes, cacheTtlMinutes);
    log.info("Initializing global load-on-open data cache with size: {} files, maxWeightBytes: {}, TTL: {} minutes.",
        loadOnOpenDataCacheSize, cacheMaxWeightBytes, cacheTtlMinutes);
    this.blockCache = new HFileBlockCache(blockCacheSize, cacheMaxWeightBytes, cacheTtlMinutes, TimeUnit.MINUTES);
    Caffeine<Object, Object> builder = Caffeine.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(cacheTtlMinutes))
        .recordStats();
    if (cacheMaxWeightBytes > 0L) {
      this.loadOnOpenDataCache = builder.maximumWeight(cacheMaxWeightBytes)
          .weigher((String key, LoadOnOpenBlocks blocks) -> (int) Math.min(Integer.MAX_VALUE, Math.max(1L, blocks.heapSize())))
          .build();
    } else {
      this.loadOnOpenDataCache = builder.maximumSize(loadOnOpenDataCacheSize).build();
    }
  }

  /**
   * Returns the shared manager, creating it with the given configuration on first use. The
   * configuration of later calls is ignored; a positive {@code cacheMaxWeightBytes} bounds each
   * of the two caches by retained bytes instead of by entry count.
   */
  public static HFileReaderCacheManager getInstance(int blockCacheSize,
                                                    long cacheMaxWeightBytes,
                                                    int loadOnOpenDataCacheSize,
                                                    int cacheTtlMinutes) {
    if (INSTANCE == null) {
      synchronized (INSTANCE_LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new HFileReaderCacheManager(blockCacheSize, cacheMaxWeightBytes, loadOnOpenDataCacheSize, cacheTtlMinutes);
        } else {
          INSTANCE.warnIfConfigIgnored(blockCacheSize, cacheMaxWeightBytes, loadOnOpenDataCacheSize, cacheTtlMinutes);
        }
      }
    } else {
      INSTANCE.warnIfConfigIgnored(blockCacheSize, cacheMaxWeightBytes, loadOnOpenDataCacheSize, cacheTtlMinutes);
    }
    return INSTANCE;
  }

  /**
   * Returns the shared manager, or empty if no caching reader has created it yet.
   */
  public static Option<HFileReaderCacheManager> getInstanceIfInitialized() {
    return Option.ofNullable(INSTANCE);
  }

  /**
   * Returns the shared caches' statistics, or a sentinel when no caching reader has been created yet.
   */
  public static String globalStatsString() {
    HFileReaderCacheManager instance = INSTANCE;
    return instance != null ? instance.getStats() : "cache=inactive";
  }

  public static void reset() {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE != null) {
        INSTANCE.clear();
        INSTANCE = null;
      }
    }
  }

  public <T extends HFileBlock> T getOrComputeBlock(String filePath,
                                                    long offset,
                                                    int size,
                                                    Class<T> blockClass,
                                                    Callable<HFileBlock> loader) throws IOException {
    HFileBlockCache.BlockCacheKey cacheKey = new HFileBlockCache.BlockCacheKey(filePath, offset, size);
    HFileBlock block = blockCache.getOrCompute(cacheKey, loader);
    return blockClass.cast(block);
  }

  public LoadOnOpenBlocks getOrComputeLoadOnOpenData(String filePath,
                                                     Callable<LoadOnOpenBlocks> loader) throws IOException {
    return HFileBlockCache.getOrLoad(loadOnOpenDataCache, filePath, loader);
  }

  public boolean containsLoadOnOpenData(String filePath) {
    return loadOnOpenDataCache.getIfPresent(filePath) != null;
  }

  public void invalidateLoadOnOpenData(String filePath) {
    loadOnOpenDataCache.invalidate(filePath);
  }

  public void clear() {
    blockCache.clear();
    loadOnOpenDataCache.invalidateAll();
  }

  public long getBlockCacheSize() {
    return blockCache.size();
  }

  public long getLoadOnOpenDataCacheSize() {
    return loadOnOpenDataCache.estimatedSize();
  }

  public String getStats() {
    return "HFileReader Cache Stats - Block Cache [" + blockCache.statsString()
        + "], Load On Open Data Cache [" + HFileBlockCache.statsString("files", loadOnOpenDataCache) + "]";
  }

  /**
   * Forces cache maintenance operations like eviction on both caches.
   * This is useful for testing to ensure consistent behavior.
   */
  public void cleanUp() {
    blockCache.cleanUp();
    loadOnOpenDataCache.cleanUp();
  }

  private void warnIfConfigIgnored(int requestedBlockCacheSize,
                                   long requestedCacheMaxWeightBytes,
                                   int requestedLoadOnOpenDataCacheSize,
                                   int requestedCacheTtlMinutes) {
    if (blockCacheSize != requestedBlockCacheSize
        || cacheMaxWeightBytes != requestedCacheMaxWeightBytes
        || cacheTtlMinutes != requestedCacheTtlMinutes) {
      log.warn("HFile block cache is already initialized. The provided configuration is being ignored. "
              + "Existing config: [Size: {}, MaxWeightBytes: {}, TTL: {} mins], "
              + "Ignored config: [Size: {}, MaxWeightBytes: {}, TTL: {} mins].",
          blockCacheSize, cacheMaxWeightBytes, cacheTtlMinutes,
          requestedBlockCacheSize, requestedCacheMaxWeightBytes, requestedCacheTtlMinutes);
    }
    if (loadOnOpenDataCacheSize != requestedLoadOnOpenDataCacheSize
        || cacheMaxWeightBytes != requestedCacheMaxWeightBytes
        || cacheTtlMinutes != requestedCacheTtlMinutes) {
      log.warn("HFile load-on-open data cache is already initialized. The provided configuration is being ignored. "
              + "Existing config: [Size: {}, MaxWeightBytes: {}, TTL: {} mins], "
              + "Ignored config: [Size: {}, MaxWeightBytes: {}, TTL: {} mins].",
          loadOnOpenDataCacheSize, cacheMaxWeightBytes, cacheTtlMinutes,
          requestedLoadOnOpenDataCacheSize, requestedCacheMaxWeightBytes, requestedCacheTtlMinutes);
    }
  }

  /**
   * Materialized representation of the file's load-on-open region.
   *
   * <p>These objects are cached as a whole because they are read from a single contiguous region
   * and are all required to initialize reader metadata.
   */
  public static final class LoadOnOpenBlocks {
    final HFileTrailer trailer;
    final HFileRootIndexBlock rootDataIndexBlock;
    final HFileRootIndexBlock metaRootIndexBlock;
    final HFileFileInfoBlock fileInfoBlock;

    public LoadOnOpenBlocks(HFileTrailer trailer,
                            HFileRootIndexBlock rootDataIndexBlock,
                            HFileRootIndexBlock metaRootIndexBlock,
                            HFileFileInfoBlock fileInfoBlock) {
      this.trailer = trailer;
      this.rootDataIndexBlock = rootDataIndexBlock;
      this.metaRootIndexBlock = metaRootIndexBlock;
      this.fileInfoBlock = fileInfoBlock;
    }

    /**
     * Returns the bytes this entry keeps alive for byte-weighted caching. The three blocks are
     * slices of the one array read for the whole load-on-open region, so each distinct array
     * is counted once.
     */
    long heapSize() {
      Set<byte[]> buffers = Collections.newSetFromMap(new IdentityHashMap<>());
      for (HFileBlock block : Arrays.asList(rootDataIndexBlock, metaRootIndexBlock, fileInfoBlock)) {
        buffers.addAll(block.retainedBuffers());
      }
      long size = 0L;
      for (byte[] buffer : buffers) {
        size += buffer.length;
      }
      return size;
    }
  }
}
