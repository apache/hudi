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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
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
  private final int loadOnOpenDataCacheSize;
  private final int cacheTtlMinutes;

  private HFileReaderCacheManager(int blockCacheSize, int loadOnOpenDataCacheSize, int cacheTtlMinutes) {
    this.blockCacheSize = blockCacheSize;
    this.loadOnOpenDataCacheSize = loadOnOpenDataCacheSize;
    this.cacheTtlMinutes = cacheTtlMinutes;
    log.info("Initializing global HFileBlockCache with size: {}, TTL: {} minutes.",
        blockCacheSize, cacheTtlMinutes);
    log.info("Initializing global load-on-open data cache with size: {}, TTL: {} minutes.",
        loadOnOpenDataCacheSize, cacheTtlMinutes);
    this.blockCache = new HFileBlockCache(blockCacheSize, cacheTtlMinutes, TimeUnit.MINUTES);
    this.loadOnOpenDataCache = Caffeine.newBuilder()
        .maximumSize(loadOnOpenDataCacheSize)
        .expireAfterAccess(Duration.ofMinutes(cacheTtlMinutes))
        .build();
  }

  public static HFileReaderCacheManager getInstance(int blockCacheSize, int loadOnOpenDataCacheSize, int cacheTtlMinutes) {
    if (INSTANCE == null) {
      synchronized (INSTANCE_LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new HFileReaderCacheManager(blockCacheSize, loadOnOpenDataCacheSize, cacheTtlMinutes);
        } else {
          INSTANCE.warnIfConfigIgnored(blockCacheSize, loadOnOpenDataCacheSize, cacheTtlMinutes);
        }
      }
    } else {
      INSTANCE.warnIfConfigIgnored(blockCacheSize, loadOnOpenDataCacheSize, cacheTtlMinutes);
    }
    return INSTANCE;
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
                                                    Callable<HFileBlock> loader) {
    HFileBlockCache.BlockCacheKey cacheKey = new HFileBlockCache.BlockCacheKey(filePath, offset, size);
    HFileBlock block = blockCache.getOrCompute(cacheKey, loader);
    return blockClass.cast(block);
  }

  public LoadOnOpenBlocks getOrComputeLoadOnOpenData(String filePath,
                                                     Callable<LoadOnOpenBlocks> loader) throws IOException {
    return loadOnOpenDataCache.get(filePath, key -> {
      try {
        return loader.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
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
    return "HFileReader Cache Stats - Block Cache Size: " + getBlockCacheSize()
        + ", Load On Open Data Cache Size: " + getLoadOnOpenDataCacheSize();
  }

  private void warnIfConfigIgnored(int requestedBlockCacheSize, int requestedLoadOnOpenDataCacheSize, int requestedCacheTtlMinutes) {
    if (blockCacheSize != requestedBlockCacheSize || cacheTtlMinutes != requestedCacheTtlMinutes) {
      log.warn("HFile block cache is already initialized. The provided configuration is being ignored. "
              + "Existing config: [Size: {}, TTL: {} mins], Ignored config: [Size: {}, TTL: {} mins].",
          blockCacheSize, cacheTtlMinutes, requestedBlockCacheSize, requestedCacheTtlMinutes);
    }
    if (loadOnOpenDataCacheSize != requestedLoadOnOpenDataCacheSize || cacheTtlMinutes != requestedCacheTtlMinutes) {
      log.warn("HFile load-on-open data cache is already initialized. The provided configuration is being ignored. "
              + "Existing config: [Size: {}, TTL: {} mins], Ignored config: [Size: {}, TTL: {} mins].",
          loadOnOpenDataCacheSize, cacheTtlMinutes, requestedLoadOnOpenDataCacheSize, requestedCacheTtlMinutes);
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
  }
}
