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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Central manager for all shared caches used by {@link CachingHFileReaderImpl}.
 *
 * <p>The manager intentionally keeps the block cache and the trailer and "load-on-open" cache
 * separate, because they have different keys, values, and reuse patterns:
 * <ul>
 *   <li>{@link HFileBlockCache} stores independently addressable blocks keyed by file, offset, and size.</li>
 *   <li>The trailer and "load-on-open" cache stores one {@link HFileTrailerAndLoadOnOpenBlocks} per
 *   file, keyed by file identity only.</li>
 * </ul>
 *
 * <p>One manager is shared by all readers in the JVM. Its configuration comes from the first
 * reader that asks for it with an explicit configuration; readers running on default configuration
 * neither pin nor change it, so a default-configured reader created first does not stop a later,
 * explicitly configured one from taking effect.
 */
@Slf4j
public final class HFileReaderCacheManager {

  private static volatile HFileReaderCacheManager INSTANCE;
  private static final Object INSTANCE_LOCK = new Object();

  private final HFileBlockCache blockCache;
  private final Cache<String, HFileTrailerAndLoadOnOpenBlocks> trailerAndLoadOnOpenCache;
  private final int blockCacheSize;
  private final long cacheMaxWeightBytes;
  private final int loadOnOpenCacheSize;
  private final int cacheTtlMinutes;
  private volatile boolean explicitlyConfigured;
  private final AtomicBoolean ignoredConfigWarned = new AtomicBoolean();

  private HFileReaderCacheManager(int blockCacheSize,
                                  long cacheMaxWeightBytes,
                                  int loadOnOpenCacheSize,
                                  int cacheTtlMinutes,
                                  boolean explicitlyConfigured) {
    this.blockCacheSize = blockCacheSize;
    this.cacheMaxWeightBytes = cacheMaxWeightBytes;
    this.loadOnOpenCacheSize = loadOnOpenCacheSize;
    this.cacheTtlMinutes = cacheTtlMinutes;
    this.explicitlyConfigured = explicitlyConfigured;
    log.info("Initializing global HFile caches: block cache size: {} blocks, trailer and load-on-open cache size: {} files, "
            + "maxWeightBytes: {}, TTL: {} minutes, explicitly configured: {}.",
        blockCacheSize, loadOnOpenCacheSize, cacheMaxWeightBytes, cacheTtlMinutes, explicitlyConfigured);
    this.blockCache = new HFileBlockCache(blockCacheSize, cacheMaxWeightBytes, cacheTtlMinutes, TimeUnit.MINUTES);
    Caffeine<Object, Object> builder = Caffeine.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(cacheTtlMinutes))
        .recordStats();
    if (cacheMaxWeightBytes > 0L) {
      this.trailerAndLoadOnOpenCache = builder.maximumWeight(cacheMaxWeightBytes)
          .weigher((String key, HFileTrailerAndLoadOnOpenBlocks blocks) -> (int) Math.min(Integer.MAX_VALUE, Math.max(1L, blocks.heapSize())))
          .build();
    } else {
      this.trailerAndLoadOnOpenCache = builder.maximumSize(loadOnOpenCacheSize).build();
    }
  }

  /**
   * Returns the shared manager, creating it on first use. A positive {@code cacheMaxWeightBytes}
   * bounds each of the two caches by retained bytes instead of by entry count.
   *
   * <p>An explicitly configured request replaces a manager that was created on default
   * configuration, dropping its cached entries. Once the manager is explicitly configured, a
   * differing explicit configuration is ignored with a single warning.
   */
  public static HFileReaderCacheManager getInstance(int blockCacheSize,
                                                    long cacheMaxWeightBytes,
                                                    int loadOnOpenCacheSize,
                                                    int cacheTtlMinutes,
                                                    boolean explicitlyConfigured) {
    HFileReaderCacheManager instance = INSTANCE;
    if (instance != null && (!explicitlyConfigured || instance.explicitlyConfigured)) {
      if (explicitlyConfigured && !instance.hasConfig(blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes)) {
        instance.warnConfigIgnoredOnce(blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes);
      }
      return instance;
    }
    synchronized (INSTANCE_LOCK) {
      instance = INSTANCE;
      if (instance == null) {
        INSTANCE = instance = new HFileReaderCacheManager(
            blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes, explicitlyConfigured);
      } else if (explicitlyConfigured && !instance.explicitlyConfigured) {
        if (instance.hasConfig(blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes)) {
          instance.explicitlyConfigured = true;
        } else {
          log.info("Replacing the global HFile caches created on default configuration with explicitly configured ones.");
          INSTANCE = instance = new HFileReaderCacheManager(
              blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes, true);
        }
      } else if (explicitlyConfigured
          && !instance.hasConfig(blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes)) {
        instance.warnConfigIgnoredOnce(blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes);
      }
      return instance;
    }
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

  public <T extends HFileBlock> T getOrComputeBlock(String fileIdentity,
                                                    long offset,
                                                    int size,
                                                    Class<T> blockClass,
                                                    Callable<HFileBlock> loader) throws IOException {
    HFileBlockCache.BlockCacheKey cacheKey = new HFileBlockCache.BlockCacheKey(fileIdentity, offset, size);
    HFileBlock block = blockCache.getOrCompute(cacheKey, loader);
    return blockClass.cast(block);
  }

  public HFileTrailerAndLoadOnOpenBlocks getOrLoadTrailerAndLoadOnOpenBlocks(String fileIdentity,
                                                                           Callable<HFileTrailerAndLoadOnOpenBlocks> loader)
      throws IOException {
    return HFileBlockCache.getOrLoad(trailerAndLoadOnOpenCache, fileIdentity, loader);
  }

  public boolean containsTrailerAndLoadOnOpenBlocks(String fileIdentity) {
    return trailerAndLoadOnOpenCache.getIfPresent(fileIdentity) != null;
  }

  public void invalidateTrailerAndLoadOnOpenBlocks(String fileIdentity) {
    trailerAndLoadOnOpenCache.invalidate(fileIdentity);
  }

  public void clear() {
    blockCache.clear();
    trailerAndLoadOnOpenCache.invalidateAll();
  }

  public long getBlockCacheSize() {
    return blockCache.size();
  }

  public long getTrailerAndLoadOnOpenCacheSize() {
    return trailerAndLoadOnOpenCache.estimatedSize();
  }

  public boolean isExplicitlyConfigured() {
    return explicitlyConfigured;
  }

  public String getStats() {
    return "HFileReader Cache Stats - Block Cache [" + blockCache.statsString()
        + "], Trailer And Load-On-Open Cache [" + HFileBlockCache.statsString("files", trailerAndLoadOnOpenCache) + "]";
  }

  /**
   * Forces cache maintenance operations like eviction on both caches.
   * This is useful for testing to ensure consistent behavior.
   */
  public void cleanUp() {
    blockCache.cleanUp();
    trailerAndLoadOnOpenCache.cleanUp();
  }

  private boolean hasConfig(int blockCacheSize, long cacheMaxWeightBytes, int loadOnOpenCacheSize, int cacheTtlMinutes) {
    return this.blockCacheSize == blockCacheSize
        && this.cacheMaxWeightBytes == cacheMaxWeightBytes
        && this.loadOnOpenCacheSize == loadOnOpenCacheSize
        && this.cacheTtlMinutes == cacheTtlMinutes;
  }

  private void warnConfigIgnoredOnce(int requestedBlockCacheSize,
                                     long requestedCacheMaxWeightBytes,
                                     int requestedLoadOnOpenCacheSize,
                                     int requestedCacheTtlMinutes) {
    if (ignoredConfigWarned.compareAndSet(false, true)) {
      log.warn("The global HFile caches are already configured; a different configuration is ignored. "
              + "Existing config: [blockCacheSize: {}, loadOnOpenCacheSize: {}, maxWeightBytes: {}, TTL: {} mins], "
              + "ignored config: [blockCacheSize: {}, loadOnOpenCacheSize: {}, maxWeightBytes: {}, TTL: {} mins].",
          blockCacheSize, loadOnOpenCacheSize, cacheMaxWeightBytes, cacheTtlMinutes,
          requestedBlockCacheSize, requestedLoadOnOpenCacheSize, requestedCacheMaxWeightBytes, requestedCacheTtlMinutes);
    }
  }
}
