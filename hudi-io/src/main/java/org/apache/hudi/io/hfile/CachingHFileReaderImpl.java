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

import org.apache.hudi.io.SeekableDataInputStream;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * HFile reader implementation with integrated caching functionality. This extends BaseHFileReaderImpl and overrides the block instantiation method to add caching capabilities.
 * <p>
 * Uses a shared static cache across all instances to maximize cache hits when multiple readers access the same file.
 */
@Slf4j
public class CachingHFileReaderImpl extends HFileReaderImpl {

  private static volatile HFileBlockCache GLOBAL_BLOCK_CACHE;
  // Store first config values to check against cache config
  private static volatile Integer INITIAL_CACHE_SIZE;
  private static volatile Integer INITIAL_CACHE_TTL;
  private static volatile Long INITIAL_CACHE_MAX_WEIGHT_BYTES;
  private static final Object CACHE_LOCK = new Object();

  // Per-thread physical (cache-miss) read attribution: [blocksLoaded, bytesLoaded].
  // Scanners snapshot this around a unit of work to attribute cold storage reads per-task.
  private static final ThreadLocal<long[]> PHYSICAL_READ = ThreadLocal.withInitial(() -> new long[2]);

  private final String filePath;

  public CachingHFileReaderImpl(SeekableDataInputStream stream, long fileSize, String filePath, int cacheSize, int cacheTtlMinutes) {
    this(stream, fileSize, filePath, cacheSize, 0L, cacheTtlMinutes);
  }

  public CachingHFileReaderImpl(SeekableDataInputStream stream, long fileSize, String filePath,
                                int cacheSize, long cacheMaxWeightBytes, int cacheTtlMinutes) {
    super(stream, fileSize);
    // A rewritten physical file must not inherit blocks cached for an older file at the same path.
    this.filePath = filePath + "#len=" + fileSize;
    getGlobalCache(cacheSize, cacheMaxWeightBytes, cacheTtlMinutes);
  }

  /**
   * Gets or creates the global cache shared by all CachingHFileReaderImpl instances.
   * Thread-safe singleton pattern with double-checked locking.
   */
  private static HFileBlockCache getGlobalCache(
      int cacheSize, long cacheMaxWeightBytes, int cacheTtlMinutes) {
    if (GLOBAL_BLOCK_CACHE == null) {
      synchronized (CACHE_LOCK) {
        if (GLOBAL_BLOCK_CACHE == null) {
          log.info("Initializing global HFileBlockCache with size: {} blocks, maxWeightBytes: {}, TTL: {} minutes.",
              cacheSize, cacheMaxWeightBytes, cacheTtlMinutes);
          INITIAL_CACHE_SIZE = cacheSize;
          INITIAL_CACHE_TTL = cacheTtlMinutes;
          INITIAL_CACHE_MAX_WEIGHT_BYTES = cacheMaxWeightBytes;
          GLOBAL_BLOCK_CACHE = new HFileBlockCache(
              cacheSize, cacheMaxWeightBytes, cacheTtlMinutes, TimeUnit.MINUTES);
        } else if (!INITIAL_CACHE_SIZE.equals(cacheSize)
            || !INITIAL_CACHE_TTL.equals(cacheTtlMinutes)
            || !INITIAL_CACHE_MAX_WEIGHT_BYTES.equals(cacheMaxWeightBytes)) {
          log.warn("HFile block cache is already initialized. The provided configuration is being ignored. "
                  + "Existing config: [Size: {}, MaxWeightBytes: {}, TTL: {} mins], "
                  + "Ignored config: [Size: {}, MaxWeightBytes: {}, TTL: {} mins].",
              INITIAL_CACHE_SIZE, INITIAL_CACHE_MAX_WEIGHT_BYTES, INITIAL_CACHE_TTL,
              cacheSize, cacheMaxWeightBytes, cacheTtlMinutes);
        }
      }
    }
    return GLOBAL_BLOCK_CACHE;
  }

  @Override
  public HFileDataBlock instantiateHFileDataBlock(BlockIndexEntry blockToRead) throws IOException {
    HFileBlockCache.BlockCacheKey cacheKey = new HFileBlockCache.BlockCacheKey(
        filePath, blockToRead.getOffset(), blockToRead.getSize());

    try {
      // The supplier runs only on a cache miss, so incrementing here attributes
      // exactly the physical (cold) block reads to the calling thread.
      HFileBlock block = GLOBAL_BLOCK_CACHE.getOrCompute(cacheKey, () -> {
        long[] pr = PHYSICAL_READ.get();
        pr[0] += 1;
        pr[1] += blockToRead.getSize();
        return super.instantiateHFileDataBlock(blockToRead);
      });
      return (HFileDataBlock) block;
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to load HFile block", e);
    }
  }

  @Override
  protected HFileBlock instantiateHFileIndexBlock(
      BlockIndexEntry indexEntry, HFileBlockType blockType) throws IOException {
    HFileBlockCache.BlockCacheKey cacheKey = new HFileBlockCache.BlockCacheKey(
        filePath, indexEntry.getOffset(), indexEntry.getSize());
    try {
      return GLOBAL_BLOCK_CACHE.getOrCompute(cacheKey, () -> {
        long[] physicalRead = PHYSICAL_READ.get();
        physicalRead[0] += 1;
        physicalRead[1] += indexEntry.getSize();
        return super.instantiateHFileIndexBlock(indexEntry, blockType);
      });
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to load HFile index block", e);
    }
  }

  @Override
  public void close() throws IOException {
    // NOTE: Do not clear the shared cache when closing individual readers
    // The cache is shared across all instances
    super.close();
  }

  /**
   * Gets current cache size from the global cache.
   *
   * @return number of cached blocks
   */
  public long getCacheSize() {
    return GLOBAL_BLOCK_CACHE != null ? GLOBAL_BLOCK_CACHE.size() : 0;
  }

  /**
   * Clears the global block cache.
   */
  public void clearCache() {
    if (GLOBAL_BLOCK_CACHE != null) {
      GLOBAL_BLOCK_CACHE.clear();
    }
  }

  /**
   * Gets cache statistics for monitoring optimization effectiveness.
   *
   * @return string representation of cache statistics
   */
  public String getCacheStats() {
    return "HFileReader Cache Stats - Size: " + (GLOBAL_BLOCK_CACHE != null ? GLOBAL_BLOCK_CACHE.size() : 0);
  }

  /**
   * Returns a copy of the current thread's physical-read counters [blocksLoaded, bytesLoaded].
   * Take two snapshots around a unit of work to attribute cold storage reads to that unit.
   *
   * @return a copy of the current thread's [blocksLoaded, bytesLoaded].
   */
  public static long[] physicalReadSnapshot() {
    long[] a = PHYSICAL_READ.get();
    return new long[] {a[0], a[1]};
  }

  /**
   * Global cache snapshot, or a sentinel when caching is inactive. Static so scanners
   * can log it without holding a reader reference.
   *
   * @return formatted statistics string.
   */
  public static String globalCacheStatsString() {
    return GLOBAL_BLOCK_CACHE != null ? GLOBAL_BLOCK_CACHE.statsString() : "cache=inactive";
  }

  /**
   * Clears the global cache. Should only be used for testing.
   */
  public static void resetGlobalCache() {
    synchronized (CACHE_LOCK) {
      if (GLOBAL_BLOCK_CACHE != null) {
        GLOBAL_BLOCK_CACHE.clear();
        GLOBAL_BLOCK_CACHE = null;
      }
      INITIAL_CACHE_SIZE = null;
      INITIAL_CACHE_TTL = null;
      INITIAL_CACHE_MAX_WEIGHT_BYTES = null;
    }
  }
}
