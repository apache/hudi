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

import java.io.IOException;

/**
 * HFile reader implementation with integrated caching functionality. This extends BaseHFileReaderImpl and overrides the block instantiation method to add caching capabilities.
 * <p>
 * Uses a shared static cache across all instances to maximize cache hits when multiple readers access the same file.
 */
public class CachingHFileReaderImpl extends BaseHFileReaderImpl {

  private static volatile HFileBlockCache GLOBAL_BLOCK_CACHE;
  private static final Object CACHE_LOCK = new Object();

  private final String filePath;

  public CachingHFileReaderImpl(SeekableDataInputStream stream, long fileSize, String filePath) {
    this(stream, fileSize, filePath, HFileReaderConfig.DEFAULT_BLOCK_CACHE_SIZE);
  }

  public CachingHFileReaderImpl(SeekableDataInputStream stream, long fileSize, String filePath, int cacheSize) {
    this(stream, fileSize, filePath, new HFileReaderConfig(cacheSize));
  }

  public CachingHFileReaderImpl(SeekableDataInputStream stream, long fileSize, String filePath, HFileReaderConfig config) {
    super(stream, fileSize);
    this.filePath = filePath;
    // Initialize global cache with provided config (ignored if already initialized)
    getGlobalCache(config);
  }

  /**
   * Gets or creates the global cache shared by all CachingHFileReaderImpl instances.
   * Thread-safe singleton pattern with double-checked locking.
   */
  private static HFileBlockCache getGlobalCache(HFileReaderConfig config) {
    HFileBlockCache result = GLOBAL_BLOCK_CACHE;
    if (result == null) {
      synchronized (CACHE_LOCK) {
        result = GLOBAL_BLOCK_CACHE;
        if (result == null) {
          GLOBAL_BLOCK_CACHE = result = new HFileBlockCache(
              config.getBlockCacheSize(),
              config.getCacheTtlMinutes(),
              java.util.concurrent.TimeUnit.MINUTES);
        }
      }
    }
    return result;
  }

  @Override
  public HFileDataBlock instantiateHFileDataBlock(BlockIndexEntry blockToRead) throws IOException {
    HFileBlockCache.BlockCacheKey cacheKey = new HFileBlockCache.BlockCacheKey(
        filePath, blockToRead.getOffset(), blockToRead.getSize());

    try {
      HFileBlock block = GLOBAL_BLOCK_CACHE.getOrCompute(cacheKey, () -> super.instantiateHFileDataBlock(blockToRead));
      return (HFileDataBlock) block;
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new IOException("Failed to load HFile block", e);
      }
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
   * Clears the global cache. Should only be used for testing.
   */
  public static void resetGlobalCache() {
    synchronized (CACHE_LOCK) {
      if (GLOBAL_BLOCK_CACHE != null) {
        GLOBAL_BLOCK_CACHE.clear();
        GLOBAL_BLOCK_CACHE = null;
      }
    }
  }
}