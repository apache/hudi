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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Least Frequently Used (LFU) cache for HFile blocks to improve read performance by avoiding repeated block reads.
 * Uses Caffeine cache with configurable size and TTL. Thread-safe for concurrent access.
 */
public class HFileBlockCache {

  private final Cache<BlockCacheKey, HFileBlock> cache;

  public HFileBlockCache(int maxCacheSize) {
    this(maxCacheSize, 30, TimeUnit.MINUTES);
  }

  public HFileBlockCache(int maxCacheSize, long expireAfterWrite, TimeUnit timeUnit) {
    this.cache = Caffeine.newBuilder()
        .maximumSize(maxCacheSize)
        .expireAfterWrite(Duration.ofMillis(timeUnit.toMillis(expireAfterWrite)))
        .build();
  }

  /**
   * Gets a block from cache.
   *
   * @param key the cache key
   * @return cached block or null if not found
   */
  public HFileBlock getBlock(BlockCacheKey key) {
    return cache.getIfPresent(key);
  }

  /**
   * Puts a block into cache.
   *
   * @param key   the cache key
   * @param block the block to cache
   */
  public void putBlock(BlockCacheKey key, HFileBlock block) {
    cache.put(key, block);
  }
  
  /**
   * Gets a block from cache, or computes and caches it if not present.
   * This method is thread-safe and prevents the "cache stampede" problem
   * where multiple threads try to load the same value simultaneously.
   *
   * @param key      the cache key
   * @param loader   callable to load the block if not in cache
   * @return cached or newly computed block
   * @throws Exception if the loader throws an exception
   */
  public HFileBlock getOrCompute(BlockCacheKey key, java.util.concurrent.Callable<HFileBlock> loader) throws Exception {
    // Caffeine uses Function instead of Callable, so we need to wrap the Callable
    return cache.get(key, (k) -> {
      try {
        return loader.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Clears all cached blocks.
   */
  public void clear() {
    cache.invalidateAll();
  }

  /**
   * Gets current cache size.
   *
   * @return number of cached blocks
   */
  public long size() {
    return cache.estimatedSize();
  }

  /**
   * Forces cache maintenance operations like eviction.
   * This is useful for testing to ensure consistent behavior.
   */
  public void cleanUp() {
    cache.cleanUp();
  }

  /**
   * Cache key for identifying blocks uniquely.
   */
  public static class BlockCacheKey {

    private final String fileIdentity;
    private final long offset;
    private final int size;

    public BlockCacheKey(long offset, int size) {
      this(null, offset, size);
    }

    public BlockCacheKey(String fileIdentity, long offset, int size) {
      this.fileIdentity = fileIdentity;
      this.offset = offset;
      this.size = size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      BlockCacheKey that = (BlockCacheKey) o;
      return offset == that.offset
          && size == that.size
          && java.util.Objects.equals(fileIdentity, that.fileIdentity);
    }

    @Override
    public int hashCode() {
      int result = fileIdentity != null ? fileIdentity.hashCode() : 0;
      result = 31 * result + (int) (offset ^ (offset >>> 32));
      result = 31 * result + size;
      return result;
    }

    @Override
    public String toString() {
      return "BlockCacheKey{"
          + "fileIdentity='" + fileIdentity + '\''
          + ", offset=" + offset
          + ", size=" + size
          + '}';
    }
  }
}