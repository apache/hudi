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

import java.util.concurrent.TimeUnit;

/**
 * Configuration class for HFile reader optimizations.
 * Controls block caching behavior and cache size limits.
 */
public class HFileReaderConfig {
  
  public static final String HFILE_BLOCK_CACHE_SIZE_PROP = "hoodie.io.hfile.cache.size";
  public static final String HFILE_BLOCK_CACHE_TTL_MINUTES_PROP = "hoodie.io.hfile.cache.ttl.minutes";
  
  public static final int DEFAULT_BLOCK_CACHE_SIZE = 100;
  public static final long DEFAULT_CACHE_TTL_MINUTES = 30;
  
  private final int blockCacheSize;
  private final long cacheTtlMinutes;
  
  public HFileReaderConfig() {
    this(DEFAULT_BLOCK_CACHE_SIZE, DEFAULT_CACHE_TTL_MINUTES);
  }
  
  public HFileReaderConfig(int blockCacheSize) {
    this(blockCacheSize, DEFAULT_CACHE_TTL_MINUTES);
  }
  
  public HFileReaderConfig(int blockCacheSize, long cacheTtlMinutes) {
    this.blockCacheSize = blockCacheSize;
    this.cacheTtlMinutes = cacheTtlMinutes;
  }
  
  /**
   * Creates configuration from system properties.
   * 
   * @return configuration with values from system properties or defaults
   */
  public static HFileReaderConfig fromProperties() {
    int cacheSize = Integer.getInteger(HFILE_BLOCK_CACHE_SIZE_PROP, DEFAULT_BLOCK_CACHE_SIZE);
    long ttlMinutes = Long.getLong(HFILE_BLOCK_CACHE_TTL_MINUTES_PROP, DEFAULT_CACHE_TTL_MINUTES);
    return new HFileReaderConfig(cacheSize, ttlMinutes);
  }
  
  /**
   * Gets the maximum number of blocks to cache.
   * 
   * @return maximum cache size
   */
  public int getBlockCacheSize() {
    return blockCacheSize;
  }
  
  /**
   * Gets the cache TTL in minutes.
   * 
   * @return cache TTL in minutes
   */
  public long getCacheTtlMinutes() {
    return cacheTtlMinutes;
  }
  
  /**
   * Gets the cache TTL with specified time unit.
   * 
   * @param timeUnit desired time unit
   * @return cache TTL in specified time unit
   */
  public long getCacheTtl(TimeUnit timeUnit) {
    return timeUnit.convert(cacheTtlMinutes, TimeUnit.MINUTES);
  }
  
  @Override
  public String toString() {
    return "HFileReaderConfig{"
        + "blockCacheSize=" + blockCacheSize
        + ", cacheTtlMinutes=" + cacheTtlMinutes
        + '}';
  }
}