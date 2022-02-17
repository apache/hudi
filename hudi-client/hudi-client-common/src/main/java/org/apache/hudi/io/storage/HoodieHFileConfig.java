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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

public class HoodieHFileConfig {

  public static final CellComparator HFILE_COMPARATOR = new HoodieHBaseKVComparator();
  public static final boolean PREFETCH_ON_OPEN = CacheConfig.DEFAULT_PREFETCH_ON_OPEN;
  public static final boolean CACHE_DATA_IN_L1 = HColumnDescriptor.DEFAULT_CACHE_DATA_IN_L1;
  // This is private in CacheConfig so have been copied here.
  public static final boolean DROP_BEHIND_CACHE_COMPACTION = true;

  private final Compression.Algorithm compressionAlgorithm;
  private final int blockSize;
  private final long maxFileSize;
  private final boolean prefetchBlocksOnOpen;
  private final boolean cacheDataInL1;
  private final boolean dropBehindCacheCompaction;
  private final Configuration hadoopConf;
  private final BloomFilter bloomFilter;
  private final CellComparator hfileComparator;
  private final String keyFieldName;

  public HoodieHFileConfig(Configuration hadoopConf, Compression.Algorithm compressionAlgorithm, int blockSize,
                           long maxFileSize, String keyFieldName, boolean prefetchBlocksOnOpen, boolean cacheDataInL1,
                           boolean dropBehindCacheCompaction, BloomFilter bloomFilter, CellComparator hfileComparator) {
    this.hadoopConf = hadoopConf;
    this.compressionAlgorithm = compressionAlgorithm;
    this.blockSize = blockSize;
    this.maxFileSize = maxFileSize;
    this.prefetchBlocksOnOpen = prefetchBlocksOnOpen;
    this.cacheDataInL1 = cacheDataInL1;
    this.dropBehindCacheCompaction = dropBehindCacheCompaction;
    this.bloomFilter = bloomFilter;
    this.hfileComparator = hfileComparator;
    this.keyFieldName = keyFieldName;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public Compression.Algorithm getCompressionAlgorithm() {
    return compressionAlgorithm;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public long getMaxFileSize() {
    return maxFileSize;
  }

  public boolean shouldPrefetchBlocksOnOpen() {
    return prefetchBlocksOnOpen;
  }

  public boolean shouldCacheDataInL1() {
    return cacheDataInL1;
  }

  public boolean shouldDropBehindCacheCompaction() {
    return dropBehindCacheCompaction;
  }

  public boolean useBloomFilter() {
    return bloomFilter != null;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public CellComparator getHFileComparator() {
    return hfileComparator;
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }
}
