/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import org.apache.hudi.sink.partitioner.index.RocksDBIndexBackend;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.rocksdb.TickerType;

/**
 * Metrics for RocksDB-backed index bootstrap state in flink bucket assign.
 */
public class FlinkRocksDBIndexMetrics extends HoodieFlinkMetrics {
  private static final String TOTAL_SST_FILES_SIZE_PROPERTY = "rocksdb.total-sst-files-size";
  private static final String LIVE_SST_FILES_SIZE_PROPERTY = "rocksdb.live-sst-files-size";
  private static final String BLOCK_CACHE_CAPACITY_PROPERTY = "rocksdb.block-cache-capacity";
  private static final String BLOCK_CACHE_USAGE_PROPERTY = "rocksdb.block-cache-usage";
  private static final String ACTIVE_MEMTABLE_SIZE_PROPERTY = "rocksdb.cur-size-active-mem-table";
  private static final String ALL_MEMTABLES_SIZE_PROPERTY = "rocksdb.cur-size-all-mem-tables";
  private static final String IMMUTABLE_MEMTABLE_COUNT_PROPERTY = "rocksdb.num-immutable-mem-table";

  public static final String ROCKSDB_DISK_TOTAL_SST_FILES_SIZE = "rocksdb.disk.total_sst_files_size";
  public static final String ROCKSDB_DISK_LIVE_SST_FILES_SIZE = "rocksdb.disk.live_sst_files_size";
  public static final String ROCKSDB_BLOCK_CACHE_CAPACITY = "rocksdb.block_cache.capacity";
  public static final String ROCKSDB_BLOCK_CACHE_USAGE = "rocksdb.block_cache.usage";
  public static final String ROCKSDB_BLOCK_CACHE_HIT_RATIO = "rocksdb.block_cache.hit_ratio";
  public static final String ROCKSDB_BLOCK_CACHE_DATA_HIT_RATIO = "rocksdb.block_cache.data_hit_ratio";
  public static final String ROCKSDB_BLOCK_CACHE_INDEX_HIT_RATIO = "rocksdb.block_cache.index_hit_ratio";
  public static final String ROCKSDB_BLOCK_CACHE_FILTER_HIT_RATIO = "rocksdb.block_cache.filter_hit_ratio";

  public static final String ROCKSDB_MEMTABLE_ACTIVE_SIZE = "rocksdb.memtable.active_size";
  public static final String ROCKSDB_MEMTABLE_ALL_SIZE = "rocksdb.memtable.all_size";
  public static final String ROCKSDB_MEMTABLE_IMMUTABLE_COUNT = "rocksdb.memtable.immutable_count";
  public static final String ROCKSDB_MEMTABLE_HIT_RATIO = "rocksdb.memtable.hit_ratio";

  private final RocksDBIndexBackend rocksDBIndexBackend;

  public FlinkRocksDBIndexMetrics(MetricGroup metricGroup, RocksDBIndexBackend rocksDBIndexBackend) {
    super(metricGroup);
    this.rocksDBIndexBackend = rocksDBIndexBackend;
  }

  @Override
  public void registerMetrics() {
    // disk metric
    metricGroup.gauge(ROCKSDB_DISK_TOTAL_SST_FILES_SIZE, (Gauge<Long>) this::getTotalSstFilesSize);
    metricGroup.gauge(ROCKSDB_DISK_LIVE_SST_FILES_SIZE, (Gauge<Long>) this::getLiveSstFilesSize);

    // block cache metric
    metricGroup.gauge(ROCKSDB_BLOCK_CACHE_CAPACITY, (Gauge<Long>) this::getBlockCacheCapacity);
    metricGroup.gauge(ROCKSDB_BLOCK_CACHE_USAGE, (Gauge<Long>) this::getBlockCacheUsage);
    metricGroup.gauge(ROCKSDB_BLOCK_CACHE_HIT_RATIO, (Gauge<Double>) this::getBlockCacheHitRatio);
    metricGroup.gauge(ROCKSDB_BLOCK_CACHE_DATA_HIT_RATIO, (Gauge<Double>) this::getBlockCacheDataHitRatio);
    metricGroup.gauge(ROCKSDB_BLOCK_CACHE_INDEX_HIT_RATIO, (Gauge<Double>) this::getBlockCacheIndexHitRatio);
    metricGroup.gauge(ROCKSDB_BLOCK_CACHE_FILTER_HIT_RATIO, (Gauge<Double>) this::getBlockCacheFilterHitRatio);

    // mem-table metric
    metricGroup.gauge(ROCKSDB_MEMTABLE_ACTIVE_SIZE, (Gauge<Long>) this::getActiveMemTableSize);
    metricGroup.gauge(ROCKSDB_MEMTABLE_ALL_SIZE, (Gauge<Long>) this::getAllMemTablesSize);
    metricGroup.gauge(ROCKSDB_MEMTABLE_IMMUTABLE_COUNT, (Gauge<Long>) this::getImmutableMemTableCount);
    metricGroup.gauge(ROCKSDB_MEMTABLE_HIT_RATIO, (Gauge<Double>) this::getMemTableHitRatio);
  }

  private long getTotalSstFilesSize() {
    return rocksDBIndexBackend.getLongMetric(TOTAL_SST_FILES_SIZE_PROPERTY);
  }

  private long getLiveSstFilesSize() {
    return rocksDBIndexBackend.getLongMetric(LIVE_SST_FILES_SIZE_PROPERTY);
  }

  private long getBlockCacheCapacity() {
    return rocksDBIndexBackend.getLongMetric(BLOCK_CACHE_CAPACITY_PROPERTY);
  }

  private long getBlockCacheUsage() {
    return rocksDBIndexBackend.getLongMetric(BLOCK_CACHE_USAGE_PROPERTY);
  }

  private double getBlockCacheHitRatio() {
    return rocksDBIndexBackend.getRatioMetric(TickerType.BLOCK_CACHE_HIT, TickerType.BLOCK_CACHE_MISS);
  }

  private double getBlockCacheDataHitRatio() {
    return rocksDBIndexBackend.getRatioMetric(TickerType.BLOCK_CACHE_DATA_HIT, TickerType.BLOCK_CACHE_DATA_MISS);
  }

  private double getBlockCacheIndexHitRatio() {
    return rocksDBIndexBackend.getRatioMetric(TickerType.BLOCK_CACHE_INDEX_HIT, TickerType.BLOCK_CACHE_INDEX_MISS);
  }

  private double getBlockCacheFilterHitRatio() {
    return rocksDBIndexBackend.getRatioMetric(TickerType.BLOCK_CACHE_FILTER_HIT, TickerType.BLOCK_CACHE_FILTER_MISS);
  }

  private long getActiveMemTableSize() {
    return rocksDBIndexBackend.getLongMetric(ACTIVE_MEMTABLE_SIZE_PROPERTY);
  }

  private long getAllMemTablesSize() {
    return rocksDBIndexBackend.getLongMetric(ALL_MEMTABLES_SIZE_PROPERTY);
  }

  private long getImmutableMemTableCount() {
    return rocksDBIndexBackend.getLongMetric(IMMUTABLE_MEMTABLE_COUNT_PROPERTY);
  }

  private double getMemTableHitRatio() {
    return rocksDBIndexBackend.getRatioMetric(TickerType.MEMTABLE_HIT, TickerType.MEMTABLE_MISS);
  }
}
