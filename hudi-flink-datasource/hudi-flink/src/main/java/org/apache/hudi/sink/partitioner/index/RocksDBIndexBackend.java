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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.util.collection.RocksDBDAO;
import org.apache.hudi.metrics.FlinkRocksDBIndexMetrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.MetricGroup;
import org.rocksdb.RocksDBException;
import org.rocksdb.TickerType;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of {@link GlobalIndexBackend} based on RocksDB.
 */
@Slf4j
public class RocksDBIndexBackend implements GlobalIndexBackend {
  private static final String COLUMN_FAMILY = "index_cache";

  private final RocksDBDAO rocksDBDAO;
  private transient FlinkRocksDBIndexMetrics rocksDBIndexMetrics;

  public RocksDBIndexBackend(String rocksDbBasePath, boolean isPartitionedTable) {
    // Register custom serializer for HoodieRecordGlobalLocation to minimize storage overhead
    ConcurrentHashMap<String, CustomSerializer<?>> serializers = new ConcurrentHashMap<>();
    serializers.put(COLUMN_FAMILY, isPartitionedTable
        ? new CodedRecordGlobalLocationSerializer()
        : new RecordGlobalLocationSerializer());

    this.rocksDBDAO = new RocksDBDAO("hudi-index-backend", rocksDbBasePath, serializers, true);
    this.rocksDBDAO.addColumnFamily(COLUMN_FAMILY);
  }

  @Override
  public HoodieRecordGlobalLocation get(String recordKey) {
    return this.rocksDBDAO.get(COLUMN_FAMILY, recordKey);
  }

  @Override
  public void update(String recordKey, HoodieRecordGlobalLocation recordGlobalLocation) {
    this.rocksDBDAO.put(COLUMN_FAMILY, recordKey, recordGlobalLocation);
  }

  @Override
  public void registerMetrics(MetricGroup metricGroup) {
    if (rocksDBIndexMetrics != null) {
      return;
    }
    this.rocksDBIndexMetrics = new FlinkRocksDBIndexMetrics(metricGroup, this);
    this.rocksDBIndexMetrics.registerMetrics();
  }

  public long getLongMetric(String property) {
    try {
      return this.rocksDBDAO.getLongProperty(property);
    } catch (RocksDBException | RuntimeException e) {
      log.debug("Failed to read RocksDB metric property {}", property, e);
      return 0L;
    }
  }

  public double getRatioMetric(TickerType hitTicker, TickerType missTicker) {
    try {
      long hits = this.rocksDBDAO.getTickerCount(hitTicker);
      long misses = this.rocksDBDAO.getTickerCount(missTicker);
      long total = hits + misses;
      return total == 0 ? 0D : (double) hits / total;
    } catch (RuntimeException e) {
      log.debug("Failed to read RocksDB ticker metrics {} and {}", hitTicker, missTicker, e);
      return 0D;
    }
  }

  @Override
  public void close() throws IOException {
    this.rocksDBDAO.close();
  }
}
