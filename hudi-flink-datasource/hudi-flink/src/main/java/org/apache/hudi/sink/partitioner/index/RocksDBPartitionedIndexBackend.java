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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.RocksDBDAO;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * An implementation of {@link PartitionedIndexBackend} based on RocksDB.
 *
 * <p>Each data partition is stored in its own RocksDB column family, so that a partition's mapping
 * can be dropped as a unit. A partition is only registered as visible to {@link #get} in RocksDB's
 * own {@code default} column family after its own column family has been created, so a lookup never
 * sees a partition whose column family creation is still in flight.
 *
 * <p>This backend operates against an already-open RocksDB instance: it does not bootstrap partitions
 * from the metadata table and does not implement TTL-based eviction of partitions.
 */
@Slf4j
public class RocksDBPartitionedIndexBackend implements PartitionedIndexBackend {
  private static final String PARTITION_COLUMN_FAMILY_PREFIX = "pcf_";
  private static final String REGISTRY_COLUMN_FAMILY = StringUtils.fromUTF8Bytes(RocksDB.DEFAULT_COLUMN_FAMILY);

  private final RocksDBDAO rocksDBDAO;

  public RocksDBPartitionedIndexBackend(String rocksDbBasePath) {
    this.rocksDBDAO = new RocksDBDAO("hudi-partitioned-index-backend", rocksDbBasePath, new ConcurrentHashMap<>(), true);
  }

  @Override
  public String get(String partitionPath, String recordKey) {
    if (!isPartitionRegistered(partitionPath)) {
      return null;
    }
    return this.rocksDBDAO.get(partitionColumnFamily(partitionPath), recordKey);
  }

  @Override
  public void update(String partitionPath, String recordKey, String fileId) {
    String columnFamily = partitionColumnFamily(partitionPath);
    if (!this.rocksDBDAO.columnFamilyExists(columnFamily)) {
      this.rocksDBDAO.addColumnFamily(columnFamily);
    }
    this.rocksDBDAO.put(columnFamily, recordKey, fileId);
    if (!isPartitionRegistered(partitionPath)) {
      registerPartition(partitionPath);
    }
  }

  /**
   * Returns whether the given partition's column family has been fully created and is safe to read from.
   *
   * @param partitionPath the partition path to check
   */
  public boolean isPartitionRegistered(String partitionPath) {
    Boolean registered = this.rocksDBDAO.get(REGISTRY_COLUMN_FAMILY, partitionPath);
    return registered != null && registered;
  }

  /**
   * Lists the partitions currently registered in this backend.
   */
  public List<String> listRegisteredPartitions() {
    return this.rocksDBDAO.<Boolean>prefixSearch(REGISTRY_COLUMN_FAMILY, "")
        .map(Pair::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Drops a partition's column family and removes it from the registry.
   *
   * @param partitionPath the partition path to delete
   */
  public void deletePartition(String partitionPath) {
    String columnFamily = partitionColumnFamily(partitionPath);
    if (this.rocksDBDAO.columnFamilyExists(columnFamily)) {
      this.rocksDBDAO.dropColumnFamily(columnFamily);
    }
    this.rocksDBDAO.delete(REGISTRY_COLUMN_FAMILY, partitionPath);
  }

  private void registerPartition(String partitionPath) {
    this.rocksDBDAO.put(REGISTRY_COLUMN_FAMILY, partitionPath, Boolean.TRUE);
  }

  private static String partitionColumnFamily(String partitionPath) {
    return PARTITION_COLUMN_FAMILY_PREFIX + partitionPath;
  }

  @Override
  public void close() throws IOException {
    this.rocksDBDAO.close();
  }
}
