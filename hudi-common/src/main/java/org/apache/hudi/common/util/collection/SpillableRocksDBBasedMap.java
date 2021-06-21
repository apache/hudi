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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * This class provides a disk spillable only map implementation that is based on RocksDB.
 */
public final class SpillableRocksDBBasedMap<T extends Serializable, R extends Serializable> implements SpillableDiskMap<T, R> {
  // ColumnFamily allows partitioning data within RockDB, which allows
  // independent configuration and faster deletes across partitions
  // https://github.com/facebook/rocksdb/wiki/Column-Families
  // For this use case, we use a single static column family/ partition
  //
  private static final String COLUMN_FAMILY_NAME = "spill_map";

  private static final Logger LOG = LogManager.getLogger(SpillableRocksDBBasedMap.class);
  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Set<T> keySet;
  private final String rocksDbStoragePath;
  private RocksDBDAO rocksDb;

  public SpillableRocksDBBasedMap(String rocksDbStoragePath) throws IOException {
    this.keySet = new HashSet<>();
    this.rocksDbStoragePath = rocksDbStoragePath;
  }

  @Override
  public int size() {
    return keySet.size();
  }

  @Override
  public boolean isEmpty() {
    return keySet.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return keySet.contains((T) key);
  }

  @Override
  public boolean containsValue(Object value) {
    throw new HoodieNotSupportedException("unable to compare values in map");
  }

  @Override
  public R get(Object key) {
    if (!containsKey(key)) {
      return null;
    }
    return getRocksDb().get(COLUMN_FAMILY_NAME, (T) key);
  }

  @Override
  public R put(T key, R value) {
    getRocksDb().put(COLUMN_FAMILY_NAME, key, value);
    keySet.add(key);
    return value;
  }

  @Override
  public R remove(Object key) {
    R value = get(key);
    if (value != null) {
      keySet.remove((T) key);
      getRocksDb().delete(COLUMN_FAMILY_NAME, (T) key);
    }
    return value;
  }

  @Override
  public void putAll(Map<? extends T, ? extends R> keyValues) {
    getRocksDb().writeBatch(batch -> keyValues.forEach((key, value) -> getRocksDb().putInBatch(batch, COLUMN_FAMILY_NAME, key, value)));
    keySet.addAll(keyValues.keySet());
  }

  @Override
  public void clear() {
    close();
  }

  @Override
  public @NotNull Set<T> keySet() {
    return keySet;
  }

  @Override
  public @NotNull Collection<R> values() {
    throw new HoodieException("Unsupported Operation Exception");
  }

  @Override
  public @NotNull Set<Entry<T, R>> entrySet() {
    Set<Entry<T, R>> entrySet = new HashSet<>();
    for (T key : keySet) {
      entrySet.add(new AbstractMap.SimpleEntry<>(key, get(key)));
    }
    return entrySet;
  }

  /**
   * Custom iterator to iterate over values written to disk.
   */
  @Override
  public @NotNull Iterator<R> iterator() {
    return getRocksDb().iterator(COLUMN_FAMILY_NAME);
  }

  @Override
  public Stream<R> valueStream() {
    return keySet.stream().sorted().sequential().map(valueMetaData -> (R) get(valueMetaData));
  }

  @Override
  public long sizeOfFileOnDiskInBytes() {
    return getRocksDb().getTotalBytesWritten();
  }

  @Override
  public void close() {
    keySet.clear();
    if (null != rocksDb) {
      rocksDb.close();
    }
    rocksDb = null;
  }

  private RocksDBDAO getRocksDb() {
    if (null == rocksDb) {
      rocksDb = new RocksDBDAO(COLUMN_FAMILY_NAME, rocksDbStoragePath);
      rocksDb.addColumnFamily(COLUMN_FAMILY_NAME);
    }
    return rocksDb;
  }


}