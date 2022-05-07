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

import org.apache.hudi.exception.HoodieNotSupportedException;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A map's implementation based on RocksDB.
 */
public final class RocksDBBasedMap<K extends Serializable, R extends Serializable> implements Map<K, R> {

  private static final String COL_FAMILY_NAME = "map_handle";

  private final String rocksDbStoragePath;
  private RocksDBDAO rocksDBDAO;
  private final String columnFamilyName;

  public RocksDBBasedMap(String rocksDbStoragePath) {
    this.rocksDbStoragePath = rocksDbStoragePath;
    this.columnFamilyName = COL_FAMILY_NAME;
  }

  @Override
  public int size() {
    return (int) getRocksDBDAO().prefixSearch(columnFamilyName, "").count();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    // Wont be able to store nulls as values
    return getRocksDBDAO().get(columnFamilyName, key.toString()) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    throw new HoodieNotSupportedException("Not Supported");
  }

  @Override
  public R get(Object key) {
    return getRocksDBDAO().get(columnFamilyName, (Serializable) key);
  }

  @Override
  public R put(K key, R value) {
    getRocksDBDAO().put(columnFamilyName, key, value);
    return value;
  }

  @Override
  public R remove(Object key) {
    R val = getRocksDBDAO().get(columnFamilyName, key.toString());
    getRocksDBDAO().delete(columnFamilyName, key.toString());
    return val;
  }

  @Override
  public void putAll(Map<? extends K, ? extends R> m) {
    getRocksDBDAO().writeBatch(batch -> m.forEach((key, value) -> getRocksDBDAO().putInBatch(batch, columnFamilyName, key, value)));
  }

  private RocksDBDAO getRocksDBDAO() {
    if (null == rocksDBDAO) {
      rocksDBDAO = new RocksDBDAO("default", rocksDbStoragePath);
      rocksDBDAO.addColumnFamily(columnFamilyName);
    }
    return rocksDBDAO;
  }

  @Override
  public void clear() {
    if (null != rocksDBDAO) {
      rocksDBDAO.close();
    }
    rocksDBDAO = null;
  }

  @Override
  public Set<K> keySet() {
    throw new HoodieNotSupportedException("Not Supported");
  }

  @Override
  public Collection<R> values() {
    throw new HoodieNotSupportedException("Not Supported");
  }

  @Override
  public Set<Entry<K, R>> entrySet() {
    throw new HoodieNotSupportedException("Not Supported");
  }

  public Iterator<R> iterator() {
    return getRocksDBDAO().prefixSearch(columnFamilyName, "").map(p -> (R) (p.getValue())).iterator();
  }
}
