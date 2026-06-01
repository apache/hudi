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

import org.apache.hudi.common.serialization.CustomSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory that creates and caches {@link RocksDBDAO} singletons keyed by {@code (basePath, rocksDBBasePath)}.
 *
 * <p>Multiple callers that supply identical parameters share a single underlying RocksDB instance.
 * The factory tracks a reference count; the instance is physically closed and evicted only when
 * every holder has called {@link #release}.
 *
 * <p>Callers <em>must</em> call {@link #release} (not {@link RocksDBDAO#close}) when they are
 * done with the instance, so that the reference count is decremented correctly.
 *
 * <p>Note: column-family serializers and the {@code disableWAL} flag are applied only on the
 * first creation for a given key. Subsequent callers that pass different values for these
 * parameters will share the instance created by the first caller.
 */
public class RocksDBDAOFactory {
  private static final Object LOCK = new Object();
  private static final Map<String, Entry> INSTANCES = new HashMap<>();

  private RocksDBDAOFactory() {
  }

  /**
   * Returns a shared {@link RocksDBDAO} for the given parameters, creating one if absent.
   *
   * <p>Increments the internal reference count. Callers must invoke {@link #release} with the
   * same {@code basePath} and {@code rocksDBBasePath} when the instance is no longer needed.
   *
   * @param basePath                logical base path identifying the Hudi table
   * @param rocksDBBasePath         filesystem path under which RocksDB stores its data
   * @param columnFamilySerializers per-column-family serializers (applied only on first creation)
   * @param disableWAL              whether to disable the write-ahead log (applied only on first creation)
   * @return shared {@link RocksDBDAO} instance
   */
  public static RocksDBDAO getOrCreate(
      String basePath,
      String rocksDBBasePath,
      ConcurrentHashMap<String, CustomSerializer<?>> columnFamilySerializers,
      boolean disableWAL) {
    String key = buildKey(basePath, rocksDBBasePath);
    synchronized (LOCK) {
      Entry entry = INSTANCES.computeIfAbsent(key,
          k -> new Entry(new RocksDBDAO(basePath, rocksDBBasePath, columnFamilySerializers, disableWAL)));
      entry.refCount++;
      return entry.dao;
    }
  }

  /**
   * Releases a previously acquired {@link RocksDBDAO}.
   *
   * <p>Decrements the reference count for the instance identified by the given parameters.
   * When the count reaches zero the underlying RocksDB instance is closed and removed from
   * the cache. Subsequent calls to {@link #getOrCreate} with the same parameters will create
   * a fresh instance.
   *
   * @param basePath        logical base path used when acquiring the instance
   * @param rocksDBBasePath filesystem path used when acquiring the instance
   */
  public static void release(String basePath, String rocksDBBasePath) {
    String key = buildKey(basePath, rocksDBBasePath);
    RocksDBDAO toClose = null;
    synchronized (LOCK) {
      Entry entry = INSTANCES.get(key);
      if (entry != null && --entry.refCount <= 0) {
        INSTANCES.remove(key);
        toClose = entry.dao;
      }
    }
    // Close outside the lock so we do not hold it during potentially blocking I/O.
    if (toClose != null) {
      toClose.close();
    }
  }

  private static String buildKey(String basePath, String rocksDBBasePath) {
    return basePath + "|" + rocksDBBasePath;
  }

  private static final class Entry {
    final RocksDBDAO dao;
    int refCount;

    Entry(RocksDBDAO dao) {
      this.dao = dao;
      this.refCount = 0;
    }
  }
}
