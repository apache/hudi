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

package org.apache.hudi.hbase.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A thread-safe shared object pool in which object creation is expected to be lightweight, and the
 * objects may be excessively created and discarded.
 */
@InterfaceAudience.Private
public abstract class ObjectPool<K, V> {
  /**
   * An {@code ObjectFactory} object is used to create
   * new shared objects on demand.
   */
  public interface ObjectFactory<K, V> {
    /**
     * Creates a new shared object associated with the given {@code key},
     * identified by the {@code equals} method.
     * This method may be simultaneously called by multiple threads
     * with the same key, and the excessive objects are just discarded.
     */
    V createObject(K key);
  }

  protected final ReferenceQueue<V> staleRefQueue = new ReferenceQueue<>();

  private final ObjectFactory<K, V> objectFactory;

  /** Does not permit null keys. */
  protected final ConcurrentMap<K, Reference<V>> referenceCache;

  /** For preventing parallel purge */
  private final Lock purgeLock = new ReentrantLock();

  /**
   * The default initial capacity,
   * used when not otherwise specified in a constructor.
   */
  public static final int DEFAULT_INITIAL_CAPACITY = 16;

  /**
   * The default concurrency level,
   * used when not otherwise specified in a constructor.
   */
  public static final int DEFAULT_CONCURRENCY_LEVEL = 16;

  /**
   * Creates a new pool with the default initial capacity (16)
   * and the default concurrency level (16).
   *
   * @param objectFactory the factory to supply new objects on demand
   *
   * @throws NullPointerException if {@code objectFactory} is null
   */
  public ObjectPool(ObjectFactory<K, V> objectFactory) {
    this(objectFactory, DEFAULT_INITIAL_CAPACITY, DEFAULT_CONCURRENCY_LEVEL);
  }

  /**
   * Creates a new pool with the given initial capacity
   * and the default concurrency level (16).
   *
   * @param objectFactory the factory to supply new objects on demand
   * @param initialCapacity the initial capacity to keep objects in the pool
   *
   * @throws NullPointerException if {@code objectFactory} is null
   * @throws IllegalArgumentException if {@code initialCapacity} is negative
   */
  public ObjectPool(ObjectFactory<K, V> objectFactory, int initialCapacity) {
    this(objectFactory, initialCapacity, DEFAULT_CONCURRENCY_LEVEL);
  }

  /**
   * Creates a new pool with the given initial capacity
   * and the given concurrency level.
   *
   * @param objectFactory the factory to supply new objects on demand
   * @param initialCapacity the initial capacity to keep objects in the pool
   * @param concurrencyLevel the estimated count of concurrently accessing threads
   *
   * @throws NullPointerException if {@code objectFactory} is null
   * @throws IllegalArgumentException if {@code initialCapacity} is negative or
   *    {@code concurrencyLevel} is non-positive
   */
  public ObjectPool(
      ObjectFactory<K, V> objectFactory,
      int initialCapacity,
      int concurrencyLevel) {

    if (objectFactory == null) {
      throw new NullPointerException("Given object factory instance is NULL");
    }
    this.objectFactory = objectFactory;

    this.referenceCache =
        new ConcurrentHashMap<K, Reference<V>>(initialCapacity, 0.75f, concurrencyLevel);
  }

  /**
   * Removes stale references of shared objects from the pool. References newly becoming stale may
   * still remain.
   * <p/>
   * The implementation of this method is expected to be lightweight when there is no stale
   * reference with the Oracle (Sun) implementation of {@code ReferenceQueue}, because
   * {@code ReferenceQueue.poll} just checks a volatile instance variable in {@code ReferenceQueue}.
   */
  public void purge() {
    if (purgeLock.tryLock()) {// no parallel purge
      try {
        while (true) {
          @SuppressWarnings("unchecked")
          Reference<V> ref = (Reference<V>) staleRefQueue.poll();
          if (ref == null) {
            break;
          }
          referenceCache.remove(getReferenceKey(ref), ref);
        }
      } finally {
        purgeLock.unlock();
      }
    }
  }

  /**
   * Create a reference associated with the given object
   * @param key the key to store in the reference
   * @param obj the object to associate with
   * @return the reference instance
   */
  public abstract Reference<V> createReference(K key, V obj);

  /**
   * Get key of the given reference
   * @param ref The reference
   * @return key of the reference
   */
  public abstract K getReferenceKey(Reference<V> ref);

  /**
   * Returns a shared object associated with the given {@code key},
   * which is identified by the {@code equals} method.
   * @throws NullPointerException if {@code key} is null
   */
  public V get(K key) {
    Reference<V> ref = referenceCache.get(key);
    if (ref != null) {
      V obj = ref.get();
      if (obj != null) {
        return obj;
      }
      referenceCache.remove(key, ref);
    }

    V newObj = objectFactory.createObject(key);
    Reference<V> newRef = createReference(key, newObj);
    while (true) {
      Reference<V> existingRef = referenceCache.putIfAbsent(key, newRef);
      if (existingRef == null) {
        return newObj;
      }

      V existingObject = existingRef.get();
      if (existingObject != null) {
        return existingObject;
      }
      referenceCache.remove(key, existingRef);
    }
  }

  /**
   * Returns an estimated count of objects kept in the pool.
   * This also counts stale references,
   * and you might want to call {@link #purge()} beforehand.
   */
  public int size() {
    return referenceCache.size();
  }
}
