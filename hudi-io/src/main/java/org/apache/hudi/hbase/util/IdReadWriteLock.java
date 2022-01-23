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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Allows multiple concurrent clients to lock on a numeric id with ReentrantReadWriteLock. The
 * intended usage for read lock is as follows:
 *
 * <pre>
 * ReentrantReadWriteLock lock = idReadWriteLock.getLock(id);
 * try {
 *   lock.readLock().lock();
 *   // User code.
 * } finally {
 *   lock.readLock().unlock();
 * }
 * </pre>
 *
 * For write lock, use lock.writeLock()
 */
@InterfaceAudience.Private
public class IdReadWriteLock<T> {
  // The number of lock we want to easily support. It's not a maximum.
  private static final int NB_CONCURRENT_LOCKS = 1000;
  /**
   * The pool to get entry from, entries are mapped by {@link Reference} and will be automatically
   * garbage-collected by JVM
   */
  private final ObjectPool<T, ReentrantReadWriteLock> lockPool;
  private final ReferenceType refType;

  public IdReadWriteLock() {
    this(ReferenceType.WEAK);
  }

  /**
   * Constructor of IdReadWriteLock
   * @param referenceType type of the reference used in lock pool, {@link ReferenceType#WEAK} by
   *          default. Use {@link ReferenceType#SOFT} if the key set is limited and the locks will
   *          be reused with a high frequency
   */
  public IdReadWriteLock(ReferenceType referenceType) {
    this.refType = referenceType;
    switch (referenceType) {
      case SOFT:
        lockPool = new SoftObjectPool<>(new ObjectPool.ObjectFactory<T, ReentrantReadWriteLock>() {
          @Override
          public ReentrantReadWriteLock createObject(T id) {
            return new ReentrantReadWriteLock();
          }
        }, NB_CONCURRENT_LOCKS);
        break;
      case WEAK:
      default:
        lockPool = new WeakObjectPool<>(new ObjectPool.ObjectFactory<T, ReentrantReadWriteLock>() {
          @Override
          public ReentrantReadWriteLock createObject(T id) {
            return new ReentrantReadWriteLock();
          }
        }, NB_CONCURRENT_LOCKS);
    }
  }

  public static enum ReferenceType {
    WEAK, SOFT
  }

  /**
   * Get the ReentrantReadWriteLock corresponding to the given id
   * @param id an arbitrary number to identify the lock
   */
  public ReentrantReadWriteLock getLock(T id) {
    lockPool.purge();
    ReentrantReadWriteLock readWriteLock = lockPool.get(id);
    return readWriteLock;
  }

  /** For testing */
  int purgeAndGetEntryPoolSize() {
    gc();
    Threads.sleep(200);
    lockPool.purge();
    return lockPool.size();
  }

  private void gc() {
    System.gc();
  }

  public void waitForWaiters(T id, int numWaiters) throws InterruptedException {
    for (ReentrantReadWriteLock readWriteLock;;) {
      readWriteLock = lockPool.get(id);
      if (readWriteLock != null) {
        synchronized (readWriteLock) {
          if (readWriteLock.getQueueLength() >= numWaiters) {
            return;
          }
        }
      }
      Thread.sleep(50);
    }
  }

  public ReferenceType getReferenceType() {
    return this.refType;
  }
}
