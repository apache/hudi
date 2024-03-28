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

package org.apache.hudi.common.table.view;

import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HoodieTableFileSystemViewLock  implements Serializable {

  // Locks to control concurrency. Sync operations use write-lock blocking all fetch operations.
  // For the common-case, we allow concurrent read of single or multiple partitions
  private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = globalLock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = globalLock.writeLock();

  /**
   * Acquire read lock
   */
  public void readLock() {
    readLock.lock();
  }

  /**
   * Release read lock
   */
  public void readUnlock() {
    readLock.unlock();
  }

  /**
   * Acquire write lock
   */
  public void writeLock() {
    writeLock.lock();
  }

  /**
   * Release write lock
   */
  public void writeUnlock() {
    writeLock.unlock();
  }

  /**
   * Check if the current thread holds write lock
   */
  public boolean hasWriteLock() {
    return globalLock.isWriteLockedByCurrentThread();
  }
}
