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

package org.apache.hudi.sink.utils;

import org.apache.hudi.exception.HoodieException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The commit guard used for blocking instant time generation.
 *
 * <p>A new instant time can not be generated until all the pending instants are committed.
 */
public class CommitGuard {
  /**
   * A lock used to block instant generation until being signaled after pending instant is committed.
   */
  private final Lock lock;
  private final Condition condition;
  private final long commitAckTimeout;

  private CommitGuard(long commitAckTimeout) {
    this.lock = new ReentrantLock();
    this.condition = lock.newCondition();
    this.commitAckTimeout = commitAckTimeout;
  }

  public static CommitGuard create(long commitAckTimeout) {
    return new CommitGuard(commitAckTimeout);
  }

  /**
   * Wait until all the pending instants are committed.
   *
   * @param instants Current pending instants
   */
  public void blockFor(String instants) {
    lock.lock();
    try {
      if (!condition.await(commitAckTimeout, TimeUnit.MILLISECONDS)) {
        throw new HoodieException("Timeout(" + commitAckTimeout + "ms) while waiting for instants [" + instants + "] to commit");
      }
    } catch (InterruptedException e) {
      throw new HoodieException("Blocking for instants completion is interrupted.", e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Signals all waited threads which are waiting on the condition.
   */
  public void unblock() {
    lock.lock();
    try {
      condition.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
