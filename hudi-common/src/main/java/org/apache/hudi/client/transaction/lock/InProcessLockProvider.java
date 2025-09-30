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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * InProcess level lock. This {@link LockProvider} implementation is to
 * guard table from concurrent operations happening in the local JVM process.
 * A separate lock is maintained per "table basepath".
 * <p>
 * Note: This Lock provider implementation doesn't allow lock reentrancy.
 * Attempting to reacquire the lock from the same thread will throw
 * HoodieLockException. Threads other than the current lock owner, will
 * block on lock() and return false on tryLock().
 */
public class InProcessLockProvider implements LockProvider<ReentrantReadWriteLock>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(InProcessLockProvider.class);
  private static final Map<String, ReentrantReadWriteLock> LOCK_INSTANCE_PER_BASEPATH = new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock lock;
  private final String basePath;
  private final long maxWaitTimeMillis;

  public InProcessLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    TypedProperties typedProperties = lockConfiguration.getConfig();
    basePath = lockConfiguration.getConfig().getProperty(HoodieCommonConfig.BASE_PATH.key());
    ValidationUtils.checkArgument(basePath != null);
    lock = LOCK_INSTANCE_PER_BASEPATH.computeIfAbsent(basePath, (ignore) -> new ReentrantReadWriteLock());
    maxWaitTimeMillis = typedProperties.getLong(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY,
        LockConfiguration.DEFAULT_LOCK_ACQUIRE_WAIT_TIMEOUT_MS);
  }

  @Override
  public void lock() {
    LOG.info(getLogMessage(LockState.ACQUIRING));
    if (lock.isWriteLockedByCurrentThread()) {
      throw new HoodieLockException(getLogMessage(LockState.ALREADY_ACQUIRED));
    }
    lock.writeLock().lock();
    LOG.info(getLogMessage(LockState.ACQUIRED));
  }

  @Override
  public boolean tryLock() {
    return tryLock(maxWaitTimeMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    LOG.info(getLogMessage(LockState.ACQUIRING));
    if (lock.isWriteLockedByCurrentThread()) {
      throw new HoodieLockException(getLogMessage(LockState.ALREADY_ACQUIRED));
    }

    boolean isLockAcquired;
    try {
      isLockAcquired = lock.writeLock().tryLock(time, unit);
    } catch (InterruptedException e) {
      throw new HoodieLockException(getLogMessage(LockState.FAILED_TO_ACQUIRE));
    }

    LOG.info(getLogMessage(isLockAcquired ? LockState.ACQUIRED : LockState.FAILED_TO_ACQUIRE));
    return isLockAcquired;
  }

  @Override
  public void unlock() {
    LOG.info(getLogMessage(LockState.RELEASING));
    try {
      if (lock.isWriteLockedByCurrentThread()) {
        lock.writeLock().unlock();
        LOG.info(getLogMessage(LockState.RELEASED));
      } else {
        LOG.info("Cannot unlock because the current thread does not hold the lock.");
      }
    } catch (Exception e) {
      throw new HoodieLockException(getLogMessage(LockState.FAILED_TO_RELEASE), e);
    }
  }

  @Override
  public ReentrantReadWriteLock getLock() {
    return lock;
  }

  @Override
  public void close() {
    if (lock.isWriteLockedByCurrentThread()) {
      lock.writeLock().unlock();
    }
    LOG.info(getLogMessage(LockState.ALREADY_RELEASED));
  }

  private String getLogMessage(LockState state) {
    return String.format("Base Path %s, Lock Instance %s, Thread %s, In-process lock state %s", basePath, getLock().toString(), Thread.currentThread().getName(), state.name());
  }
}
