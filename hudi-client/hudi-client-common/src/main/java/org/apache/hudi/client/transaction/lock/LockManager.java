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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

/**
 * This class wraps implementations of {@link LockProvider} and provides an easy way to manage the lifecycle of a lock.
 */
public class LockManager implements Serializable, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(LockManager.class);
  private final HoodieWriteConfig writeConfig;
  private final LockConfiguration lockConfiguration;
  private final SerializableConfiguration hadoopConf;
  private final int maxRetries;
  private final long maxWaitTimeInMs;
  private transient HoodieLockMetrics metrics;
  private volatile LockProvider lockProvider;

  public LockManager(HoodieWriteConfig writeConfig, FileSystem fs) {
    this.writeConfig = writeConfig;
    this.hadoopConf = new SerializableConfiguration(fs.getConf());
    this.lockConfiguration = new LockConfiguration(writeConfig.getProps());
    maxRetries = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY,
        Integer.parseInt(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.defaultValue()));
    maxWaitTimeInMs = lockConfiguration.getConfig().getLong(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY,
        Long.parseLong(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS.defaultValue()));
    metrics = new HoodieLockMetrics(writeConfig);
  }

  public void lock() {
    if (WriteConcurrencyMode.fromValue(writeConfig.getString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE)).supportsOptimisticConcurrencyControl()) {
      LockProvider lockProvider = getLockProvider();
      int retryCount = 0;
      boolean acquired = false;
      while (retryCount <= maxRetries) {
        try {
          metrics.startLockApiTimerContext();
          acquired = lockProvider.tryLock(writeConfig.getLong(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS), TimeUnit.MILLISECONDS);
          if (acquired) {
            metrics.updateLockAcquiredMetric();
            break;
          }
          metrics.updateLockNotAcquiredMetric();
          LOG.info("Retrying to acquire lock...");
          Thread.sleep(maxWaitTimeInMs);
        } catch (HoodieLockException | InterruptedException e) {
          metrics.updateLockNotAcquiredMetric();
          if (retryCount >= maxRetries) {
            throw new HoodieLockException("Unable to acquire lock, lock object " + lockProvider.getLock(), e);
          }
          try {
            Thread.sleep(maxWaitTimeInMs);
          } catch (InterruptedException ex) {
            // ignore InterruptedException here
          }
        } finally {
          retryCount++;
        }
      }
      if (!acquired) {
        throw new HoodieLockException("Unable to acquire lock, lock object " + lockProvider.getLock());
      }
    }
  }

  /**
   * We need to take care of the scenarios that current thread may not be the holder of this lock
   * and tries to call unlock()
   */
  public void unlock() {
    if (WriteConcurrencyMode.fromValue(writeConfig.getString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE)).supportsOptimisticConcurrencyControl()) {
      getLockProvider().unlock();
      metrics.updateLockHeldTimerMetrics();
    }
  }

  public synchronized LockProvider getLockProvider() {
    // Perform lazy initialization of lock provider only if needed
    if (lockProvider == null) {
      LOG.info("LockProvider " + writeConfig.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME));
      lockProvider = (LockProvider) ReflectionUtils.loadClass(writeConfig.getString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME),
          lockConfiguration, hadoopConf.get());
    }
    return lockProvider;
  }

  @Override
  public void close() {
    closeQuietly();
  }

  private void closeQuietly() {
    try {
      if (lockProvider != null) {
        lockProvider.close();
        LOG.info("Released connection created for acquiring lock");
        lockProvider = null;
      }
    } catch (Exception e) {
      LOG.error("Unable to close and release connection created for acquiring lock", e);
    }
  }
}
