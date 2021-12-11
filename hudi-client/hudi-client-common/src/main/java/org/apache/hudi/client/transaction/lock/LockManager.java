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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
  private volatile LockProvider lockProvider;
  // Holds the latest completed write instant to know which ones to check conflict against
  private final AtomicReference<Option<HoodieInstant>> latestCompletedWriteInstant;

  public LockManager(HoodieWriteConfig writeConfig, FileSystem fs) {
    this.latestCompletedWriteInstant = new AtomicReference<>(Option.empty());
    this.writeConfig = writeConfig;
    this.hadoopConf = new SerializableConfiguration(fs.getConf());
    this.lockConfiguration = new LockConfiguration(writeConfig.getProps());
  }

  public void lock() {
    if (writeConfig.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
      LockProvider lockProvider = getLockProvider();
      int retryCount = 0;
      boolean acquired = false;
      int retries = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY);
      long waitTimeInMs = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY);
      while (retryCount <= retries) {
        try {
          acquired = lockProvider.tryLock(writeConfig.getLockAcquireWaitTimeoutInMs(), TimeUnit.MILLISECONDS);
          if (acquired) {
            break;
          }
          LOG.info("Retrying to acquire lock...");
          Thread.sleep(waitTimeInMs);
          retryCount++;
        } catch (HoodieLockException | InterruptedException e) {
          if (retryCount >= retries) {
            throw new HoodieLockException("Unable to acquire lock, lock object ", e);
          }
        }
      }
      if (!acquired) {
        throw new HoodieLockException("Unable to acquire lock, lock object " + lockProvider.getLock());
      }
    }
  }

  public void unlock() {
    if (writeConfig.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
      getLockProvider().unlock();
    }
  }

  public synchronized LockProvider getLockProvider() {
    // Perform lazy initialization of lock provider only if needed
    if (lockProvider == null) {
      LOG.info("LockProvider " + writeConfig.getLockProviderClass());
      lockProvider = (LockProvider) ReflectionUtils.loadClass(writeConfig.getLockProviderClass(),
          lockConfiguration, hadoopConf.get());
    }
    return lockProvider;
  }

  public void setLatestCompletedWriteInstant(Option<HoodieInstant> instant) {
    this.latestCompletedWriteInstant.set(instant);
  }

  public void compareAndSetLatestCompletedWriteInstant(Option<HoodieInstant> expected, Option<HoodieInstant> newValue) {
    this.latestCompletedWriteInstant.compareAndSet(expected, newValue);
  }

  public AtomicReference<Option<HoodieInstant>> getLatestCompletedWriteInstant() {
    return latestCompletedWriteInstant;
  }

  public void resetLatestCompletedWriteInstant() {
    this.latestCompletedWriteInstant.set(Option.empty());
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
