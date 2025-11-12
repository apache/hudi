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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;

/**
 * This class wraps implementations of {@link LockProvider} and provides an easy way to manage the lifecycle of a lock.
 */
public class LockManager implements Serializable, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);
  private final HoodieWriteConfig writeConfig;
  private final LockConfiguration lockConfiguration;
  private final StorageConfiguration<?> storageConf;
  private final int maxRetries;
  private final long maxWaitTimeInMs;
  private final RetryHelper<Boolean, HoodieLockException> lockRetryHelper;
  private transient HoodieLockMetrics metrics;
  private volatile LockProvider lockProvider;

  public LockManager(HoodieWriteConfig writeConfig, HoodieStorage storage) {
    this(writeConfig, storage, writeConfig.getProps());
  }

  public LockManager(HoodieWriteConfig writeConfig, HoodieStorage storage, TypedProperties lockProps) {
    this.writeConfig = writeConfig;
    this.storageConf = storage.getConf().newInstance();
    this.lockConfiguration = new LockConfiguration(lockProps);
    maxRetries = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY,
        Integer.parseInt(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.defaultValue()));
    maxWaitTimeInMs = lockConfiguration.getConfig().getLong(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY,
        Long.parseLong(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS.defaultValue()));
    metrics = new HoodieLockMetrics(writeConfig, storage);
    lockRetryHelper = new RetryHelper<>(maxWaitTimeInMs, maxRetries, maxWaitTimeInMs,
        Arrays.asList(HoodieLockException.class, InterruptedException.class), "acquire lock");
  }

  public void lock() {
    lockRetryHelper.start(() -> {
      try {
        metrics.startLockApiTimerContext();
        if (!getLockProvider().tryLock(writeConfig.getLockAcquireWaitTimeoutInMs(), TimeUnit.MILLISECONDS)) {
          metrics.updateLockNotAcquiredMetric();
          throw new HoodieLockException("Unable to acquire the lock. Current lock owner information : "
              + getLockProvider().getCurrentOwnerLockInfo());
        }
        metrics.updateLockAcquiredMetric();
        return true;
      } catch (InterruptedException e) {
        throw new HoodieLockException(e);
      }
    });
  }

  /**
   * We need to take care of the scenarios that current thread may not be the holder of this lock
   * and tries to call unlock()
   */
  public void unlock() {
    getLockProvider().unlock();
    try {
      metrics.updateLockHeldTimerMetrics();
    } catch (HoodieException e) {
      LOG.error(String.format("Exception encountered when updating lock metrics: %s", e));
    }
    metrics.updateLockReleaseSuccessMetric();
    close();
  }

  public synchronized LockProvider getLockProvider() {
    // Perform lazy initialization of lock provider only if needed
    if (lockProvider == null) {
      LOG.info("LockProvider " + writeConfig.getLockProviderClass());
      
      // Try to load lock provider with HoodieLockMetrics constructor first
      Class<?>[] metricsConstructorTypes = {LockConfiguration.class, StorageConfiguration.class, HoodieLockMetrics.class};
      if (ReflectionUtils.hasConstructor(writeConfig.getLockProviderClass(), metricsConstructorTypes)) {
        lockProvider = (LockProvider) ReflectionUtils.loadClass(writeConfig.getLockProviderClass(),
            metricsConstructorTypes, lockConfiguration, storageConf, metrics);
      } else {
        LOG.debug("LockProvider does not support HoodieLockMetrics param in constructor, falling back to standard constructor");
        // Fallback to original constructor without metrics
        lockProvider = (LockProvider) ReflectionUtils.loadClass(writeConfig.getLockProviderClass(),
                new Class<?>[] {LockConfiguration.class, StorageConfiguration.class},
                lockConfiguration, storageConf);
      }
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
