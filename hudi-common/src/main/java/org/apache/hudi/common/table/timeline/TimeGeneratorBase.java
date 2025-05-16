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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_NUM_RETRIES;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_WAIT_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;

/**
 * Base time generator facility that maintains lock-related utilities.
 */
public abstract class TimeGeneratorBase implements TimeGenerator, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TimeGeneratorBase.class);

  /**
   * The lock provider.
   */
  private volatile LockProvider<?> lockProvider;
  /**
   * The maximum time to block for acquiring a lock.
   */
  private final int lockAcquireWaitTimeInMs;

  protected final HoodieTimeGeneratorConfig config;
  private final LockConfiguration lockConfiguration;

  /**
   * The hadoop configuration.
   */
  private final StorageConfiguration<?> storageConf;

  private final RetryHelper<Boolean, HoodieLockException> lockRetryHelper;

  public TimeGeneratorBase(HoodieTimeGeneratorConfig config, StorageConfiguration<?> storageConf) {
    this.config = config;
    this.lockConfiguration = config.getLockConfiguration();
    this.storageConf = storageConf;

    // The maximum times to retry in case there are failures.
    int maxRetries = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY,
        Integer.parseInt(DEFAULT_LOCK_ACQUIRE_NUM_RETRIES));
    lockAcquireWaitTimeInMs = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY,
        DEFAULT_LOCK_ACQUIRE_WAIT_TIMEOUT_MS);
    // The initial time to wait for each time generation to resolve the clock skew issue on distributed hosts.
    long initialRetryInternal = lockConfiguration.getConfig().getLong(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, 200);
    lockRetryHelper = new RetryHelper<>(initialRetryInternal, maxRetries, initialRetryInternal,
        Arrays.asList(HoodieLockException.class, InterruptedException.class), "acquire timeGenerator lock");
  }

  protected LockProvider<?> getLockProvider() {
    // Perform lazy initialization of lock provider only if needed
    if (lockProvider == null) {
      synchronized (this) {
        if (lockProvider == null) {
          String lockProviderClass = lockConfiguration.getConfig().getString("hoodie.write.lock.provider");
          LOG.info("LockProvider for TimeGenerator: {}", lockProviderClass);
          lockProvider = (LockProvider<?>) ReflectionUtils.loadClass(lockProviderClass,
              new Class<?>[] {LockConfiguration.class, StorageConfiguration.class},
              lockConfiguration, storageConf);
        }
      }
    }
    return lockProvider;
  }

  public void lock() {
    lockRetryHelper.start(() -> {
      try {
        if (!getLockProvider().tryLock(lockAcquireWaitTimeInMs, TimeUnit.MILLISECONDS)) {
          throw new HoodieLockException("Unable to acquire the lock. Current lock owner information : "
              + getLockProvider().getCurrentOwnerLockInfo());
        }
        return true;
      } catch (InterruptedException e) {
        throw new HoodieLockException(e);
      }
    });
  }

  public void unlock() {
    getLockProvider().unlock();
    closeQuietly();
  }

  private synchronized void closeQuietly() {
    try {
      if (lockProvider != null) {
        lockProvider.close();
        lockProvider = null;
        LOG.info("Released the connection of the timeGenerator lock");
      }
    } catch (Exception e) {
      LOG.info("Unable to release the connection of the timeGenerator lock");
    }
  }
}
