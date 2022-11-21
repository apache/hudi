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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.transaction.lock.metrics.HoodieLockMetrics;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.exception.HoodieNotSupportedException;
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
  private HoodieWriteConfig writeConfig;
  private LockConfiguration lockConfiguration;
  private SerializableConfiguration hadoopConf;
  private int maxRetries;
  private long maxWaitTimeInMs;
  private transient HoodieLockMetrics metrics;
  private volatile LockProvider lockProvider;

  public LockManager(HoodieWriteConfig writeConfig, FileSystem fs) {
    init(writeConfig, fs.getConf(), writeConfig.getProps());
  }

  /**
   * Try to have a lock at partitionPath + fileID level for different write handler.
   * @param writeConfig
   * @param fs
   * @param partitionPath
   * @param fileId
   */
  public LockManager(HoodieWriteConfig writeConfig, FileSystem fs, String partitionPath, String fileId) {
    TypedProperties props = refreshLockConfig(writeConfig, partitionPath + "/" + fileId);
    init(writeConfig, fs.getConf(), props);
  }

  private void init(HoodieWriteConfig writeConfig, Configuration conf, TypedProperties lockProps) {
    this.lockConfiguration = new LockConfiguration(lockProps);
    this.writeConfig = writeConfig;
    this.hadoopConf = new SerializableConfiguration(conf);
    this.maxRetries = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY,
        Integer.parseInt(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.defaultValue()));
    this.maxWaitTimeInMs = lockConfiguration.getConfig().getLong(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY,
        Long.parseLong(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS.defaultValue()));
    this.metrics = new HoodieLockMetrics(writeConfig);
  }

  /**
   * rebuild lock related configs, only support ZK related lock for now.
   */
  private TypedProperties refreshLockConfig(HoodieWriteConfig writeConfig, String key) {
    TypedProperties props = new TypedProperties(writeConfig.getProps());
    String zkBasePath = props.getProperty(LockConfiguration.ZK_BASE_PATH_PROP_KEY);
    if (StringUtils.isNullOrEmpty(zkBasePath)) {
      throw new HoodieNotSupportedException("Only Support ZK based lock for now.");
    }
    props.setProperty(LockConfiguration.ZK_LOCK_KEY_PROP_KEY, key);
    return props;
  }

  public void lock() {
    if (writeConfig.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
      LockProvider lockProvider = getLockProvider();
      int retryCount = 0;
      boolean acquired = false;
      while (retryCount <= maxRetries) {
        try {
          metrics.startLockApiTimerContext();
          acquired = lockProvider.tryLock(writeConfig.getLockAcquireWaitTimeoutInMs(), TimeUnit.MILLISECONDS);
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
    if (writeConfig.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
      getLockProvider().unlock();
      metrics.updateLockHeldTimerMetrics();
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
