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

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_CONNECTION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_SESSION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP_KEY;

/**
 * A zookeeper based lock. This {@link LockProvider} implementation allows to lock table operations
 * using zookeeper. Users need to have a Zookeeper cluster deployed to be able to use this lock.
 */
@NotThreadSafe
public class ZookeeperBasedLockProvider implements LockProvider<InterProcessMutex>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperBasedLockProvider.class);

  private final transient CuratorFramework curatorFrameworkClient;
  private volatile InterProcessMutex lock = null;
  protected LockConfiguration lockConfiguration;

  public ZookeeperBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    this.curatorFrameworkClient = CuratorFrameworkFactory.builder()
        .connectString(lockConfiguration.getConfig().getString(ZK_CONNECT_URL_PROP_KEY))
        .retryPolicy(new BoundedExponentialBackoffRetry(lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY),
            lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP_KEY), lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY)))
        .sessionTimeoutMs(lockConfiguration.getConfig().getInteger(ZK_SESSION_TIMEOUT_MS_PROP_KEY, DEFAULT_ZK_SESSION_TIMEOUT_MS))
        .connectionTimeoutMs(lockConfiguration.getConfig().getInteger(ZK_CONNECTION_TIMEOUT_MS_PROP_KEY, DEFAULT_ZK_CONNECTION_TIMEOUT_MS))
        .build();
    this.curatorFrameworkClient.start();
    createPathIfNotExists();
  }

  private String getLockPath() {
    return lockConfiguration.getConfig().getString(ZK_BASE_PATH_PROP_KEY) + "/"
        + this.lockConfiguration.getConfig().getString(ZK_LOCK_KEY_PROP_KEY);
  }

  private void createPathIfNotExists() {
    try {
      String lockPath = getLockPath();
      LOG.info(String.format("Creating zookeeper path %s if not exists", lockPath));
      String[] parts = lockPath.split("/");
      StringBuilder currentPath = new StringBuilder();
      for (String part : parts) {
        if (!part.isEmpty()) {
          currentPath.append("/").append(part);
          createNodeIfNotExists(currentPath.toString());
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to create ZooKeeper path: " + e.getMessage());
      throw new HoodieLockException("Failed to initialize ZooKeeper path", e);
    }
  }

  private void createNodeIfNotExists(String path) throws Exception {
    if (this.curatorFrameworkClient.checkExists().forPath(path) == null) {
      try {
        this.curatorFrameworkClient.create().forPath(path);
        // to avoid failure due to synchronous calls.
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.NODEEXISTS) {
          LOG.debug(String.format("Node already exist for path = %s", path));
        } else {
          throw new HoodieLockException("Failed to create zookeeper node", e);
        }
      }
    }
  }


  // Only used for testing
  public ZookeeperBasedLockProvider(
      final LockConfiguration lockConfiguration, final CuratorFramework curatorFrameworkClient) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    this.curatorFrameworkClient = curatorFrameworkClient;
    synchronized (this.curatorFrameworkClient) {
      if (this.curatorFrameworkClient.getState() != CuratorFrameworkState.STARTED) {
        this.curatorFrameworkClient.start();
        createPathIfNotExists();
      }
    }
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    LOG.info(generateLogStatement(LockState.ACQUIRING, generateLogSuffixString()));
    try {
      acquireLock(time, unit);
      LOG.info(generateLogStatement(LockState.ACQUIRED, generateLogSuffixString()));
    } catch (HoodieLockException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()), e);
    }
    return lock != null && lock.isAcquiredInThisProcess();
  }

  @Override
  public void unlock() {
    try {
      LOG.info(generateLogStatement(LockState.RELEASING, generateLogSuffixString()));
      if (lock == null || !lock.isAcquiredInThisProcess()) {
        return;
      }
      lock.release();
      lock = null;
      LOG.info(generateLogStatement(LockState.RELEASED, generateLogSuffixString()));
    } catch (Exception e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()), e);
    }
  }

  @Override
  public void close() {
    try {
      if (lock != null) {
        lock.release();
        lock = null;
      }
      this.curatorFrameworkClient.close();
    } catch (Exception e) {
      LOG.error(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()));
    }
  }

  @Override
  public InterProcessMutex getLock() {
    return this.lock;
  }

  private void acquireLock(long time, TimeUnit unit) throws Exception {
    ValidationUtils.checkArgument(this.lock == null, generateLogStatement(LockState.ALREADY_ACQUIRED, generateLogSuffixString()));
    InterProcessMutex newLock = new InterProcessMutex(
        this.curatorFrameworkClient, lockConfiguration.getConfig().getString(ZK_BASE_PATH_PROP_KEY) + "/"
        + this.lockConfiguration.getConfig().getString(ZK_LOCK_KEY_PROP_KEY));
    boolean acquired = newLock.acquire(time, unit);
    if (!acquired) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()));
    }
    if (newLock.isAcquiredInThisProcess()) {
      lock = newLock;
    } else {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()));
    }
  }

  private void checkRequiredProps(final LockConfiguration config) {
    ValidationUtils.checkArgument(config.getConfig().getString(ZK_CONNECT_URL_PROP_KEY) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(ZK_BASE_PATH_PROP_KEY) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(ZK_LOCK_KEY_PROP_KEY) != null);
  }

  private String generateLogSuffixString() {
    String zkBasePath = this.lockConfiguration.getConfig().getString(ZK_BASE_PATH_PROP_KEY);
    String lockKey = this.lockConfiguration.getConfig().getString(ZK_LOCK_KEY_PROP_KEY);
    return StringUtils.join("ZkBasePath = ", zkBasePath, ", lock key = ", lockKey);
  }

  protected String generateLogStatement(LockState state, String suffix) {
    return StringUtils.join(state.name(), " lock at", suffix);
  }
}
