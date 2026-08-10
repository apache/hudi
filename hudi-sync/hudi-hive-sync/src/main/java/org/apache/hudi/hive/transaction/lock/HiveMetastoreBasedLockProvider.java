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

package org.apache.hudi.hive.transaction.lock;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.hive.util.IMetaStoreClientUtil;
import org.apache.hudi.storage.StorageConfiguration;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_HEARTBEAT_INTERVAL_MS;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_METASTORE_URI_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_HEARTBEAT_INTERVAL_MS_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_PORT_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.lock.LockState.ACQUIRING;
import static org.apache.hudi.common.lock.LockState.ALREADY_ACQUIRED;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_ACQUIRE;
import static org.apache.hudi.common.lock.LockState.FAILED_TO_RELEASE;
import static org.apache.hudi.common.lock.LockState.RELEASED;
import static org.apache.hudi.common.lock.LockState.RELEASING;

/**
 * A hivemetastore based lock. Default HiveMetastore Lock Manager uses zookeeper to provide locks,
 * <a href="https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-Locking">read here</a>
 * This {@link LockProvider} implementation allows to lock table operations
 * using hive metastore APIs. Users need to have a HiveMetastore & Zookeeper cluster deployed to be able to use this lock.
 *
 */
@Slf4j
public class HiveMetastoreBasedLockProvider implements LockProvider<LockResponse>, Serializable {

  private final String databaseName;
  private final String tableName;
  private final String hiveMetastoreUris;
  @Getter
  private transient IMetaStoreClient hiveClient;
  @Getter
  private volatile LockResponse lock = null;
  protected LockConfiguration lockConfiguration;
  // Assigned by the acquiring thread, read and cancelled by the heartbeat thread.
  private transient volatile ScheduledFuture<?> future = null;
  // Set when the metastore reports the lock as expired or aborted, so that a later unlock() can
  // tell the caller its exclusivity was lost instead of silently doing nothing.
  private volatile boolean lockLostRemotely = false;
  private final transient ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

  public HiveMetastoreBasedLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
    this(lockConfiguration);
    try {
      HiveConf hiveConf = new HiveConf();
      setHiveLockConfs(hiveConf);
      hiveConf.addResource(conf.unwrapAs(Configuration.class));
      this.hiveClient = IMetaStoreClientUtil.getMSC(hiveConf);
    } catch (MetaException | HiveException e) {
      throw new HoodieLockException("Failed to create HiveMetaStoreClient", e);
    }
  }

  public HiveMetastoreBasedLockProvider(final LockConfiguration lockConfiguration, final IMetaStoreClient metaStoreClient) {
    this(lockConfiguration);
    this.hiveClient = metaStoreClient;
  }

  HiveMetastoreBasedLockProvider(final LockConfiguration lockConfiguration) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    this.databaseName = this.lockConfiguration.getConfig().getString(HIVE_DATABASE_NAME_PROP_KEY);
    this.tableName = this.lockConfiguration.getConfig().getString(HIVE_TABLE_NAME_PROP_KEY);
    this.hiveMetastoreUris = this.lockConfiguration.getConfig().getOrDefault(HIVE_METASTORE_URI_PROP_KEY, "").toString();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    log.info(generateLogStatement(ACQUIRING, generateLogSuffixString()));
    try {
      acquireLock(time, unit);
    } catch (ExecutionException | InterruptedException | TimeoutException | TException e) {
      throw new HoodieLockException(generateLogStatement(FAILED_TO_ACQUIRE, generateLogSuffixString()), e);
    }
    return isLockAcquired();
  }

  /**
   * Releases the lock held at the metastore, if any.
   *
   * <p>Unlike most providers, which stay silent when they do not believe they hold a lock, this one
   * deliberately fails when the metastore had already expired or aborted the lock: the writer went
   * on committing without exclusivity and has to hear about it. Note that {@code LockManager.unlock()}
   * skips its metrics and its {@code close()} when the provider throws.
   *
   * @throws HoodieLockException if the metastore took the lock away, or if releasing it fails.
   */
  @Override
  public void unlock() {
    try {
      log.info(generateLogStatement(RELEASING, generateLogSuffixString()));
      LockResponse lockResponseLocal = lock;
      if (lockResponseLocal == null) {
        if (lockLostRemotely) {
          // The heartbeat already dropped the lock. Unlocking it would fail with a bare
          // NoSuchLockException anyway, so fail with the actual reason instead.
          throw new HoodieLockException(generateLogStatement(FAILED_TO_RELEASE, generateLogSuffixString())
              + ", the metastore had already expired or aborted it");
        }
        return;
      }
      lock = null;
      cancelHeartbeat();
      hiveClient.unlock(lockResponseLocal.getLockid());
      log.info(generateLogStatement(RELEASED, generateLogSuffixString()));
    } catch (TException e) {
      throw new HoodieLockException(generateLogStatement(FAILED_TO_RELEASE, generateLogSuffixString()), e);
    }
  }

  public void acquireLock(long time, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException, TException {
    ValidationUtils.checkArgument(this.lock == null, ALREADY_ACQUIRED.name());
    final LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, this.databaseName);
    lockComponent.setTablename(tableName);
    acquireLockInternal(time, unit, lockComponent);
  }

  // NOTE: HiveMetastoreClient does not implement AutoCloseable. Additionally, we cannot call close() after unlock()
  // because if there are multiple operations started from the same WriteClient (via multiple threads), closing the
  // hive client causes all other threads who may have already initiated the tryLock() to fail since the
  // HiveMetastoreClient is shared.
  @Override
  public void close() {
    try {
      // Snapshot the lock, then stop claiming it before releasing it, exactly as unlock() does:
      // the release itself makes an in-flight heartbeat fail, and that must not be mistaken for
      // the metastore taking the lock away.
      LockResponse lockResponseLocal = lock;
      lock = null;
      cancelHeartbeat();
      if (lockResponseLocal != null) {
        hiveClient.unlock(lockResponseLocal.getLockid());
      }
      Hive.closeCurrent();
    } catch (Exception e) {
      log.error(generateLogStatement(org.apache.hudi.common.lock.LockState.FAILED_TO_RELEASE, generateLogSuffixString()), e);
    } finally {
      // Always release the heartbeat thread pool, even if unlock/closeCurrent above threw,
      // otherwise its scheduled threads leak for the lifetime of the JVM.
      executor.shutdown();
    }
  }

  // This API is exposed for tests and not intended to be used elsewhere
  public boolean acquireLock(long time, TimeUnit unit, final LockComponent component)
      throws InterruptedException, ExecutionException, TimeoutException, TException {
    ValidationUtils.checkArgument(this.lock == null, ALREADY_ACQUIRED.name());
    acquireLockInternal(time, unit, component);
    return isLockAcquired();
  }

  /**
   * Whether the lock is held right now. Reads {@link #lock} once: the heartbeat thread clears it
   * as soon as the metastore reports the lock as gone, so re-reading the field can mix two states.
   */
  private boolean isLockAcquired() {
    LockResponse lockResponseLocal = this.lock;
    return lockResponseLocal != null && lockResponseLocal.getState() == LockState.ACQUIRED;
  }

  private void acquireLockInternal(long time, TimeUnit unit, LockComponent lockComponent)
      throws InterruptedException, ExecutionException, TimeoutException, TException {
    lockLostRemotely = false;
    try {
      // TODO : FIX:Using the parameterized constructor throws MethodNotFound
      final LockRequestBuilder builder = new LockRequestBuilder();
      final LockRequest lockRequest = builder.addLockComponent(lockComponent).setUser(System.getProperty("user.name")).build();
      lockRequest.setUserIsSet(true);
      this.lock = executor.submit(() -> hiveClient.lock(lockRequest))
          .get(time, unit);
      scheduleHeartbeat();
    } finally {
      // it is better to release WAITING lock, otherwise hive lock will hang forever
      // Snapshot the lock: the heartbeat thread clears it as soon as the metastore reports it gone.
      LockResponse lockResponseLocal = this.lock;
      if (lockResponseLocal != null && lockResponseLocal.getState() != LockState.ACQUIRED) {
        hiveClient.unlock(lockResponseLocal.getLockid());
        lock = null;
        cancelHeartbeat();
      }
    }
  }

  /**
   * Schedules a periodic {@link Heartbeat} to refresh the currently held lock in case a commit
   * takes a long time. Does nothing unless {@link #lock} is held right now, so that callers may
   * invoke it without checking the state of the response they just got.
   */
  private void scheduleHeartbeat() {
    LockResponse lockResponseLocal = lock;
    if (lockResponseLocal == null) {
      // Released while the acquisition was still completing, so there is nothing left to renew.
      return;
    }
    if (lockResponseLocal.getState() != LockState.ACQUIRED) {
      // The metastore only queued the request. The caller releases such a lock right away, and
      // renewing one that was never granted can only latch a loss that never happened.
      return;
    }
    // Bind the id into the task and its callback: cancelling a schedule does not stop a tick that
    // is already inside the heartbeat RPC, so a tick can outlive the lock it was scheduled for.
    long lockId = lockResponseLocal.getLockid();
    Heartbeat heartbeat = new Heartbeat(hiveClient, lockId, cause -> onLockLost(lockId, cause));
    long heartbeatIntervalMs = lockConfiguration.getConfig()
        .getLong(LOCK_HEARTBEAT_INTERVAL_MS_KEY, DEFAULT_LOCK_HEARTBEAT_INTERVAL_MS);
    future = executor.scheduleAtFixedRate(heartbeat, heartbeatIntervalMs / 2, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Invoked from the heartbeat thread once the metastore reports the lock as expired or aborted.
   * The lock cannot be renewed anymore, so stop heartbeating it and stop claiming it is held.
   *
   * @param lockId the lock this heartbeat was scheduled for, which is not necessarily the one held
   *               now: releasing a lock is itself a reason for an in-flight heartbeat to fail.
   */
  private void onLockLost(long lockId, Exception cause) {
    LockResponse lockResponseLocal = lock;
    if (lockResponseLocal == null || lockResponseLocal.getLockid() != lockId) {
      // We released this lock ourselves while the tick was in flight, which is why the metastore
      // no longer knows about it. Nothing was lost, and any lock held now is a different one that
      // keeps its own heartbeat.
      log.debug("Ignoring a heartbeat failure for the already released lock {}", lockId, cause);
      return;
    }
    // Order matters: the flag is set first, so an unlock() racing with this can never find the
    // lock gone without a reason for it; and the schedule is cancelled before the lock is dropped,
    // so whoever observes the drop is guaranteed to see the renewal already stopped.
    lockLostRemotely = true;
    cancelHeartbeat();
    lock = null;
    log.error("The metastore expired or aborted the lock at{}, heartbeat stopped and exclusivity is lost",
        generateLogSuffixString(), cause);
  }

  private void cancelHeartbeat() {
    ScheduledFuture<?> futureLocal = future;
    if (futureLocal != null) {
      // Never interrupt: this can run on the heartbeat thread itself, and the current tick is
      // harmless. Cancelling only prevents further executions.
      futureLocal.cancel(false);
    }
  }

  private void checkRequiredProps(final LockConfiguration lockConfiguration) {
    ValidationUtils.checkArgument(lockConfiguration.getConfig().getString(HIVE_DATABASE_NAME_PROP_KEY) != null);
    ValidationUtils.checkArgument(lockConfiguration.getConfig().getString(HIVE_TABLE_NAME_PROP_KEY) != null);
  }

  private void setHiveLockConfs(HiveConf hiveConf) {
    if (!StringUtils.isNullOrEmpty(this.hiveMetastoreUris)) {
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, this.hiveMetastoreUris);
    }
    hiveConf.set("hive.support.concurrency", "true");
    hiveConf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager");
    hiveConf.set("hive.lock.numretries", lockConfiguration.getConfig().getString(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY));
    hiveConf.set("hive.unlock.numretries", lockConfiguration.getConfig().getString(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY));
    hiveConf.set("hive.lock.sleep.between.retries", lockConfiguration.getConfig().getString(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY));
    String zkConnectUrl = lockConfiguration.getConfig().getOrDefault(ZK_CONNECT_URL_PROP_KEY, "").toString();
    if (zkConnectUrl.length() > 0) {
      hiveConf.set("hive.zookeeper.quorum", zkConnectUrl);
    }
    String zkPort = lockConfiguration.getConfig().getOrDefault(ZK_PORT_PROP_KEY, "").toString();
    if (zkPort.length() > 0) {
      hiveConf.set("hive.zookeeper.client.port", zkPort);
    }
    String zkSessionTimeout = lockConfiguration.getConfig().getOrDefault(ZK_SESSION_TIMEOUT_MS_PROP_KEY, "").toString();
    if (zkSessionTimeout.length() > 0) {
      hiveConf.set("hive.zookeeper.session.timeout", zkSessionTimeout);
    }
  }

  private String generateLogSuffixString() {
    return StringUtils.join(" database ", databaseName, " and ", "table ", tableName);
  }

  protected String generateLogStatement(org.apache.hudi.common.lock.LockState state, String suffix) {
    return StringUtils.join(state.name(), " lock at", suffix);
  }
}
