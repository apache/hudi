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

package org.apache.hudi.hive;

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
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_METASTORE_URI_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
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
 * A hivemetastore based lock. Default HiveMetastore Lock Manager uses zookeeper to provide locks, read here
 * {@link /cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-Locking}
 * This {@link LockProvider} implementation allows to lock table operations
 * using hive metastore APIs. Users need to have a HiveMetastore & Zookeeper cluster deployed to be able to use this lock.
 *
 */
public class HiveMetastoreBasedLockProvider implements LockProvider<LockResponse> {

  private static final Logger LOG = LogManager.getLogger(HiveMetastoreBasedLockProvider.class);

  private final String databaseName;
  private final String tableName;
  private final String hiveMetastoreUris;
  private IMetaStoreClient hiveClient;
  private volatile LockResponse lock = null;
  protected LockConfiguration lockConfiguration;
  ExecutorService executor = Executors.newSingleThreadExecutor();

  public HiveMetastoreBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) {
    this(lockConfiguration);
    try {
      HiveConf hiveConf = new HiveConf();
      setHiveLockConfs(hiveConf);
      hiveConf.addResource(conf);
      this.hiveClient = Hive.get(hiveConf).getMSC();
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
    LOG.info(generateLogStatement(ACQUIRING, generateLogSuffixString()));
    try {
      acquireLock(time, unit);
    } catch (ExecutionException | InterruptedException | TimeoutException | TException e) {
      throw new HoodieLockException(generateLogStatement(FAILED_TO_ACQUIRE, generateLogSuffixString()), e);
    }
    return this.lock != null && this.lock.getState() == LockState.ACQUIRED;
  }

  @Override
  public boolean tryLockWithInstant(long time, TimeUnit unit, String timestamp) {
    return tryLock(time, unit);
  }

  @Override
  public void unlock() {
    try {
      LOG.info(generateLogStatement(RELEASING, generateLogSuffixString()));
      LockResponse lockResponseLocal = lock;
      if (lockResponseLocal == null) {
        return;
      }
      lock = null;
      hiveClient.unlock(lockResponseLocal.getLockid());
      LOG.info(generateLogStatement(RELEASED, generateLogSuffixString()));
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
      if (lock != null) {
        hiveClient.unlock(lock.getLockid());
      }
      Hive.closeCurrent();
    } catch (Exception e) {
      LOG.error(generateLogStatement(org.apache.hudi.common.lock.LockState.FAILED_TO_RELEASE, generateLogSuffixString()));
    }
  }

  public IMetaStoreClient getHiveClient() {
    return hiveClient;
  }

  @Override
  public LockResponse getLock() {
    return this.lock;
  }

  // This API is exposed for tests and not intended to be used elsewhere
  public boolean acquireLock(long time, TimeUnit unit, final LockComponent component)
      throws InterruptedException, ExecutionException, TimeoutException, TException {
    ValidationUtils.checkArgument(this.lock == null, ALREADY_ACQUIRED.name());
    acquireLockInternal(time, unit, component);
    return this.lock != null && this.lock.getState() == LockState.ACQUIRED;
  }

  private void acquireLockInternal(long time, TimeUnit unit, LockComponent lockComponent)
      throws InterruptedException, ExecutionException, TimeoutException, TException {
    LockRequest lockRequest = null;
    try {
      // TODO : FIX:Using the parameterized constructor throws MethodNotFound
      final LockRequestBuilder builder = new LockRequestBuilder();
      lockRequest = builder.addLockComponent(lockComponent).setUser(System.getProperty("user.name")).build();
      lockRequest.setUserIsSet(true);
      final LockRequest lockRequestFinal = lockRequest;
      this.lock = executor.submit(() -> hiveClient.lock(lockRequestFinal))
          .get(time, unit);
    } catch (InterruptedException | TimeoutException e) {
      if (this.lock == null || this.lock.getState() != LockState.ACQUIRED) {
        LockResponse lockResponse = this.hiveClient.checkLock(lockRequest.getTxnid());
        if (lockResponse.getState() == LockState.ACQUIRED) {
          this.lock = lockResponse;
        } else {
          throw e;
        }
      }
    } finally {
      // it is better to release WAITING lock, otherwise hive lock will hang forever
      if (this.lock != null && this.lock.getState() != LockState.ACQUIRED) {
        hiveClient.unlock(this.lock.getLockid());
      }
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
