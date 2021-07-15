/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.config;

import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.common.lock.LockProvider;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ACQUIRE_LOCK_WAIT_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_CLIENT_NUM_RETRIES;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_MAX_RETRY_WAIT_TIME_IN_MILLIS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_NUM_RETRIES;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_CONNECTION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_SESSION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_METASTORE_URI_PROP;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_PREFIX;
import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_PORT_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP;


/**
 * Hoodie Configs for Locks.
 */
public class HoodieLockConfig extends DefaultHoodieConfig {

  // Pluggable type of lock provider
  public static final String LOCK_PROVIDER_CLASS_PROP = LOCK_PREFIX + "provider";
  public static final String DEFAULT_LOCK_PROVIDER_CLASS = ZookeeperBasedLockProvider.class.getName();
  // Pluggable strategies to use when resolving conflicts
  public static final String WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP =
      LOCK_PREFIX + "conflict.resolution.strategy";
  public static final String DEFAULT_WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS =
      SimpleConcurrentFileWritesConflictResolutionStrategy.class.getName();

  private HoodieLockConfig(Properties props) {
    super(props);
  }

  public static HoodieLockConfig.Builder newBuilder() {
    return new HoodieLockConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieLockConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public HoodieLockConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieLockConfig.Builder withLockProvider(Class<? extends LockProvider> lockProvider) {
      props.setProperty(LOCK_PROVIDER_CLASS_PROP, lockProvider.getName());
      return this;
    }

    public HoodieLockConfig.Builder withHiveDatabaseName(String databaseName) {
      props.setProperty(HIVE_DATABASE_NAME_PROP, databaseName);
      return this;
    }

    public HoodieLockConfig.Builder withHiveTableName(String tableName) {
      props.setProperty(HIVE_TABLE_NAME_PROP, tableName);
      return this;
    }

    public HoodieLockConfig.Builder withHiveMetastoreURIs(String hiveMetastoreURIs) {
      props.setProperty(HIVE_METASTORE_URI_PROP, hiveMetastoreURIs);
      return this;
    }

    public HoodieLockConfig.Builder withZkQuorum(String zkQuorum) {
      props.setProperty(ZK_CONNECT_URL_PROP, zkQuorum);
      return this;
    }

    public HoodieLockConfig.Builder withZkBasePath(String zkBasePath) {
      props.setProperty(ZK_BASE_PATH_PROP, zkBasePath);
      return this;
    }

    public HoodieLockConfig.Builder withZkPort(String zkPort) {
      props.setProperty(ZK_PORT_PROP, zkPort);
      return this;
    }

    public HoodieLockConfig.Builder withZkLockKey(String zkLockKey) {
      props.setProperty(ZK_LOCK_KEY_PROP, zkLockKey);
      return this;
    }

    public HoodieLockConfig.Builder withZkConnectionTimeoutInMs(Long connectionTimeoutInMs) {
      props.setProperty(ZK_CONNECTION_TIMEOUT_MS_PROP, String.valueOf(connectionTimeoutInMs));
      return this;
    }

    public HoodieLockConfig.Builder withZkSessionTimeoutInMs(Long sessionTimeoutInMs) {
      props.setProperty(ZK_SESSION_TIMEOUT_MS_PROP, String.valueOf(sessionTimeoutInMs));
      return this;
    }

    public HoodieLockConfig.Builder withNumRetries(int numRetries) {
      props.setProperty(LOCK_ACQUIRE_NUM_RETRIES_PROP, String.valueOf(numRetries));
      return this;
    }

    public HoodieLockConfig.Builder withRetryWaitTimeInMillis(Long retryWaitTimeInMillis) {
      props.setProperty(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP, String.valueOf(retryWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withRetryMaxWaitTimeInMillis(Long retryMaxWaitTimeInMillis) {
      props.setProperty(LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP, String.valueOf(retryMaxWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withClientNumRetries(int clientNumRetries) {
      props.setProperty(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP, String.valueOf(clientNumRetries));
      return this;
    }

    public HoodieLockConfig.Builder withClientRetryWaitTimeInMillis(Long clientRetryWaitTimeInMillis) {
      props.setProperty(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP, String.valueOf(clientRetryWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withLockWaitTimeInMillis(Long waitTimeInMillis) {
      props.setProperty(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP, String.valueOf(waitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withConflictResolutionStrategy(ConflictResolutionStrategy conflictResolutionStrategy) {
      props.setProperty(WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP, conflictResolutionStrategy.getClass().getName());
      return this;
    }

    public HoodieLockConfig build() {
      HoodieLockConfig config = new HoodieLockConfig(props);
      setDefaultOnCondition(props, !props.containsKey(LOCK_PROVIDER_CLASS_PROP),
          LOCK_PROVIDER_CLASS_PROP, DEFAULT_LOCK_PROVIDER_CLASS);
      setDefaultOnCondition(props, !props.containsKey(WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP),
          WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP, DEFAULT_WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS);
      setDefaultOnCondition(props, !props.containsKey(LOCK_ACQUIRE_NUM_RETRIES_PROP),
          LOCK_ACQUIRE_NUM_RETRIES_PROP, DEFAULT_LOCK_ACQUIRE_NUM_RETRIES);
      setDefaultOnCondition(props, !props.containsKey(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP),
          LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP, DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS);
      setDefaultOnCondition(props, !props.containsKey(LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP),
          LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP, DEFAULT_LOCK_ACQUIRE_MAX_RETRY_WAIT_TIME_IN_MILLIS);
      setDefaultOnCondition(props, !props.containsKey(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP),
          LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP, DEFAULT_LOCK_ACQUIRE_CLIENT_NUM_RETRIES);
      setDefaultOnCondition(props, !props.containsKey(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP),
          LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP, DEFAULT_LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS);
      setDefaultOnCondition(props, !props.containsKey(ZK_CONNECTION_TIMEOUT_MS_PROP),
          ZK_CONNECTION_TIMEOUT_MS_PROP, String.valueOf(DEFAULT_ZK_CONNECTION_TIMEOUT_MS));
      setDefaultOnCondition(props, !props.containsKey(ZK_SESSION_TIMEOUT_MS_PROP),
          ZK_SESSION_TIMEOUT_MS_PROP, String.valueOf(DEFAULT_ZK_SESSION_TIMEOUT_MS));
      setDefaultOnCondition(props, !props.containsKey(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP),
          LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP, String.valueOf(DEFAULT_ACQUIRE_LOCK_WAIT_TIMEOUT_MS));
      return config;
    }
  }

}
