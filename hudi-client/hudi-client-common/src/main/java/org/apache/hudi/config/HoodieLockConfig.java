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

import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.util.Option;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_NUM_RETRIES;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_CONNECTION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_ZK_SESSION_TIMEOUT_MS;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_METASTORE_URI_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_PREFIX;
import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_PORT_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP_KEY;

/**
 * Hoodie Configs for Locks.
 */
@ConfigClassProperty(name = "Locks Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configs that control locking mechanisms required for concurrency control "
        + " between writers to a Hudi table. Concurrency between Hudi's own table services "
        + " are auto managed internally.")
public class HoodieLockConfig extends HoodieConfig {

  public static final ConfigProperty<String> LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS = ConfigProperty
      .key(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY)
      .defaultValue(DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS)
      .sinceVersion("0.8.0")
      .withDocumentation("Initial amount of time to wait between retries to acquire locks, "
          + " subsequent retries will exponentially backoff.");

  public static final ConfigProperty<String> LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS = ConfigProperty
      .key(LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP_KEY)
      .defaultValue(String.valueOf(5000L))
      .sinceVersion("0.8.0")
      .withDocumentation("Maximum amount of time to wait between retries by lock provider client. This bounds"
          + " the maximum delay from the exponential backoff. Currently used by ZK based lock provider only.");

  public static final ConfigProperty<String> LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS = ConfigProperty
      .key(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY)
      .defaultValue(String.valueOf(10000L))
      .sinceVersion("0.8.0")
      .withDocumentation("Amount of time to wait between retries on the lock provider by the lock manager");

  public static final ConfigProperty<String> LOCK_ACQUIRE_NUM_RETRIES = ConfigProperty
      .key(LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY)
      .defaultValue(DEFAULT_LOCK_ACQUIRE_NUM_RETRIES)
      .sinceVersion("0.8.0")
      .withDocumentation("Maximum number of times to retry lock acquire, at each lock provider");

  public static final ConfigProperty<String> LOCK_ACQUIRE_CLIENT_NUM_RETRIES = ConfigProperty
      .key(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY)
      .defaultValue(String.valueOf(0))
      .sinceVersion("0.8.0")
      .withDocumentation("Maximum number of times to retry to acquire lock additionally from the lock manager.");

  public static final ConfigProperty<Integer> LOCK_ACQUIRE_WAIT_TIMEOUT_MS = ConfigProperty
      .key(LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY)
      .defaultValue(60 * 1000)
      .sinceVersion("0.8.0")
      .withDocumentation("Timeout in ms, to wait on an individual lock acquire() call, at the lock provider.");

  public static final ConfigProperty<String> FILESYSTEM_LOCK_PATH = ConfigProperty
      .key(FILESYSTEM_LOCK_PATH_PROP_KEY)
      .noDefaultValue()
      .sinceVersion("0.8.0")
      .withDocumentation("For DFS based lock providers, path to store the locks under.");

  public static final ConfigProperty<String> HIVE_DATABASE_NAME = ConfigProperty
      .key(HIVE_DATABASE_NAME_PROP_KEY)
      .noDefaultValue()
      .sinceVersion("0.8.0")
      .withDocumentation("For Hive based lock provider, the Hive database to acquire lock against");

  public static final ConfigProperty<String> HIVE_TABLE_NAME = ConfigProperty
      .key(HIVE_TABLE_NAME_PROP_KEY)
      .noDefaultValue()
      .sinceVersion("0.8.0")
      .withDocumentation("For Hive based lock provider, the Hive table to acquire lock against");

  public static final ConfigProperty<String> HIVE_METASTORE_URI = ConfigProperty
      .key(HIVE_METASTORE_URI_PROP_KEY)
      .noDefaultValue()
      .sinceVersion("0.8.0")
      .withDocumentation("For Hive based lock provider, the Hive metastore URI to acquire locks against.");

  public static final ConfigProperty<String> ZK_BASE_PATH = ConfigProperty
      .key(ZK_BASE_PATH_PROP_KEY)
      .noDefaultValue()
      .sinceVersion("0.8.0")
      .withDocumentation("The base path on Zookeeper under which to create lock related ZNodes. "
          + "This should be same for all concurrent writers to the same table");

  public static final ConfigProperty<Integer> ZK_SESSION_TIMEOUT_MS = ConfigProperty
      .key(ZK_SESSION_TIMEOUT_MS_PROP_KEY)
      .defaultValue(DEFAULT_ZK_SESSION_TIMEOUT_MS)
      .sinceVersion("0.8.0")
      .withDocumentation("Timeout in ms, to wait after losing connection to ZooKeeper, before the session is expired");

  public static final ConfigProperty<Integer> ZK_CONNECTION_TIMEOUT_MS = ConfigProperty
      .key(ZK_CONNECTION_TIMEOUT_MS_PROP_KEY)
      .defaultValue(DEFAULT_ZK_CONNECTION_TIMEOUT_MS)
      .sinceVersion("0.8.0")
      .withDocumentation("Timeout in ms, to wait for establishing connection with Zookeeper.");

  public static final ConfigProperty<String> ZK_CONNECT_URL = ConfigProperty
      .key(ZK_CONNECT_URL_PROP_KEY)
      .noDefaultValue()
      .sinceVersion("0.8.0")
      .withDocumentation("Zookeeper URL to connect to.");

  public static final ConfigProperty<String> ZK_PORT = ConfigProperty
      .key(ZK_PORT_PROP_KEY)
      .noDefaultValue()
      .sinceVersion("0.8.0")
      .withDocumentation("Zookeeper port to connect to.");

  public static final ConfigProperty<String> ZK_LOCK_KEY = ConfigProperty
      .key(ZK_LOCK_KEY_PROP_KEY)
      .noDefaultValue()
      .withInferFunction(p -> Option.ofNullable(p.getStringOrDefault(HoodieWriteConfig.TBL_NAME, null)))
      .sinceVersion("0.8.0")
      .withDocumentation("Key name under base_path at which to create a ZNode and acquire lock. "
          + "Final path on zk will look like base_path/lock_key. If this parameter is not set, we would "
          + "set it as the table name");

  // Pluggable type of lock provider
  public static final ConfigProperty<String> LOCK_PROVIDER_CLASS_NAME = ConfigProperty
      .key(LOCK_PREFIX + "provider")
      .defaultValue(ZookeeperBasedLockProvider.class.getName())
      .sinceVersion("0.8.0")
      .withDocumentation("Lock provider class name, user can provide their own implementation of LockProvider "
          + "which should be subclass of org.apache.hudi.common.lock.LockProvider");

  // Pluggable strategies to use when resolving conflicts
  public static final ConfigProperty<String> WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME = ConfigProperty
      .key(LOCK_PREFIX + "conflict.resolution.strategy")
      .defaultValue(SimpleConcurrentFileWritesConflictResolutionStrategy.class.getName())
      .sinceVersion("0.8.0")
      .withDocumentation("Lock provider class name, this should be subclass of "
          + "org.apache.hudi.client.transaction.ConflictResolutionStrategy");

  /** @deprecated Use {@link #WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME} and its methods instead */
  @Deprecated
  public static final String WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP = WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.key();
  /** @deprecated Use {@link #WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME} and its methods instead */
  @Deprecated
  public static final String DEFAULT_WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS = WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME.defaultValue();
  /** @deprecated Use {@link #LOCK_PROVIDER_CLASS_NAME} and its methods instead */
  @Deprecated
  public static final String LOCK_PROVIDER_CLASS_PROP = LOCK_PROVIDER_CLASS_NAME.key();
  /** @deprecated Use {@link #LOCK_PROVIDER_CLASS_NAME} and its methods instead */
  @Deprecated
  public static final String DEFAULT_LOCK_PROVIDER_CLASS = LOCK_PROVIDER_CLASS_NAME.defaultValue();

  private HoodieLockConfig() {
    super();
  }

  public static HoodieLockConfig.Builder newBuilder() {
    return new HoodieLockConfig.Builder();
  }

  public static class Builder {

    private final HoodieLockConfig lockConfig = new HoodieLockConfig();

    public HoodieLockConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.lockConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieLockConfig.Builder fromProperties(Properties props) {
      this.lockConfig.getProps().putAll(props);
      return this;
    }

    public HoodieLockConfig.Builder withLockProvider(Class<? extends LockProvider> lockProvider) {
      lockConfig.setValue(LOCK_PROVIDER_CLASS_NAME, lockProvider.getName());
      return this;
    }

    public HoodieLockConfig.Builder withHiveDatabaseName(String databaseName) {
      lockConfig.setValue(HIVE_DATABASE_NAME, databaseName);
      return this;
    }

    public HoodieLockConfig.Builder withHiveTableName(String tableName) {
      lockConfig.setValue(HIVE_TABLE_NAME, tableName);
      return this;
    }

    public HoodieLockConfig.Builder withHiveMetastoreURIs(String hiveMetastoreURIs) {
      lockConfig.setValue(HIVE_METASTORE_URI, hiveMetastoreURIs);
      return this;
    }

    public HoodieLockConfig.Builder withZkQuorum(String zkQuorum) {
      lockConfig.setValue(ZK_CONNECT_URL, zkQuorum);
      return this;
    }

    public HoodieLockConfig.Builder withZkBasePath(String zkBasePath) {
      lockConfig.setValue(ZK_BASE_PATH, zkBasePath);
      return this;
    }

    public HoodieLockConfig.Builder withZkPort(String zkPort) {
      lockConfig.setValue(ZK_PORT, zkPort);
      return this;
    }

    public HoodieLockConfig.Builder withZkLockKey(String zkLockKey) {
      lockConfig.setValue(ZK_LOCK_KEY, zkLockKey);
      return this;
    }

    public HoodieLockConfig.Builder withZkConnectionTimeoutInMs(Long connectionTimeoutInMs) {
      lockConfig.setValue(ZK_CONNECTION_TIMEOUT_MS, String.valueOf(connectionTimeoutInMs));
      return this;
    }

    public HoodieLockConfig.Builder withZkSessionTimeoutInMs(Long sessionTimeoutInMs) {
      lockConfig.setValue(ZK_SESSION_TIMEOUT_MS, String.valueOf(sessionTimeoutInMs));
      return this;
    }

    public HoodieLockConfig.Builder withNumRetries(int numRetries) {
      lockConfig.setValue(LOCK_ACQUIRE_NUM_RETRIES, String.valueOf(numRetries));
      return this;
    }

    public HoodieLockConfig.Builder withRetryWaitTimeInMillis(Long retryWaitTimeInMillis) {
      lockConfig.setValue(LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS, String.valueOf(retryWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withRetryMaxWaitTimeInMillis(Long retryMaxWaitTimeInMillis) {
      lockConfig.setValue(LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS, String.valueOf(retryMaxWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withClientNumRetries(int clientNumRetries) {
      lockConfig.setValue(LOCK_ACQUIRE_CLIENT_NUM_RETRIES, String.valueOf(clientNumRetries));
      return this;
    }

    public HoodieLockConfig.Builder withClientRetryWaitTimeInMillis(Long clientRetryWaitTimeInMillis) {
      lockConfig.setValue(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS, String.valueOf(clientRetryWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withLockWaitTimeInMillis(Long waitTimeInMillis) {
      lockConfig.setValue(LOCK_ACQUIRE_WAIT_TIMEOUT_MS, String.valueOf(waitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withConflictResolutionStrategy(ConflictResolutionStrategy conflictResolutionStrategy) {
      lockConfig.setValue(WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_NAME, conflictResolutionStrategy.getClass().getName());
      return this;
    }

    public HoodieLockConfig build() {
      lockConfig.setDefaults(HoodieLockConfig.class.getName());
      return lockConfig;
    }
  }

}
