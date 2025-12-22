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

package org.apache.hudi.common.config;

import java.io.Serializable;
import java.util.Properties;

/**
 * Configuration for managing locks. Since this configuration needs to be shared with HiveMetaStore based lock,
 * which is in a different package than other lock providers, we use this as a data transfer object in hoodie-common
 */
public class LockConfiguration implements Serializable {

  public static final String LOCK_PREFIX = "hoodie.write.lock.";

  public static final String LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY = LOCK_PREFIX + "wait_time_ms_between_retry";
  public static final String DEFAULT_LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS = String.valueOf(1000L);

  public static final String LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP_KEY = LOCK_PREFIX + "max_wait_time_ms_between_retry";

  public static final String LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY = LOCK_PREFIX + "client.wait_time_ms_between_retry";

  public static final String LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY = LOCK_PREFIX + "num_retries";
  public static final String DEFAULT_LOCK_ACQUIRE_NUM_RETRIES = String.valueOf(15);

  public static final String LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY = LOCK_PREFIX + "client.num_retries";

  public static final String LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY = LOCK_PREFIX + "wait_time_ms";

  public static final String LOCK_HEARTBEAT_INTERVAL_MS_KEY = LOCK_PREFIX + "heartbeat_interval_ms";
  public static final int DEFAULT_LOCK_HEARTBEAT_INTERVAL_MS = 60 * 1000;

  // configs for file system based locks. NOTE: This only works for DFS with atomic create/delete operation
  public static final String FILESYSTEM_BASED_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "filesystem.";

  public static final String FILESYSTEM_LOCK_PATH_PROP_KEY = FILESYSTEM_BASED_LOCK_PROPERTY_PREFIX + "path";

  public static final String FILESYSTEM_LOCK_EXPIRE_PROP_KEY = FILESYSTEM_BASED_LOCK_PROPERTY_PREFIX + "expire";

  // configs for metastore based locks
  public static final String HIVE_METASTORE_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "hivemetastore.";

  public static final String HIVE_DATABASE_NAME_PROP_KEY = HIVE_METASTORE_LOCK_PROPERTY_PREFIX + "database";

  public static final String HIVE_TABLE_NAME_PROP_KEY = HIVE_METASTORE_LOCK_PROPERTY_PREFIX + "table";

  public static final String HIVE_METASTORE_URI_PROP_KEY = HIVE_METASTORE_LOCK_PROPERTY_PREFIX + "uris";

  // Zookeeper configs for zk based locks
  public static final String ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "zookeeper.";

  public static final String ZK_BASE_PATH_PROP_KEY = ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "base_path";

  public static final String ZK_SESSION_TIMEOUT_MS_PROP_KEY = ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "session_timeout_ms";
  public static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 60 * 1000;

  public static final String ZK_CONNECTION_TIMEOUT_MS_PROP_KEY = ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "connection_timeout_ms";
  public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 15 * 1000;

  public static final String ZK_CONNECT_URL_PROP_KEY = ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "url";

  public static final String ZK_PORT_PROP_KEY = ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "port";

  public static final String ZK_LOCK_KEY_PROP_KEY = ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "lock_key";

  /** @deprecated Use {@link #LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY} */
  @Deprecated
  public static final String LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP = LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
  /** @deprecated Use {@link #LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP_KEY} */
  @Deprecated
  public static final String LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP = LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP_KEY;
  @Deprecated
  public static final String DEFAULT_LOCK_ACQUIRE_MAX_RETRY_WAIT_TIME_IN_MILLIS = String.valueOf(5000L);
  /** @deprecated Use {@link #LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY} */
  @Deprecated
  public static final String LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP = LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY;
  @Deprecated
  public static final String DEFAULT_LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS = String.valueOf(10000L);
  /** @deprecated Use {@link #LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY} */
  @Deprecated
  public static final String LOCK_ACQUIRE_NUM_RETRIES_PROP = LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY;
  /** @deprecated Use {@link #LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY} */
  @Deprecated
  public static final String LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP = LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY;
  @Deprecated
  public static final String DEFAULT_LOCK_ACQUIRE_CLIENT_NUM_RETRIES = String.valueOf(0);
  /** @deprecated Use {@link #LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY} */
  @Deprecated
  public static final String LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP = LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY;
  @Deprecated
  public static final int DEFAULT_ACQUIRE_LOCK_WAIT_TIMEOUT_MS = 60 * 1000;
  /** @deprecated Use {@link #HIVE_DATABASE_NAME_PROP_KEY} */
  @Deprecated
  public static final String HIVE_DATABASE_NAME_PROP = HIVE_DATABASE_NAME_PROP_KEY;
  /** @deprecated Use {@link #HIVE_TABLE_NAME_PROP_KEY} */
  @Deprecated
  public static final String HIVE_TABLE_NAME_PROP = HIVE_TABLE_NAME_PROP_KEY;
  /** @deprecated Use {@link #HIVE_METASTORE_URI_PROP_KEY} */
  @Deprecated
  public static final String HIVE_METASTORE_URI_PROP = HIVE_METASTORE_URI_PROP_KEY;
  /** @deprecated Use {@link #ZK_BASE_PATH_PROP_KEY} */
  @Deprecated
  public static final String ZK_BASE_PATH_PROP = ZK_BASE_PATH_PROP_KEY;
  /** @deprecated Use {@link #ZK_SESSION_TIMEOUT_MS_PROP_KEY} */
  @Deprecated
  public static final String ZK_SESSION_TIMEOUT_MS_PROP = ZK_SESSION_TIMEOUT_MS_PROP_KEY;
  /** @deprecated Use {@link #ZK_CONNECTION_TIMEOUT_MS_PROP_KEY} */
  @Deprecated
  public static final String ZK_CONNECTION_TIMEOUT_MS_PROP = ZK_CONNECTION_TIMEOUT_MS_PROP_KEY;
  /** @deprecated Use {@link #ZK_CONNECT_URL_PROP_KEY} */
  @Deprecated
  public static final String ZK_CONNECT_URL_PROP = ZK_CONNECT_URL_PROP_KEY;
  /** @deprecated Use {@link #ZK_PORT_PROP_KEY} */
  @Deprecated
  public static final String ZK_PORT_PROP = ZK_PORT_PROP_KEY;
  /** @deprecated Use {@link #ZK_LOCK_KEY_PROP_KEY} */
  @Deprecated
  public static final String ZK_LOCK_KEY_PROP = ZK_LOCK_KEY_PROP_KEY;

  private final TypedProperties props;

  public LockConfiguration(Properties props) {
    this.props = new TypedProperties(props);
  }

  public TypedProperties getConfig() {
    return props;
  }

}
