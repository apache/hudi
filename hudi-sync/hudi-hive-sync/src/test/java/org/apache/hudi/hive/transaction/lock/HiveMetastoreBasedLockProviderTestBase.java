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
import org.apache.hudi.common.config.TypedProperties;

import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.junit.jupiter.api.BeforeEach;

import java.lang.reflect.Field;

import static org.apache.hudi.common.config.LockConfiguration.DEFAULT_LOCK_HEARTBEAT_INTERVAL_MS;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_HEARTBEAT_INTERVAL_MS_KEY;

/**
 * Shared fixture for the {@link HiveMetastoreBasedLockProvider} unit tests that drive the provider
 * against a mocked {@code IMetaStoreClient}, without a live metastore or ZooKeeper.
 */
abstract class HiveMetastoreBasedLockProviderTestBase {

  protected static final String DB = "testdb";
  protected static final String TABLE = "testtable";

  protected LockConfiguration lockConfiguration;
  protected LockComponent lockComponent;

  @BeforeEach
  void setUpLockFixture() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HIVE_DATABASE_NAME_PROP_KEY, DB);
    props.setProperty(HIVE_TABLE_NAME_PROP_KEY, TABLE);
    props.setProperty(LOCK_HEARTBEAT_INTERVAL_MS_KEY, String.valueOf(heartbeatIntervalMs()));
    lockConfiguration = new LockConfiguration(props);
    lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, DB);
    lockComponent.setTablename(TABLE);
  }

  /**
   * The heartbeat interval the provider is configured with, overridden by tests that need the
   * scheduled heartbeat to actually fire while the test runs.
   */
  protected long heartbeatIntervalMs() {
    return DEFAULT_LOCK_HEARTBEAT_INTERVAL_MS;
  }

  protected static LockResponse acquiredLock(long lockId) {
    return lockResponse(lockId, LockState.ACQUIRED);
  }

  protected static LockResponse waitingLock(long lockId) {
    return lockResponse(lockId, LockState.WAITING);
  }

  private static LockResponse lockResponse(long lockId, LockState state) {
    LockResponse response = new LockResponse();
    response.setLockid(lockId);
    response.setState(state);
    return response;
  }

  /**
   * Reads a private field of the provider, for the state it does not expose: the heartbeat
   * schedule and the thread pool running it.
   */
  @SuppressWarnings("unchecked")
  protected static <T> T readField(HiveMetastoreBasedLockProvider provider, String name) throws Exception {
    Field field = HiveMetastoreBasedLockProvider.class.getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(provider);
  }
}
