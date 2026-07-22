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

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HiveMetastoreBasedLockProvider#close()} that exercise the thread-pool
 * shutdown path with a mocked {@link IMetaStoreClient}, without a live metastore or ZooKeeper.
 */
class TestHiveMetastoreBasedLockProviderClose {

  private static final String DB = "testdb";
  private static final String TABLE = "testtable";

  private LockConfiguration lockConfiguration;
  private LockComponent lockComponent;

  @BeforeEach
  void setUp() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HIVE_DATABASE_NAME_PROP_KEY, DB);
    props.setProperty(HIVE_TABLE_NAME_PROP_KEY, TABLE);
    lockConfiguration = new LockConfiguration(props);
    lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, DB);
    lockComponent.setTablename(TABLE);
  }

  @Test
  void closeShutsDownExecutorEvenWhenUnlockThrows() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(42L));
    doThrow(new TException("boom")).when(client).unlock(anyLong());

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

    // A failing unlock() must not prevent the heartbeat thread pool from being shut down.
    assertDoesNotThrow(provider::close);
    assertTrue(executorOf(provider).isShutdown(),
        "executor must be shut down even when unlock() throws");
  }

  @Test
  void closeShutsDownExecutorOnNormalPath() throws Exception {
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.lock(any())).thenReturn(acquiredLock(1L));

    HiveMetastoreBasedLockProvider provider = new HiveMetastoreBasedLockProvider(lockConfiguration, client);
    assertTrue(provider.acquireLock(1000L, TimeUnit.MILLISECONDS, lockComponent));

    provider.close();

    verify(client).unlock(1L);
    assertTrue(executorOf(provider).isShutdown());
  }

  private static LockResponse acquiredLock(long lockId) {
    LockResponse response = new LockResponse();
    response.setLockid(lockId);
    response.setState(LockState.ACQUIRED);
    return response;
  }

  private static ScheduledExecutorService executorOf(HiveMetastoreBasedLockProvider provider) throws Exception {
    Field field = HiveMetastoreBasedLockProvider.class.getDeclaredField("executor");
    field.setAccessible(true);
    return (ScheduledExecutorService) field.get(provider);
  }
}
