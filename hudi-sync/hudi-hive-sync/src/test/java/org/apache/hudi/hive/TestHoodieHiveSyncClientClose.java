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

import org.apache.hudi.hive.ddl.DDLExecutor;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link HoodieHiveSyncClient#close()} connection cleanup.
 *
 * <p>Regression coverage for the metastore connection leak: when
 * {@code RetryingMetaStoreClient} rebuilds the underlying client on a transient
 * error, {@code Hive.closeCurrent()} alone closes the stale singleton-bound
 * client and orphans the retry-created one. {@code close()} must therefore
 * release the client held on the proxy field directly.
 */
class TestHoodieHiveSyncClientClose {

  @Test
  void closeReleasesProxiedMetastoreClientDirectly() throws Exception {
    HoodieHiveSyncClient syncClient = mock(HoodieHiveSyncClient.class, CALLS_REAL_METHODS);
    IMetaStoreClient metaStoreClient = mock(IMetaStoreClient.class);
    DDLExecutor ddlExecutor = mock(DDLExecutor.class);
    setField(syncClient, "client", metaStoreClient);
    setField(syncClient, "ddlExecutor", ddlExecutor);

    syncClient.close();

    verify(metaStoreClient).close();
    verify(ddlExecutor).close();
  }

  @Test
  void closeSwallowsProxyCloseFailure() throws Exception {
    HoodieHiveSyncClient syncClient = mock(HoodieHiveSyncClient.class, CALLS_REAL_METHODS);
    IMetaStoreClient metaStoreClient = mock(IMetaStoreClient.class);
    DDLExecutor ddlExecutor = mock(DDLExecutor.class);
    doThrow(new RuntimeException("transient close failure")).when(metaStoreClient).close();
    setField(syncClient, "client", metaStoreClient);
    setField(syncClient, "ddlExecutor", ddlExecutor);

    // A transient failure closing the proxied client must not propagate.
    assertDoesNotThrow(syncClient::close);
    verify(metaStoreClient).close();
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = HoodieHiveSyncClient.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
