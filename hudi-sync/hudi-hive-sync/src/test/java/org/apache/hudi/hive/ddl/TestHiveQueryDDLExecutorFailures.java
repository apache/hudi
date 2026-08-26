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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHiveQueryDDLExecutorFailures {

  @Test
  void emptyAndMetastoreFailurePathsAreHandled() throws Exception {
    HiveQueryDDLExecutor executor = mock(HiveQueryDDLExecutor.class, CALLS_REAL_METHODS);
    IMetaStoreClient metaStoreClient = mock(IMetaStoreClient.class);
    HiveSyncConfig config = mock(HiveSyncConfig.class);
    PartitionValueExtractor partitionValueExtractor = mock(PartitionValueExtractor.class);
    setField(executor, "driverPool", Option.empty());
    setField(executor, "metaStoreClient", metaStoreClient);
    setField(executor, "databaseName", "test_db");
    setField(executor, "config", config);
    setField(executor, "partitionValueExtractor", partitionValueExtractor);

    assertDoesNotThrow(() -> executor.runSQLs(Collections.emptyList()));
    assertDoesNotThrow(() -> executor.dropPartitionsToTable("table", Collections.emptyList()));

    when(metaStoreClient.getTable(anyString(), anyString())).thenThrow(new TException("unavailable"));
    when(config.getStringOrDefault(META_SYNC_DATABASE_NAME)).thenReturn("test_db");
    when(partitionValueExtractor.extractPartitionValuesInPath(anyString()))
        .thenReturn(Collections.singletonList("2026-08-06"));
    when(metaStoreClient.getPartition(anyString(), anyString(), anyList()))
        .thenThrow(new TException("unavailable"));
    assertThrows(HoodieHiveSyncException.class, () -> executor.getTableSchema("table"));
    assertThrows(HoodieHiveSyncException.class,
        () -> executor.dropPartitionsToTable("table", Collections.singletonList("datestr=2026-08-06")));
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Class<?> type = target.getClass();
    while (type != null) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        type = type.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }
}
