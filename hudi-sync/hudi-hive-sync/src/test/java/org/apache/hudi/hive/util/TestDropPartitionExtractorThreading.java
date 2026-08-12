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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.ddl.HiveQueryDDLExecutor;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The DROP path is the only sync path that fans work across pool workers while a
 * user-supplied {@link PartitionValueExtractor} is in play. ADD/TOUCH/SET_LOCATION build
 * their clauses on the calling thread before dispatch, so they never reach a shared
 * extractor from more than one thread.
 *
 * <p>Extractors are pluggable and carry no thread-safety contract. One holding mutable
 * state (a {@code SimpleDateFormat}, say) could return a garbled clause under concurrency
 * and drop the wrong partition, so the values are resolved before any fan-out.
 *
 * <p>Lives in {@code hive.util} rather than next to the executor because it needs the
 * package-private {@link HiveMetaStoreClientPool} constructor that takes pre-built clients.
 */
class TestDropPartitionExtractorThreading {

  @Test
  void extractorIsNeverInvokedFromAPoolWorker() throws Exception {
    // Given a pool wide enough to fan out, and more partitions than a single batch holds.
    List<IMetaStoreClient> poolClients = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      poolClients.add(mock(IMetaStoreClient.class));
    }
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(poolClients, 4);

    Set<String> extractorThreads = ConcurrentHashMap.newKeySet();
    PartitionValueExtractor recordingExtractor = mock(PartitionValueExtractor.class);
    when(recordingExtractor.extractPartitionValuesInPath(anyString())).thenAnswer(inv -> {
      extractorThreads.add(Thread.currentThread().getName());
      return Collections.singletonList("2026-08-06");
    });

    HiveSyncConfig config = mock(HiveSyncConfig.class);
    when(config.getStringOrDefault(META_SYNC_DATABASE_NAME)).thenReturn("test_db");
    when(config.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM)).thenReturn(2);
    when(config.getSplitStrings(META_SYNC_PARTITION_FIELDS))
        .thenReturn(Collections.singletonList("datestr"));

    HiveQueryDDLExecutor executor = mock(HiveQueryDDLExecutor.class, CALLS_REAL_METHODS);
    setField(executor, "driverPool", Option.empty());
    setField(executor, "metaStoreClient", mock(IMetaStoreClient.class));
    setField(executor, "metaStoreClientPool", Option.of(pool));
    setField(executor, "databaseName", "test_db");
    setField(executor, "config", config);
    setField(executor, "partitionValueExtractor", recordingExtractor);

    List<String> partitions = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      partitions.add("datestr=2026-08-" + String.format("%02d", i + 1));
    }

    try {
      // When the partitions are dropped across the pool.
      executor.dropPartitionsToTable("table", partitions);

      // Then every extractor call happened on this thread, never on a worker.
      assertEquals(Collections.singleton(Thread.currentThread().getName()), extractorThreads,
          "PartitionValueExtractor must only be invoked on the calling thread; a pool "
              + "worker thread here means a custom extractor is being shared concurrently");

      // Sanity: the drops really did reach the pool, so the assertion above had something
      // to catch rather than passing on a path that never fanned out at all.
      verify(poolClients.get(0), atLeastOnce()).getPartition(anyString(), anyString(), anyList());
    } finally {
      pool.close();
    }
  }

  /**
   * {@link PartitionValueExtractor} does not require returning a fresh list, so an
   * implementation may hand back one buffer it clears and refills per call. Merely wrapping
   * that list unmodifiable would leave every resolved partition aliasing the final
   * extraction, and {@code partitionExists} would then check the wrong partition — skipping
   * valid drops. The values must be copied at resolution time.
   */
  @Test
  void extractorReusingOneBufferStillYieldsPerPartitionValues() throws Exception {
    List<IMetaStoreClient> poolClients = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      poolClients.add(mock(IMetaStoreClient.class));
    }
    HiveMetaStoreClientPool pool = new HiveMetaStoreClientPool(poolClients, 2);

    // Given an extractor that recycles a single mutable list across calls.
    List<String> recycled = new ArrayList<>();
    PartitionValueExtractor reusingExtractor = mock(PartitionValueExtractor.class);
    when(reusingExtractor.extractPartitionValuesInPath(anyString())).thenAnswer(inv -> {
      String partition = inv.getArgument(0, String.class);
      recycled.clear();
      recycled.add(partition.replace("datestr=", ""));
      return recycled;
    });

    HiveSyncConfig config = mock(HiveSyncConfig.class);
    when(config.getStringOrDefault(META_SYNC_DATABASE_NAME)).thenReturn("test_db");
    when(config.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM)).thenReturn(2);
    when(config.getSplitStrings(META_SYNC_PARTITION_FIELDS))
        .thenReturn(Collections.singletonList("datestr"));

    HiveQueryDDLExecutor executor = mock(HiveQueryDDLExecutor.class, CALLS_REAL_METHODS);
    setField(executor, "driverPool", Option.empty());
    setField(executor, "metaStoreClient", mock(IMetaStoreClient.class));
    setField(executor, "metaStoreClientPool", Option.of(pool));
    setField(executor, "databaseName", "test_db");
    setField(executor, "config", config);
    setField(executor, "partitionValueExtractor", reusingExtractor);

    Set<String> lookedUp = ConcurrentHashMap.newKeySet();
    for (IMetaStoreClient client : poolClients) {
      when(client.getPartition(anyString(), anyString(), anyList())).thenAnswer(inv -> {
        lookedUp.add(String.join("/", (List<String>) inv.getArgument(2)));
        throw new NoSuchObjectException("absent");
      });
    }

    try {
      // When four distinct partitions are dropped.
      executor.dropPartitionsToTable("table",
          Arrays.asList("datestr=2026-08-01", "datestr=2026-08-02",
              "datestr=2026-08-03", "datestr=2026-08-04"));

      // Then each was looked up with its own values, not four copies of the last one.
      assertEquals(new HashSet<>(Arrays.asList("2026-08-01", "2026-08-02",
              "2026-08-03", "2026-08-04")), lookedUp,
          "each partition must be checked with the values extracted for it; identical "
              + "values here mean the resolved list aliased the extractor's reused buffer");
    } finally {
      pool.close();
    }
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
