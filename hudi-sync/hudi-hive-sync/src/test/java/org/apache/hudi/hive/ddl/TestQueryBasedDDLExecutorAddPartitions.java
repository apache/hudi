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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that {@link QueryBasedDDLExecutor} reports the partitions it adds batch by batch, so a
 * failure part way leaves the batches before it reported.
 */
class TestQueryBasedDDLExecutorAddPartitions {

  private static final String TABLE_NAME = "tbl";

  /**
   * Runs no SQL, failing the statement at {@code failingStatement} (0-based) if set, as the
   * metastore would.
   */
  private static final class FailingExecutor extends QueryBasedDDLExecutor {
    private final Integer failingStatement;
    private int statementsRun;

    FailingExecutor(HiveSyncConfig config, Integer failingStatement) {
      super(config);
      this.failingStatement = failingStatement;
    }

    @Override
    public void runSQL(String sql) {
      if (failingStatement != null && statementsRun == failingStatement) {
        throw new HoodieHiveSyncException("statement " + statementsRun + " fails");
      }
      statementsRun++;
    }

    @Override
    public Map<String, String> getTableSchema(String tableName) {
      return Collections.emptyMap();
    }

    @Override
    public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
      // not exercised here
    }

    @Override
    public void close() {
      // no resources held
    }
  }

  @Test
  void reportsEachBatchAdded() {
    List<Integer> added = new ArrayList<>();
    new FailingExecutor(config(), null).addPartitionsToTable(TABLE_NAME, partitions(5), added::add);
    assertEquals(Arrays.asList(2, 2, 1), added);
  }

  @Test
  void reportsTheBatchesAddedBeforeAFailure() {
    List<Integer> added = new ArrayList<>();
    FailingExecutor executor = new FailingExecutor(config(), 1);
    assertThrows(HoodieHiveSyncException.class,
        () -> executor.addPartitionsToTable(TABLE_NAME, partitions(5), added::add));
    assertEquals(Collections.singletonList(2), added);
  }

  private static HiveSyncConfig config() {
    TypedProperties props = new TypedProperties();
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "db");
    props.setProperty(META_SYNC_BASE_PATH.key(), "file:///tmp/base");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "dt");
    props.setProperty(HIVE_BATCH_SYNC_PARTITION_NUM.key(), "2");
    return new HiveSyncConfig(props);
  }

  private static List<String> partitions(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> "2026-01-0" + (i + 1))
        .collect(Collectors.toList());
  }
}
