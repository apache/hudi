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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BATCHING_ENABLED;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies which execution paths TOUCH batching applies to.
 *
 * <p>{@code hoodie.datasource.hive_sync.batching.enabled} only makes sense where the
 * resulting statements are dispatched in parallel. {@link QueryBasedDDLExecutor} is
 * also the base class for {@link JDBCExecutor}, which executes the list serially — so
 * splitting there would change statement count and partial-application semantics for
 * no benefit. The split is therefore driven by {@link QueryBasedDDLExecutor#getTouchBatchSize(int)},
 * which only {@link HiveQueryDDLExecutor} overrides (and only when a driver pool is
 * actually present).
 */
class TestQueryBasedDDLExecutorTouchBatching {

  private static final String TABLE_NAME = "tbl";
  private static final int PARTITION_COUNT = 5;
  private static final int BATCH_SIZE = 2;

  /**
   * Captures the SQL handed to the executor instead of running it. Uses the base
   * class's serial {@code runSQLs}, exactly as {@link JDBCExecutor} does.
   *
   * <p>{@code parallelBatchSize} stands in for a subclass that dispatches in parallel:
   * when set, {@link #getTouchBatchSize(int)} returns it, mimicking
   * {@link HiveQueryDDLExecutor} with a driver pool present. When unset, the base-class
   * default applies — the JDBC-mode shape.
   */
  private static final class RecordingExecutor extends QueryBasedDDLExecutor {
    private final List<String> executed = new ArrayList<>();
    private final Integer parallelBatchSize;

    RecordingExecutor(HiveSyncConfig config) {
      this(config, null);
    }

    RecordingExecutor(HiveSyncConfig config, Integer parallelBatchSize) {
      super(config);
      this.parallelBatchSize = parallelBatchSize;
    }

    @Override
    protected int getTouchBatchSize(int partitionCount) {
      return parallelBatchSize != null ? parallelBatchSize : super.getTouchBatchSize(partitionCount);
    }

    @Override
    public void runSQL(String sql) {
      executed.add(sql);
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

  private static HiveSyncConfig configWithBatching(boolean batchingEnabled) {
    TypedProperties props = new TypedProperties();
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "db");
    props.setProperty(META_SYNC_BASE_PATH.key(), "file:///tmp/base");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "dt");
    props.setProperty(HIVE_BATCH_SYNC_PARTITION_NUM.key(), String.valueOf(BATCH_SIZE));
    props.setProperty(HIVE_SYNC_BATCHING_ENABLED.key(), String.valueOf(batchingEnabled));
    return new HiveSyncConfig(props);
  }

  private static List<String> partitions() {
    return IntStream.range(0, PARTITION_COUNT)
        .mapToObj(i -> "2026-01-0" + (i + 1))
        .collect(Collectors.toList());
  }

  private static List<String> touchStatements(RecordingExecutor executor) {
    // constructPartitionAlterStatements always emits a leading `USE db`; the TOUCH
    // statements are everything after it.
    return executor.executed.stream()
        .filter(sql -> sql.contains(" TOUCH "))
        .collect(Collectors.toList());
  }

  /**
   * Given a serial executor (the JDBC-mode shape) with batching enabled, when TOUCH is
   * issued for more partitions than the batch size, then a single TOUCH statement is
   * still emitted — the flag must not reach non-parallel execution paths.
   */
  @Test
  void serialExecutorEmitsSingleTouchStatementEvenWithBatchingEnabled() {
    RecordingExecutor executor = new RecordingExecutor(configWithBatching(true));

    executor.touchPartitionsToTable(TABLE_NAME, partitions());

    List<String> touches = touchStatements(executor);
    assertEquals(1, touches.size(),
        "Serial executors (e.g. JDBC mode) must emit one TOUCH statement regardless of "
            + "hoodie.datasource.hive_sync.batching.enabled");
    assertEquals(PARTITION_COUNT, countPartitionClauses(touches.get(0)),
        "The single statement must still cover every partition");
  }

  /**
   * Given the same executor with batching disabled, when TOUCH is issued, then the SQL
   * is byte-identical to the enabled case — pinning that the flag is a no-op here.
   */
  @Test
  void serialExecutorTouchSqlIsIdenticalWithAndWithoutBatchingFlag() {
    RecordingExecutor withFlag = new RecordingExecutor(configWithBatching(true));
    RecordingExecutor withoutFlag = new RecordingExecutor(configWithBatching(false));

    withFlag.touchPartitionsToTable(TABLE_NAME, partitions());
    withoutFlag.touchPartitionsToTable(TABLE_NAME, partitions());

    assertEquals(withoutFlag.executed, withFlag.executed,
        "Enabling the batching flag must not change JDBC-mode TOUCH SQL shape");
  }

  /**
   * Given an executor that reports a parallel-dispatch batch size (the HiveQL-with-pool
   * shape), when TOUCH is issued, then partitions are split across multiple statements.
   * This pins that the base-class default is the only thing suppressing the split.
   */
  @Test
  void parallelExecutorSplitsTouchIntoBatches() {
    RecordingExecutor executor = new RecordingExecutor(configWithBatching(true), BATCH_SIZE);

    executor.touchPartitionsToTable(TABLE_NAME, partitions());

    List<String> touches = touchStatements(executor);
    // 5 partitions at 2 per batch -> 3 statements (2, 2, 1).
    assertEquals(3, touches.size());
    assertEquals(PARTITION_COUNT,
        touches.stream().mapToInt(TestQueryBasedDDLExecutorTouchBatching::countPartitionClauses).sum(),
        "Batching must not drop or duplicate partitions");
  }

  private static int countPartitionClauses(String sql) {
    int count = 0;
    int idx = sql.indexOf("PARTITION (");
    while (idx >= 0) {
      count++;
      idx = sql.indexOf("PARTITION (", idx + 1);
    }
    return count;
  }
}
