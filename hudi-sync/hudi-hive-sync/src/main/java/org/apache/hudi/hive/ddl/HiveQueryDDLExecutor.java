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

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.util.HiveDriverPool;
import org.apache.hudi.hive.util.HivePartitionUtil;
import org.apache.hudi.hive.util.IMetaStoreClientPool;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * This class offers DDL executor backed by the hive.ql Driver This class preserves the old useJDBC = false way of doing things.
 */
@Slf4j
public class HiveQueryDDLExecutor extends QueryBasedDDLExecutor {

  private final IMetaStoreClient metaStoreClient;
  private SessionState sessionState;
  private Driver hiveDriver;
  // When present, partition-phase SQL lists fan out across this pool; table-level SQL
  // (createTable, schema evolution, single-statement runSQL callers) always uses the
  // session `hiveDriver` above. See HiveDriverPool javadoc.
  private final Option<HiveDriverPool> driverPool;
  // When present, dropPartitionsToTable fans batches across this Thrift client pool.
  // Owned by HoodieHiveSyncClient; close() is delegated through there. See
  // IMetaStoreClientPool javadoc for the usage contract (partition-row ops only).
  private final Option<IMetaStoreClientPool> metaStoreClientPool;

  public HiveQueryDDLExecutor(HiveSyncConfig config, IMetaStoreClient metaStoreClient) {
    this(config, metaStoreClient, Option.empty(), Option.empty());
  }

  public HiveQueryDDLExecutor(HiveSyncConfig config, IMetaStoreClient metaStoreClient,
                              Option<HiveDriverPool> driverPool,
                              Option<IMetaStoreClientPool> metaStoreClientPool) {
    super(config);
    this.metaStoreClient = metaStoreClient;
    this.driverPool = driverPool;
    this.metaStoreClientPool = metaStoreClientPool;
    try {
      this.sessionState = new SessionState(config.getHiveConf(),
          UserGroupInformation.getCurrentUser().getShortUserName());
      SessionState.start(this.sessionState);
      this.sessionState.setCurrentDatabase(databaseName);
      this.hiveDriver = new org.apache.hadoop.hive.ql.Driver(config.getHiveConf());
    } catch (Exception e) {
      if (sessionState != null) {
        try {
          this.sessionState.close();
        } catch (IOException ioException) {
          log.error("Error while closing SessionState", ioException);
        }
      }
      if (this.hiveDriver != null) {
        this.hiveDriver.close();
      }
      throw new HoodieHiveSyncException("Failed to create HiveQueryDDL object", e);
    }
  }

  @Override
  public void runSQL(String sql) {
    updateHiveSQLs(Collections.singletonList(sql));
  }

  /**
   * Partition-phase SQL fan-out. When the driver pool is present, any leading
   * {@code USE database} statements are run on every worker (Hive 2.x's
   * ALTER PARTITION SET LOCATION ignores db.table qualifiers and uses the
   * connection's current database, so each worker needs to USE the right db
   * before any partition ALTER). The remaining statements are then dispatched
   * round-robin across the pool. Falls through to the sequential path on the
   * session Driver when no pool is configured.
   */
  @Override
  protected void runSQLs(List<String> sqls) {
    if (sqls.isEmpty()) {
      return;
    }
    if (!driverPool.isPresent()) {
      updateHiveSQLs(sqls);
      return;
    }
    HiveDriverPool pool = driverPool.get();
    int firstNonUse = 0;
    while (firstNonUse < sqls.size() && isUseStatement(sqls.get(firstNonUse))) {
      firstNonUse++;
    }
    if (firstNonUse > 0) {
      List<String> setupStatements = sqls.subList(0, firstNonUse);
      pool.runOnEachWorker(setupStatements);
    }
    List<String> partitionStatements = sqls.subList(firstNonUse, sqls.size());
    if (partitionStatements.isEmpty()) {
      return;
    }
    List<Future<?>> futures = pool.runAll(partitionStatements);
    pool.awaitAll(futures);
  }

  // Strict 4-char prefix match on "USE ". Internal callers (constructPartitionAlterStatements)
  // always emit the USE statement without leading whitespace; do not call with externally
  // supplied SQL that might be padded.
  private static boolean isUseStatement(String sql) {
    return sql != null && sql.regionMatches(true, 0, "USE ", 0, 4);
  }

  private List<CommandProcessorResponse> updateHiveSQLs(List<String> sqls) {
    List<CommandProcessorResponse> responses = new ArrayList<>();
    HoodieTimer timer = HoodieTimer.start();
    try {
      for (String sql : sqls) {
        if (hiveDriver != null) {
          responses.add(hiveDriver.run(sql));
        }
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed in executing SQL", e);
    }
    log.info("Executed {} SQL statements sequentially in {} ms", sqls.size(), timer.endTimer());
    return responses;
  }

  //TODO Duplicating it here from HMSDLExecutor as HiveQueryQL has no way of doing it on its own currently. Need to refactor it
  @Override
  public Map<String, String> getTableSchema(String tableName) {
    try {
      // HiveMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = metaStoreClient.getTable(databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getSd().getCols().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      log.info("Time taken to getTableSchema: {} ms", (end - start));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + tableName, e);
    }
  }

  @Override
  public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
    if (partitionsToDrop.isEmpty()) {
      log.info("No partitions to drop for {}", tableName);
      return;
    }

    log.info("Drop partitions {} on {}", partitionsToDrop.size(), tableName);
    try {
      int batchSyncPartitionNum = config.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM);
      List<List<String>> batches = CollectionUtils.batches(partitionsToDrop, batchSyncPartitionNum);
      runDropBatches(tableName, batches);
    } catch (Exception e) {
      log.error("{} drop partition failed", tableId(databaseName, tableName), e);
      throw new HoodieHiveSyncException(tableId(databaseName, tableName) + " drop partition failed", e);
    }
  }

  /**
   * Drops partitions one batch at a time. When {@link #metaStoreClientPool} is present,
   * batches fan out across the pool's worker threads (each borrowing an independent
   * IMetaStoreClient); otherwise batches are dispatched sequentially against the
   * session client. Hive has no batch-drop primitive that matches dropPartition's
   * semantics, so each worker still iterates its chunk one partition at a time — the
   * win is fanning chunks across independent Thrift clients.
   */
  private void runDropBatches(String tableName, List<List<String>> batches) throws Exception {
    if (!metaStoreClientPool.isPresent()) {
      for (List<String> batch : batches) {
        applyDropBatch(metaStoreClient, tableName, batch);
      }
      return;
    }
    IMetaStoreClientPool pool = metaStoreClientPool.get();
    List<Future<Void>> futures = new ArrayList<>(batches.size());
    for (List<String> batch : batches) {
      futures.add(pool.executor().submit(() ->
          pool.run(poolClient -> {
            applyDropBatch(poolClient, tableName, batch);
            return null;
          })
      ));
    }
    Exception firstError = null;
    for (Future<Void> f : futures) {
      try {
        f.get();
      } catch (Exception e) {
        if (firstError == null) {
          firstError = e;
        } else {
          log.warn("Additional drop batch failed on {} (suppressed in favor of first error)", tableName, e);
        }
      }
    }
    if (firstError != null) {
      throw firstError;
    }
  }

  private void applyDropBatch(IMetaStoreClient poolClient, String tableName, List<String> batch) throws Exception {
    for (String dropPartition : batch) {
      if (HivePartitionUtil.partitionExists(poolClient, tableName, dropPartition,
          partitionValueExtractor, config)) {
        String partitionClause =
            HivePartitionUtil.getPartitionClauseForDrop(dropPartition, partitionValueExtractor, config);
        poolClient.dropPartition(databaseName, tableName, partitionClause, false);
      }
      log.info("Drop partition {} on {}", dropPartition, tableName);
    }
  }

  @Override
  public void close() {
    // Close the pool first so the worker threads stop dispatching against their
    // Drivers before we tear down anything else. The pool's close() runs
    // Driver/SessionState cleanup on each worker's own thread.
    driverPool.ifPresent(pool -> {
      try {
        pool.close();
      } catch (Exception e) {
        log.warn("Error closing HiveDriverPool", e);
      }
    });
    if (metaStoreClient != null) {
      Hive.closeCurrent();
    }
    if (hiveDriver != null) {
      hiveDriver.close();
    }
  }
}
