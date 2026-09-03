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
import org.apache.hudi.hive.util.HiveMetaStoreClientPool;
import org.apache.hudi.hive.util.HivePartitionUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  // HiveMetaStoreClientPool javadoc for the usage contract (partition-row ops only).
  private final Option<HiveMetaStoreClientPool> metaStoreClientPool;

  public HiveQueryDDLExecutor(HiveSyncConfig config, IMetaStoreClient metaStoreClient) {
    this(config, metaStoreClient, Option.empty(), Option.empty());
  }

  public HiveQueryDDLExecutor(HiveSyncConfig config, IMetaStoreClient metaStoreClient,
                              Option<HiveDriverPool> driverPool,
                              Option<HiveMetaStoreClientPool> metaStoreClientPool) {
    super(config);
    this.metaStoreClient = metaStoreClient;
    this.driverPool = driverPool;
    this.metaStoreClientPool = metaStoreClientPool;
    // SessionState.start() attaches the session it starts to this thread, displacing whatever the
    // caller had there -- another executor's session, or that of an application embedding this
    // sync. Ours is not the thread's to keep: every statement and the teardown bind it
    // explicitly, so give the thread back once the Driver, whose constructor reads
    // SessionState.get(), has been built.
    SessionState previousSession = SessionState.get();
    ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
    try {
      // The session gets a conf of its own because it does not just read one: its constructor
      // writes hive.session.id into it and swaps in a UDFClassLoader, and close() then deletes the
      // scratch directories that id names and closes that loader. Given config's HiveConf, which
      // outlives this executor, close() would leave the caller holding a closed loader, and every
      // later copy of that conf -- the driver and metastore client pools each take one -- would
      // inherit our session id and, with it, scratch directories we delete. HiveDriverPool's
      // workers own their conf for the same reason.
      HiveConf sessionConf = new HiveConf(config.getHiveConf());
      this.sessionState = new SessionState(sessionConf,
          UserGroupInformation.getCurrentUser().getShortUserName());
      SessionState.start(this.sessionState);
      this.sessionState.setCurrentDatabase(databaseName);
      this.hiveDriver = new org.apache.hadoop.hive.ql.Driver(sessionConf);
    } catch (Exception e) {
      try {
        closeDriverAndSession();
      } catch (Exception teardownException) {
        log.error("Error while closing Hive Driver and SessionState", teardownException);
      }
      // driverPool (if present) was already constructed by the caller before this
      // ctor ran; since we're about to throw, no one else will call close() on it.
      driverPool.ifPresent(pool -> {
        try {
          pool.close();
        } catch (Exception poolCloseException) {
          log.error("Error while closing HiveDriverPool", poolCloseException);
        }
      });
      throw new HoodieHiveSyncException("Failed to create HiveQueryDDL object", e);
    } finally {
      restoreThread(previousSession, previousLoader);
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
    int useStatementCount = 0;
    while (useStatementCount < sqls.size() && isUseStatement(sqls.get(useStatementCount))) {
      useStatementCount++;
    }
    if (useStatementCount > 0) {
      List<String> setupStatements = sqls.subList(0, useStatementCount);
      pool.runOnEachWorker(setupStatements);
    }
    List<String> partitionStatements = sqls.subList(useStatementCount, sqls.size());
    if (partitionStatements.isEmpty()) {
      return;
    }
    pool.awaitAll(pool.dispatchAll(partitionStatements));
  }

  /**
   * Splits TOUCH into batches of {@code HIVE_BATCH_SYNC_PARTITION_NUM} only when a
   * driver pool is actually present — i.e. only when {@link #runSQLs(List)} will
   * dispatch those batches in parallel. Keyed on pool presence rather than on the
   * {@code batching.enabled} config so the split can never take effect on a path
   * that would just execute the batches serially (the base class, and therefore
   * {@code JDBCExecutor}, always emits one statement).
   */
  @Override
  protected int getTouchBatchSize(int partitionCount) {
    return driverPool.isPresent()
        ? config.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM) : partitionCount;
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
    // Driver.compile() resolves its session from a thread local that every executor on this
    // thread writes: the one constructed most recently wins, and one that is closed clears it.
    // Bind ours, as Hive documents a thread running several sessions must, so these statements
    // run under the session that owns hiveDriver. The thread is not ours to keep, though -- it
    // may be another executor's or belong to an application that embeds this sync and holds its
    // own session -- so hand it back in the state we found it.
    SessionState previousSession = SessionState.get();
    ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
    try {
      SessionState.setCurrentSessionState(sessionState);
      for (String sql : sqls) {
        if (hiveDriver != null) {
          responses.add(hiveDriver.run(sql));
        }
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed in executing SQL", e);
    } finally {
      restoreThread(previousSession, previousLoader);
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
      // Resolved here, on the calling thread, rather than inside the workers: this is the
      // only sync path that would otherwise call a user-supplied PartitionValueExtractor
      // from several threads at once. Extractors are pluggable and not required to be
      // thread-safe, and a garbled clause would drop the wrong partition. It also halves
      // the extractor calls, since partitionExists and the drop clause share the values.
      List<PartitionToDrop> resolved = new ArrayList<>(partitionsToDrop.size());
      for (String partition : partitionsToDrop) {
        List<String> values = partitionValueExtractor.extractPartitionValuesInPath(partition);
        resolved.add(new PartitionToDrop(partition, values,
            HivePartitionUtil.getPartitionClauseForDrop(values, config)));
      }

      int batchSyncPartitionNum = config.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM);
      List<List<PartitionToDrop>> batches = CollectionUtils.batches(resolved, batchSyncPartitionNum);
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
   *
   * <p>First-error semantics come from {@link ParallelDispatch}, shared with
   * {@code HiveDriverPool}: the first failure is rethrown, batches that have not started
   * are stopped via the task-side abort flag, and later failures are logged at WARN.
   */
  private void runDropBatches(String tableName, List<List<PartitionToDrop>> batches) throws Exception {
    if (!metaStoreClientPool.isPresent()) {
      for (List<PartitionToDrop> batch : batches) {
        applyDropBatch(metaStoreClient, tableName, batch);
      }
      return;
    }
    HiveMetaStoreClientPool pool = metaStoreClientPool.get();
    pool.awaitAll(
        pool.dispatchAll(batches, (client, batch) -> applyDropBatch(client, tableName, batch)),
        "drop partition");
  }

  private void applyDropBatch(IMetaStoreClient client, String tableName, List<PartitionToDrop> batch) throws Exception {
    int dropped = 0;
    for (PartitionToDrop dropPartition : batch) {
      if (HivePartitionUtil.partitionExists(client, tableName, dropPartition.path,
          dropPartition.values, config)) {
        client.dropPartition(databaseName, tableName, dropPartition.clause, false);
        dropped++;
      }
      // Per-partition detail stays at debug: a batch can hold thousands of partitions
      // and N workers log concurrently, so INFO carries the per-batch summary instead.
      log.debug("Dropped partition {} on {}", dropPartition.path, tableName);
    }
    log.info("Dropped {} of {} partitions in batch on {}", dropped, batch.size(), tableName);
  }

  /**
   * A partition to drop with its extractor-derived values already resolved, so worker
   * threads never touch the shared {@link PartitionValueExtractor}. Immutable: the
   * {@code values} list is copied and wrapped unmodifiable at construction.
   */
  private static final class PartitionToDrop {
    private final String path;
    private final List<String> values;
    private final String clause;

    private PartitionToDrop(String path, List<String> values, String clause) {
      this.path = path;
      // Copied, not just wrapped: PartitionValueExtractor may hand back a buffer it reuses
      // across calls, and unmodifiableList would leave every entry aliasing the last
      // extraction. partitionExists would then check the wrong partition and skip drops.
      this.values = Collections.unmodifiableList(new ArrayList<>(values));
      this.clause = clause;
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
    closeDriverAndSession();
  }

  /**
   * Tears down the Driver and the SessionState this executor owns, with that session bound for the
   * duration. Both Driver methods act on whichever session the thread currently holds:
   * {@code close()} clears its lineage state and {@code destroy()} takes its transaction manager to
   * release locks. {@code SessionState.close()} then detaches, again regardless of whose session is
   * attached. So an executor constructed later on this thread would have its session damaged and
   * then unattached by this teardown unless ours is bound first and its own put back afterwards.
   */
  private void closeDriverAndSession() {
    SessionState previousSession = SessionState.get();
    ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
    bindQuietly(sessionState);
    try {
      if (hiveDriver != null) {
        try {
          hiveDriver.close();
        } finally {
          destroyQuietly(hiveDriver);
        }
      }
    } finally {
      closeQuietly(sessionState);
      // Unless the thread was holding ours, in which case there is nothing left to put back:
      // closeQuietly has detached that session and closed the loader that came with it.
      if (previousSession != sessionState) {
        restoreThread(previousSession, previousLoader);
      }
    }
  }

  /**
   * Binds this executor's session for a teardown that has to run either way: a bind failure must
   * not cost us the Driver's shutdown hook and the session's scratch directories, which are what
   * the teardown is for. The statement path reports a failed bind instead, since running SQL under
   * someone else's session is worse than not running it. A null session comes from the
   * constructor's error path, where the session is what failed.
   */
  private static void bindQuietly(SessionState state) {
    if (state == null) {
      return;
    }
    try {
      SessionState.setCurrentSessionState(state);
    } catch (Exception e) {
      log.error("Error while binding SessionState for teardown", e);
    }
  }

  /**
   * Puts the thread back the way an operation found it, so that binding this executor's session is
   * scoped to the operation that needs it. Hive has no notion of an empty session slot to assign,
   * hence the detach. The class loader has to be restored separately because
   * {@code setCurrentSessionState()} swaps in the session conf's loader -- a UDFClassLoader that
   * belongs to that one session -- while {@code detachSession()} only clears the session, and
   * {@code SessionState.close()} closes that loader on the way out. Left alone, the thread keeps a
   * loader it does not own, or a closed one, and class loading breaks for whoever owns the thread.
   */
  private static void restoreThread(SessionState previousSession, ClassLoader previousLoader) {
    if (previousSession != null) {
      SessionState.setCurrentSessionState(previousSession);
    } else {
      SessionState.detachSession();
    }
    Thread.currentThread().setContextClassLoader(previousLoader);
  }

  /**
   * Closes the SessionState this executor started. Hive derives the session's four scratch
   * directory roots from hive.session.id and only reclaims them in close(), so a HiveSyncTool that
   * runs per commit leaves a directory set behind on every sync. Called only from
   * {@link #closeDriverAndSession()}, which documents why the session goes last.
   */
  private static void closeQuietly(SessionState state) {
    if (state == null) {
      return;
    }
    try {
      state.close();
    } catch (Exception e) {
      log.error("Error while closing SessionState", e);
    }
  }

  /**
   * Removes the shutdown hook that {@link Driver#compile} registered. A fresh HiveSyncTool, and
   * therefore a fresh Driver, is built per sync, and close() leaves that hook in place, so without
   * this every sync permanently adds a Driver to the static {@code ShutdownHookManager}. Runs even
   * when close() failed, and reports rather than rethrows its own failure: destroy() ends up in
   * {@code ShutdownHookManager.removeShutdownHook}, which refuses to run once JVM shutdown has
   * begun, and by then the hook set no longer matters.
   */
  private static void destroyQuietly(Driver driver) {
    try {
      driver.destroy();
    } catch (Exception e) {
      log.warn("Error while destroying Hive Driver", e);
    }
  }
}
