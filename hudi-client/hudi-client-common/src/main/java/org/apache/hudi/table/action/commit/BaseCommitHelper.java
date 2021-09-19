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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class BaseCommitHelper<T extends HoodieRecordPayload<T>> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(BaseCommitHelper.class);

  protected final transient HoodieEngineContext context;
  protected final HoodieWriteConfig config;
  protected final HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table;
  protected final String instantTime;
  protected final Option<Map<String, String>> extraMetadata;
  protected final WriteOperationType operationType;
  protected final TransactionManager txnManager;
  protected final TaskContextSupplier taskContextSupplier;
  protected Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxn;

  public BaseCommitHelper(
      HoodieEngineContext context, HoodieWriteConfig config,
      HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
      String instantTime, WriteOperationType operationType) {
    this(context, config, table, instantTime, operationType, Option.empty());
  }

  public BaseCommitHelper(
      HoodieEngineContext context, HoodieWriteConfig config,
      HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
      String instantTime, WriteOperationType operationType,
      Option<Map<String, String>> extraMetadata) {
    this.context = context;
    this.config = config;
    this.table = table;
    this.instantTime = instantTime;
    this.operationType = operationType;
    this.extraMetadata = extraMetadata;
    // TODO : Remove this once we refactor and move out autoCommit method from here,
    //  since the TxnManager is held in {@link AbstractHoodieWriteClient}.
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
    this.taskContextSupplier = context.getTaskContextSupplier();
    this.lastCompletedTxn = TransactionUtils.getLastCompletedTxnInstantAndMetadata(table.getMetaClient());
  }

  public void commit(
      Option<Map<String, String>> extraMetadata,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    context.setJobStatus(this.getClass().getSimpleName(), "Commit write status collect");
    commit(extraMetadata, result, context.map(result.getWriteStatuses(), WriteStatus::getStat));
  }

  public Iterator<List<WriteStatus>> handleUpdate(
      String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates

    HoodieMergeHandle upsertHandle = getUpdateHandle(partitionPath, fileId, recordItr);
    return handleUpdateInternal(upsertHandle, fileId);
  }

  /**
   * By default, return the writer schema in Write Config for storing in commit.
   */
  public String getSchemaToStoreInCommit() {
    return config.getSchema();
  }

  public abstract void init(HoodieWriteConfig config);

  public abstract HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      HoodieData<HoodieRecord<T>> inputRecords);

  public abstract Iterator<List<WriteStatus>> handleInsert(
      String idPfx, Iterator<HoodieRecord<T>> recordItr) throws Exception;

  protected abstract void syncTableMetadata();

  protected abstract HoodieMergeHandle getUpdateHandle(
      String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr);

  protected abstract BaseMergeHelper getMergeHelper();

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, String instantTime)
      throws HoodieCommitException {
    try {
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      profile.getPartitionPaths().forEach(path -> {
        WorkloadStat partitionStat = profile.getWorkloadStat(path);
        HoodieWriteStat insertStat = new HoodieWriteStat();
        insertStat.setNumInserts(partitionStat.getNumInserts());
        insertStat.setFileId("");
        insertStat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
        metadata.addWriteStat(path, insertStat);

        partitionStat.getUpdateLocationToCount().forEach((key, value) -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setFileId(key);
          // TODO : Write baseCommitTime is possible here ?
          writeStat.setPrevCommit(value.getKey());
          writeStat.setNumUpdateWrites(value.getValue());
          metadata.addWriteStat(path, writeStat);
        });
      });
      metadata.setOperationType(operationType);

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = getCommitActionType();
      HoodieInstant requested =
          new HoodieInstant(HoodieInstant.State.REQUESTED, commitActionType, instantTime);
      activeTimeline.transitionRequestedToInflight(requested,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)),
          config.shouldAllowMultiWriteOnSameInstant());
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
    }
  }

  protected String getCommitActionType() {
    return table.getMetaClient().getCommitActionType();
  }

  protected boolean isWorkloadProfileNeeded() {
    return true;
  }

  /**
   * Check if any validators are configured and run those validations.
   * If any of the validations fail, throws HoodieValidationException.
   */
  protected void runPrecommitValidators(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    if (StringUtils.isNullOrEmpty(config.getPreCommitValidators())) {
      return;
    }
    throw new HoodieIOException("Precommit validation not implemented for all engines yet");
  }

  protected void commit(
      Option<Map<String, String>> extraMetadata,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result, List<HoodieWriteStat> writeStats) {
    String actionType = getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType);
    result.setCommitted(true);
    result.setWriteStats(writeStats);
    // Finalize write
    finalizeWrite(instantTime, writeStats, result);
    syncTableMetadata();
    try {
      LOG.info("Committing " + instantTime + ", action Type " + getCommitActionType());
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      HoodieCommitMetadata metadata = CommitUtils.buildMetadata(writeStats, result.getPartitionToReplaceFileIds(),
          extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType());

      activeTimeline.saveAsComplete(new HoodieInstant(true, getCommitActionType(), instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      LOG.info("Committed " + instantTime);
      result.setCommitMetadata(Option.of(metadata));
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
  }

  public void commitOnAutoCommit(HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    // validate commit action before committing result
    runPrecommitValidators(result);
    if (config.shouldAutoCommit()) {
      LOG.info("Auto commit enabled: Committing " + instantTime);
      autoCommit(extraMetadata, result);
    } else {
      LOG.info("Auto commit disabled for " + instantTime);
    }
  }

  protected void autoCommit(
      Option<Map<String, String>> extraMetadata,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    this.txnManager.beginTransaction(Option.of(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime)),
        lastCompletedTxn.isPresent() ? Option.of(lastCompletedTxn.get().getLeft()) : Option.empty());
    try {
      TransactionUtils.resolveWriteConflictIfAny(
          table, this.txnManager.getCurrentTransactionOwner(), result.getCommitMetadata(),
          config, this.txnManager.getLastCompletedTransactionOwner());
      commit(extraMetadata, result);
    } finally {
      this.txnManager.endTransaction();
    }
  }

  /**
   * Finalize Write operation.
   *
   * @param instantTime Instant Time
   * @param stats       Hoodie Write Stat
   */
  protected void finalizeWrite(
      String instantTime, List<HoodieWriteStat> stats,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    try {
      Instant start = Instant.now();
      table.finalizeWrite(context, instantTime, stats);
      result.setFinalizeDuration(Duration.between(start, Instant.now()));
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException(
          "Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(
      HoodieMergeHandle<?, ?, ?, ?> upsertHandle, String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      getMergeHelper().runMerge(table, upsertHandle);
    }

    // TODO(vc): This needs to be revisited
    if (upsertHandle.getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.writeStatuses());
    }

    return Collections.singletonList(upsertHandle.writeStatuses()).iterator();
  }
}
