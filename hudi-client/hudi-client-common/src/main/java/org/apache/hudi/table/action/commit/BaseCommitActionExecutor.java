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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BaseCommitActionExecutor<T extends HoodieRecordPayload, I, K, O, R>
    extends BaseActionExecutor<T, I, K, O, R> {

  private static final Logger LOG = LogManager.getLogger(BaseCommitActionExecutor.class);

  protected final Option<Map<String, String>> extraMetadata;
  protected final WriteOperationType operationType;
  protected final TaskContextSupplier taskContextSupplier;
  protected final TransactionManager txnManager;
  protected Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxn;
  protected Set<String> pendingInflightAndRequestedInstants;

  public BaseCommitActionExecutor(HoodieEngineContext context, HoodieWriteConfig config,
                                  HoodieTable<T, I, K, O> table, String instantTime, WriteOperationType operationType,
                                  Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime);
    this.operationType = operationType;
    this.extraMetadata = extraMetadata;
    this.taskContextSupplier = context.getTaskContextSupplier();
    // TODO : Remove this once we refactor and move out autoCommit method from here, since the TxnManager is held in {@link BaseHoodieWriteClient}.
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
    this.lastCompletedTxn = TransactionUtils.getLastCompletedTxnInstantAndMetadata(table.getMetaClient());
    this.pendingInflightAndRequestedInstants = TransactionUtils.getInflightAndRequestedInstants(table.getMetaClient());
    this.pendingInflightAndRequestedInstants.remove(instantTime);
    if (!table.getStorageLayout().writeOperationSupported(operationType)) {
      throw new UnsupportedOperationException("Executor " + this.getClass().getSimpleName()
          + " is not compatible with table layout " + table.getStorageLayout().getClass().getSimpleName());
    }
  }

  public abstract HoodieWriteMetadata<O> execute(I inputRecords);

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
      profile.getOutputPartitionPaths().forEach(path -> {
        WorkloadStat partitionStat = profile.getOutputWorkloadStat(path);
        HoodieWriteStat insertStat = new HoodieWriteStat();
        insertStat.setNumInserts(partitionStat.getNumInserts());
        insertStat.setFileId("");
        insertStat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
        metadata.addWriteStat(path, insertStat);
        Map<String, Pair<String, Long>> updateLocationMap = partitionStat.getUpdateLocationToCount();
        Map<String, Pair<String, Long>> insertLocationMap = partitionStat.getInsertLocationToCount();
        Stream.concat(updateLocationMap.keySet().stream(), insertLocationMap.keySet().stream())
            .distinct()
            .forEach(fileId -> {
              HoodieWriteStat writeStat = new HoodieWriteStat();
              writeStat.setFileId(fileId);
              Pair<String, Long> updateLocation = updateLocationMap.get(fileId);
              Pair<String, Long> insertLocation = insertLocationMap.get(fileId);
              // TODO : Write baseCommitTime is possible here ?
              writeStat.setPrevCommit(updateLocation != null ? updateLocation.getKey() : insertLocation.getKey());
              if (updateLocation != null) {
                writeStat.setNumUpdateWrites(updateLocation.getValue());
              }
              if (insertLocation != null) {
                writeStat.setNumInserts(insertLocation.getValue());
              }
              metadata.addWriteStat(path, writeStat);
            });
      });
      metadata.setOperationType(operationType);

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = getCommitActionType();
      HoodieInstant requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime);
      activeTimeline.transitionRequestedToInflight(requested,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)),
          config.shouldAllowMultiWriteOnSameInstant());
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
    }
  }

  protected String getCommitActionType() {
    return  table.getMetaClient().getCommitActionType();
  }


  /**
   * Check if any validators are configured and run those validations. If any of the validations fail, throws HoodieValidationException.
   */
  protected void runPrecommitValidators(HoodieWriteMetadata<O> writeMetadata) {
    if (StringUtils.isNullOrEmpty(config.getPreCommitValidators())) {
      return;
    }
    throw new HoodieIOException("Precommit validation not implemented for all engines yet");
  }
  
  protected void commitOnAutoCommit(HoodieWriteMetadata result) {
    // validate commit action before committing result
    runPrecommitValidators(result);
    if (config.shouldAutoCommit()) {
      LOG.info("Auto commit enabled: Committing " + instantTime);
      autoCommit(extraMetadata, result);
    } else {
      LOG.info("Auto commit disabled for " + instantTime);
    }
  }

  protected void autoCommit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata<O> result) {
    final Option<HoodieInstant> inflightInstant = Option.of(new HoodieInstant(State.INFLIGHT,
        getCommitActionType(), instantTime));
    this.txnManager.beginTransaction(inflightInstant,
        lastCompletedTxn.isPresent() ? Option.of(lastCompletedTxn.get().getLeft()) : Option.empty());
    try {
      setCommitMetadata(result);
      // reload active timeline so as to get all updates after current transaction have started. hence setting last arg to true.
      TransactionUtils.resolveWriteConflictIfAny(table, this.txnManager.getCurrentTransactionOwner(),
          result.getCommitMetadata(), config, this.txnManager.getLastCompletedTransactionOwner(), true, pendingInflightAndRequestedInstants);
      commit(extraMetadata, result);
    } finally {
      this.txnManager.endTransaction(inflightInstant);
    }
  }

  protected abstract void setCommitMetadata(HoodieWriteMetadata<O> result);

  protected abstract void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata<O> result);

  /**
   * Finalize Write operation.
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(String instantTime, List<HoodieWriteStat> stats, HoodieWriteMetadata result) {
    try {
      Instant start = Instant.now();
      table.finalizeWrite(context, instantTime, stats);
      result.setFinalizeDuration(Duration.between(start, Instant.now()));
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  /**
   * By default, return the writer schema in Write Config for storing in commit.
   */
  protected String getSchemaToStoreInCommit() {
    return config.getSchema();
  }

  protected boolean isWorkloadProfileNeeded() {
    return true;
  }

  protected abstract Iterator<List<WriteStatus>> handleInsert(String idPfx,
      Iterator<HoodieRecord<T>> recordItr) throws Exception;

  protected abstract Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException;

  protected HoodieWriteMetadata<HoodieData<WriteStatus>> executeClustering(HoodieClusteringPlan clusteringPlan) {
    HoodieInstant instant = HoodieTimeline.getReplaceCommitRequestedInstant(instantTime);
    // Mark instant as clustering inflight
    table.getActiveTimeline().transitionReplaceRequestedToInflight(instant, Option.empty());
    table.getMetaClient().reloadActiveTimeline();

    // Disable auto commit. Strategy is only expected to write data in new files.
    config.setValue(HoodieWriteConfig.AUTO_COMMIT_ENABLE, Boolean.FALSE.toString());

    final Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = (
        (ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>>)
            ReflectionUtils.loadClass(config.getClusteringExecutionStrategyClass(),
                new Class<?>[] {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class}, table, context, config))
        .performClustering(clusteringPlan, schema, instantTime);
    HoodieData<WriteStatus> writeStatusList = writeMetadata.getWriteStatuses();
    HoodieData<WriteStatus> statuses = updateIndex(writeStatusList, writeMetadata);
    writeMetadata.setWriteStats(statuses.map(WriteStatus::getStat).collectAsList());
    writeMetadata.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(clusteringPlan, writeMetadata));
    validateWriteResult(clusteringPlan, writeMetadata);
    commitOnAutoCommit(writeMetadata);
    if (!writeMetadata.getCommitMetadata().isPresent()) {
      HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeMetadata.getWriteStats().get(), writeMetadata.getPartitionToReplaceFileIds(),
          extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType());
      writeMetadata.setCommitMetadata(Option.of(commitMetadata));
    }
    return writeMetadata;
  }

  private HoodieData<WriteStatus> updateIndex(HoodieData<WriteStatus> writeStatuses, HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    Instant indexStartTime = Instant.now();
    // Update the index back
    HoodieData<WriteStatus> statuses = table.getIndex().updateLocation(writeStatuses, context, table, instantTime);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
    return statuses;
  }

  private Map<String, List<String>> getPartitionToReplacedFileIds(HoodieClusteringPlan clusteringPlan, HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    Set<HoodieFileGroupId> newFilesWritten = writeMetadata.getWriteStats().get().stream()
        .map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getFileId())).collect(Collectors.toSet());

    return ClusteringUtils.getFileGroupsFromClusteringPlan(clusteringPlan)
        .filter(fg -> "org.apache.hudi.client.clustering.run.strategy.SparkSingleFileSortExecutionStrategy"
            .equals(config.getClusteringExecutionStrategyClass())
            || !newFilesWritten.contains(fg))
        .collect(Collectors.groupingBy(HoodieFileGroupId::getPartitionPath, Collectors.mapping(HoodieFileGroupId::getFileId, Collectors.toList())));
  }

  /**
   * Validate actions taken by clustering. In the first implementation, we validate at least one new file is written.
   * But we can extend this to add more validation. E.g. number of records read = number of records written etc.
   * We can also make these validations in BaseCommitActionExecutor to reuse pre-commit hooks for multiple actions.
   */
  private void validateWriteResult(HoodieClusteringPlan clusteringPlan, HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    if (writeMetadata.getWriteStatuses().isEmpty()) {
      throw new HoodieClusteringException("Clustering plan produced 0 WriteStatus for " + instantTime
          + " #groups: " + clusteringPlan.getInputGroups().size() + " expected at least "
          + clusteringPlan.getInputGroups().stream().mapToInt(HoodieClusteringGroup::getNumOutputFileGroups).sum()
          + " write statuses");
    }
  }

}
