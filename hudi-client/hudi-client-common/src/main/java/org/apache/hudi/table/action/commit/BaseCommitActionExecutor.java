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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.CommitMetadataResolverFactory;
import org.apache.hudi.client.HoodieColumnStatsIndexUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE;

public abstract class BaseCommitActionExecutor<T, I, K, O, R>
    extends BaseActionExecutor<T, I, K, O, R> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCommitActionExecutor.class);

  protected final Option<Map<String, String>> extraMetadata;
  protected final WriteOperationType operationType;
  protected final TaskContextSupplier taskContextSupplier;

  protected Option<TransactionManager> txnManagerOption;
  protected Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxn = Option.empty();
  protected Set<String> pendingInflightAndRequestedInstants = Collections.emptySet();

  public BaseCommitActionExecutor(HoodieEngineContext context, HoodieWriteConfig config,
                                  HoodieTable<T, I, K, O> table, String instantTime, WriteOperationType operationType,
                                  Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime);
    this.operationType = operationType;
    this.extraMetadata = extraMetadata;
    this.taskContextSupplier = context.getTaskContextSupplier();
    this.txnManagerOption = Option.empty();
    initializeLastCompletedTnxAndPendingInstants();
    if (!table.getStorageLayout().writeOperationSupported(operationType)) {
      throw new UnsupportedOperationException("Executor " + this.getClass().getSimpleName()
          + " is not compatible with table layout " + table.getStorageLayout().getClass().getSimpleName());
    }
  }

  private void initializeLastCompletedTnxAndPendingInstants() {
    if (this.txnManagerOption.isPresent() && this.txnManagerOption.get().isLockRequired()) {
      // these txn metadata are only needed for auto commit when optimistic concurrent control is also enabled
      this.lastCompletedTxn = TransactionUtils.getLastCompletedTxnInstantAndMetadata(table.getMetaClient());
      this.pendingInflightAndRequestedInstants = TransactionUtils.getInflightAndRequestedInstants(table.getMetaClient());
      this.pendingInflightAndRequestedInstants.remove(instantTime);
    } else {
      this.lastCompletedTxn = Option.empty();
      this.pendingInflightAndRequestedInstants = Collections.emptySet();
    }
  }

  public abstract HoodieWriteMetadata<O> execute(I inputRecords);

  public HoodieWriteMetadata<O> execute(I inputRecords, Option<HoodieTimer> sourceReadAndIndexTimer) {
    return this.execute(inputRecords);
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  protected void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, String instantTime)
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
      HoodieInstant requested = table.getMetaClient().createNewInstant(State.REQUESTED, commitActionType, instantTime);
      activeTimeline.transitionRequestedToInflight(requested, Option.of(metadata), config.shouldAllowMultiWriteOnSameInstant());
    } catch (HoodieIOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
    }
  }

  protected String getCommitActionType() {
    return table.getMetaClient().getCommitActionType();
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

  protected void completeCommit(HoodieWriteMetadata result) {
    if (!this.txnManagerOption.isPresent()) {
      this.txnManagerOption = Option.of(new TransactionManager(config, table.getStorage()));
      initializeLastCompletedTnxAndPendingInstants();
    }
    autoCommit(result);
    LOG.info("Completed commit for " + instantTime);
  }

  protected void autoCommit(HoodieWriteMetadata<O> result) {
    InstantGenerator factory = table.getMetaClient().getInstantGenerator();
    final Option<HoodieInstant> inflightInstant = Option.of(factory.createNewInstant(State.INFLIGHT,
        getCommitActionType(), instantTime));
    ValidationUtils.checkState(this.txnManagerOption.isPresent(), "The transaction manager has not been initialized");
    TransactionManager txnManager = this.txnManagerOption.get();
    txnManager.beginStateChange(inflightInstant,
        lastCompletedTxn.isPresent() ? Option.of(lastCompletedTxn.get().getLeft()) : Option.empty());
    try {
      setCommitMetadata(result);
      // table instance is created outside the transaction boundary so setting `timelineRefreshedWithinTransaction` to false below
      TransactionUtils.resolveWriteConflictIfAny(table, txnManager.getCurrentTransactionOwner(),
          result.getCommitMetadata(), config, txnManager.getLastCompletedTransactionOwner(), false, pendingInflightAndRequestedInstants);
      commit(result);
    } finally {
      txnManager.endStateChange(inflightInstant);
    }
  }

  protected abstract void setCommitMetadata(HoodieWriteMetadata<O> result);

  protected abstract void commit(HoodieWriteMetadata<O> result);

  protected void commit(HoodieWriteMetadata<O> result, List<HoodieWriteStat> writeStats) {
    String actionType = getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType + ", operation Type " + operationType);
    result.setCommitted(true);
    result.setWriteStats(writeStats);
    // Finalize write
    finalizeWrite(instantTime, writeStats, result);
    try {
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      HoodieCommitMetadata metadata = CommitMetadataResolverFactory.get(
              table.getMetaClient().getTableConfig().getTableVersion(), config.getEngineType(),
              table.getMetaClient().getTableType(), getCommitActionType())
          .reconcileMetadataForMissingFiles(
              config, context, table, instantTime, result.getCommitMetadata().get());
      writeTableMetadata(metadata, actionType);
      // cannot serialize maps with null values
      metadata.getExtraMetadata().entrySet().removeIf(entry -> entry.getValue() == null);
      activeTimeline.saveAsComplete(false,
          table.getMetaClient().createNewInstant(State.INFLIGHT, actionType, instantTime), Option.of(metadata),
          completedInstant -> table.getMetaClient().getTableFormat().commit(metadata, completedInstant, table.getContext(), table.getMetaClient(), table.getViewManager()));
      LOG.info("Committed " + instantTime);
      result.setCommitMetadata(Option.of(metadata));
      // update cols to Index as applicable
      try {
          HoodieColumnStatsIndexUtils.updateColsToIndex(table, config, metadata, actionType,
                  (Functions.Function2<HoodieTableMetaClient, List<String>, Void>) (metaClient, columnsToIndex) -> {
                      updateColumnsToIndexForColumnStats(metaClient, columnsToIndex);
                      return null;
                  });
      } catch (UnsupportedOperationException uoe) {
        LOG.warn("Failed to update col stats, bootstrap doesn't support col stats", uoe);
      }

    } catch (HoodieIOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
  }

  /**
   * Updates the list of columns indexed with col stats index in Metadata table.
   * @param metaClient instance of {@link HoodieTableMetaClient} of interest.
   * @param columnsToIndex list of columns to index.
   */
  protected abstract void updateColumnsToIndexForColumnStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex);

  /**
   * Finalize Write operation.
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(String instantTime, List<HoodieWriteStat> stats, HoodieWriteMetadata<O> result) {
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

  protected abstract Iterator<List<WriteStatus>> handleInsert(String idPfx,
                                                              Iterator<HoodieRecord<T>> recordItr) throws Exception;

  protected abstract Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
                                                              Iterator<HoodieRecord<T>> recordItr) throws IOException;

  protected HoodieWriteMetadata<HoodieData<WriteStatus>> executeClustering(HoodieClusteringPlan clusteringPlan) {
    context.setJobStatus(this.getClass().getSimpleName(), "Clustering records for " + config.getTableName());
    HoodieInstant instant = ClusteringUtils.getRequestedClusteringInstant(instantTime, table.getActiveTimeline(),
        table.getMetaClient().getInstantGenerator()).get();
    // Mark instant as clustering inflight
    ClusteringUtils.transitionClusteringOrReplaceRequestedToInflight(instant, Option.empty(), table.getActiveTimeline());
    table.getMetaClient().reloadActiveTimeline();

    Option<Schema> schema;
    try {
      schema = new TableSchemaResolver(table.getMetaClient()).getTableAvroSchemaIfPresent(false);
    } catch (Exception ex) {
      throw new HoodieSchemaException(ex);
    }
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = (
        (ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>>)
            ReflectionUtils.loadClass(config.getClusteringExecutionStrategyClass(),
                new Class<?>[] {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class}, table, context, config))
        .performClustering(clusteringPlan, schema.get(), instantTime);
    HoodieData<WriteStatus> writeStatusList = writeMetadata.getWriteStatuses();
    HoodieData<WriteStatus> statuses = updateIndex(writeStatusList, writeMetadata);
    statuses.persist(config.getString(WRITE_STATUS_STORAGE_LEVEL_VALUE), context, HoodieData.HoodieDataCacheKey.of(config.getBasePath(), instantTime));

    writeMetadata.setWriteStatuses(statuses);

    LOG.debug("Create place holder commit metadata for clustering with instant time " + instantTime);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(),
        extraMetadata, operationType, schema.get().toString(), getCommitActionType());
    writeMetadata.setCommitMetadata(Option.of(commitMetadata));

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
