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

package org.apache.hudi.client;

import org.apache.hudi.async.AsyncArchiveService;
import org.apache.hudi.async.AsyncCleanerService;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HeartbeatUtils;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.TimelineArchivers;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLogCompactException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.action.rollback.RollbackUtils;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.CommonClientUtils;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.metadata.HoodieTableMetadata.isMetadataTable;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.isIndexingCommit;

/**
 * Base class for all shared logic between table service clients regardless of engine.
 * @param <I> The {@link HoodieTable} implementation's input type
 * @param <T> The {@link HoodieTable} implementation's output type
 * @param <O> The {@link BaseHoodieWriteClient} implementation's output type (differs in case of spark)
 */
public abstract class BaseHoodieTableServiceClient<I, T, O> extends BaseHoodieClient implements RunsTableService {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieTableServiceClient.class);

  protected transient Timer.Context compactionTimer;
  protected transient Timer.Context clusteringTimer;
  protected transient Timer.Context logCompactionTimer;

  protected transient AsyncCleanerService asyncCleanerService;
  protected transient AsyncArchiveService asyncArchiveService;

  protected Set<String> pendingInflightAndRequestedInstants;

  protected BaseHoodieTableServiceClient(HoodieEngineContext context,
                                         HoodieWriteConfig clientConfig,
                                         Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  protected void startAsyncCleanerService(BaseHoodieWriteClient writeClient) {
    if (this.asyncCleanerService == null) {
      this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(writeClient);
    } else {
      this.asyncCleanerService.start(null);
    }
  }

  protected void startAsyncArchiveService(BaseHoodieWriteClient writeClient) {
    if (this.asyncArchiveService == null) {
      this.asyncArchiveService = AsyncArchiveService.startAsyncArchiveIfEnabled(writeClient);
    } else {
      this.asyncArchiveService.start(null);
    }
  }

  protected void asyncClean() {
    AsyncCleanerService.waitForCompletion(asyncCleanerService);
  }

  protected void asyncArchive() {
    AsyncArchiveService.waitForCompletion(asyncArchiveService);
  }

  protected void setTableServiceTimer(WriteOperationType operationType) {
    switch (operationType) {
      case CLUSTER:
        clusteringTimer = metrics.getClusteringCtx();
        break;
      case COMPACT:
        compactionTimer = metrics.getCompactionCtx();
        break;
      case LOG_COMPACT:
        logCompactionTimer = metrics.getLogCompactionCtx();
        break;
      default:
    }
  }

  protected void setPendingInflightAndRequestedInstants(Set<String> pendingInflightAndRequestedInstants) {
    this.pendingInflightAndRequestedInstants = pendingInflightAndRequestedInstants;
  }

  /**
   * Any pre-commit actions like conflict resolution goes here.
   *
   * @param metadata commit metadata for which pre commit is being invoked.
   */
  protected void preCommit(HoodieCommitMetadata metadata) {
    // Create a Hoodie table after startTxn which encapsulated the commits and files visible.
    // Important to create this after the lock to ensure the latest commits show up in the timeline without need for reload
    HoodieTable table = createTable(config, storageConf);
    resolveWriteConflict(table, metadata, this.pendingInflightAndRequestedInstants);
  }

  /**
   * Performs a compaction operation on a table, serially before or after an insert/upsert action.
   * Scheduling and execution is done inline.
   */
  protected Option<String> inlineCompaction(Option<Map<String, String>> extraMetadata) {
    Option<String> compactionInstantTimeOpt = inlineScheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactInstantTime -> {
      // inline compaction should auto commit as the user is never given control
      compact(compactInstantTime, true);
    });
    return compactionInstantTimeOpt;
  }

  private void inlineCompaction(HoodieTable table, Option<Map<String, String>> extraMetadata) {
    if (shouldDelegateToTableServiceManager(config, ActionType.compaction)) {
      scheduleCompaction(extraMetadata);
    } else {
      runAnyPendingCompactions(table);
      inlineCompaction(extraMetadata);
    }
  }

  /**
   * Ensures compaction instant is in expected state and performs Log Compaction for the workload stored in instant-time.s
   *
   * @param logCompactionInstantTime Compaction Instant Time
   * @return Collection of Write Status
   */
  protected HoodieWriteMetadata<O> logCompact(String logCompactionInstantTime, boolean shouldComplete) {
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf());

    // Check if a commit or compaction instant with a greater timestamp is on the timeline.
    // If an instant is found then abort log compaction, since it is no longer needed.
    Set<String> actions = CollectionUtils.createSet(COMMIT_ACTION, COMPACTION_ACTION);
    Option<HoodieInstant> compactionInstantWithGreaterTimestamp =
        Option.fromJavaOptional(table.getActiveTimeline().getInstantsAsStream()
            .filter(hoodieInstant -> actions.contains(hoodieInstant.getAction()))
            .filter(hoodieInstant -> compareTimestamps(hoodieInstant.requestedTime(),
                GREATER_THAN, logCompactionInstantTime))
            .findFirst());
    if (compactionInstantWithGreaterTimestamp.isPresent()) {
      throw new HoodieLogCompactException(String.format("Cannot log compact since a compaction instant with greater "
          + "timestamp exists. Instant details %s", compactionInstantWithGreaterTimestamp.get()));
    }

    HoodieTimeline pendingLogCompactionTimeline = table.getActiveTimeline().filterPendingLogCompactionTimeline();
    InstantGenerator instantGenerator = table.getMetaClient().getInstantGenerator();
    HoodieInstant inflightInstant = instantGenerator.getLogCompactionInflightInstant(logCompactionInstantTime);
    if (pendingLogCompactionTimeline.containsInstant(inflightInstant)) {
      LOG.info("Found Log compaction inflight file. Rolling back the commit and exiting.");
      table.rollbackInflightLogCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false), txnManager);
      table.getMetaClient().reloadActiveTimeline();
      throw new HoodieException("Execution is aborted since it found an Inflight logcompaction,"
          + "log compaction plans are mutable plans, so reschedule another logcompaction.");
    }
    logCompactionTimer = metrics.getLogCompactionCtx();
    WriteMarkersFactory.get(config.getMarkersType(), table, logCompactionInstantTime);
    HoodieWriteMetadata<T> writeMetadata = table.logCompact(context, logCompactionInstantTime);
    HoodieWriteMetadata<T> updatedWriteMetadata = partialUpdateTableMetadata(table, writeMetadata, logCompactionInstantTime);
    HoodieWriteMetadata<O> logCompactionMetadata = convertToOutputMetadata(updatedWriteMetadata);
    if (shouldComplete) {
      commitLogCompaction(logCompactionInstantTime, logCompactionMetadata, Option.of(table));
    }
    return logCompactionMetadata;
  }

  /**
   * Performs a log compaction operation on a table, serially before or after an insert/upsert action.
   */
  protected Option<String> inlineLogCompact(Option<Map<String, String>> extraMetadata) {
    Option<String> logCompactionInstantTimeOpt = scheduleLogCompaction(extraMetadata);
    logCompactionInstantTimeOpt.ifPresent(logCompactInstantTime -> {
      // inline log compaction should auto commit as the user is never given control
      logCompact(logCompactInstantTime, true);
    });
    return logCompactionInstantTimeOpt;
  }

  protected void runAnyPendingCompactions(HoodieTable table) {
    table.getActiveTimeline().getWriteTimeline().filterPendingCompactionTimeline().getInstants()
        .forEach(instant -> {
          LOG.info("Running previously failed inflight compaction at instant {}", instant);
          compact(instant.requestedTime(), true);
        });
  }

  protected void runAnyPendingLogCompactions(HoodieTable table) {
    table.getActiveTimeline().getWriteTimeline().filterPendingLogCompactionTimeline().getInstantsAsStream()
        .forEach(instant -> {
          LOG.info("Running previously failed inflight log compaction at instant {}", instant);
          logCompact(instant.requestedTime(), true);
        });
  }

  /**
   * Schedules compaction inline.
   *
   * @param extraMetadata extra metadata to be used.
   * @return compaction instant if scheduled.
   */
  protected Option<String> inlineScheduleCompaction(Option<Map<String, String>> extraMetadata) {
    return scheduleCompaction(extraMetadata);
  }

  /**
   * Schedules a new compaction instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(extraMetadata, TableServiceType.COMPACT);
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of Write Status
   */
  protected HoodieWriteMetadata<O> compact(String compactionInstantTime, boolean shouldComplete) {
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf());
    return compact(table, compactionInstantTime, shouldComplete);
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param table                 existing table instance
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of Write Status
   */
  protected HoodieWriteMetadata<O> compact(HoodieTable<?, I, ?, T> table, String compactionInstantTime, boolean shouldComplete) {
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    InstantGenerator instantGenerator = table.getMetaClient().getInstantGenerator();
    HoodieInstant inflightInstant = instantGenerator.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false), txnManager);
      table.getMetaClient().reloadActiveTimeline();
    }
    compactionTimer = metrics.getCompactionCtx();
    HoodieWriteMetadata<T> writeMetadata = table.compact(context, compactionInstantTime);
    HoodieWriteMetadata<T> updatedWriteMetadata = partialUpdateTableMetadata(table, writeMetadata, compactionInstantTime);
    HoodieWriteMetadata<O> compactionWriteMetadata = convertToOutputMetadata(updatedWriteMetadata);
    if (shouldComplete) {
      commitCompaction(compactionInstantTime, compactionWriteMetadata, Option.of(table));
    }
    return compactionWriteMetadata;
  }

  /**
   * Partially update the table metadata if the streaming writes is enabled.
   *
   * @return The passed in {@code HoodieWriteMetadata} with probable partially updated write statuses.
   */
  protected HoodieWriteMetadata<T> partialUpdateTableMetadata(HoodieTable table, HoodieWriteMetadata<T> writeMetadata, String instantTime) {
    return writeMetadata;
  }

  public void commitCompaction(String compactionInstantTime, HoodieWriteMetadata<O> compactionWriteMetadata, Option<HoodieTable> tableOpt) {
    // de-referencing the write dag for compaction for the first time.
    TableWriteStats tableWriteStats = triggerWritesAndFetchWriteStats(compactionWriteMetadata);
    // Fetch commit metadata from HoodieWriteMetadata and update HoodieWriteStat
    CommonClientUtils.stitchCompactionHoodieWriteStats(compactionWriteMetadata, tableWriteStats.getDataTableWriteStats());
    metrics.emitCompactionCompleted();

    HoodieTable table = tableOpt.orElseGet(() -> createTable(config, context.getStorageConf()));
    completeCompaction(compactionWriteMetadata.getCommitMetadata().get(), table, compactionInstantTime, tableWriteStats.getMetadataTableWriteStats());
  }

  /**
   * The API triggers the data write and fetches the corresponding write-stats using the write metadata.
   *
   * <p>When streaming writes to metadata table is enabled, writes to metadata table is expected to be triggered here
   * and the list of {@link HoodieWriteStat} are returned as part of this call.
   *
   * @return instance of {@link TableWriteStats} to hold stats for the writes.
   */
  protected abstract TableWriteStats triggerWritesAndFetchWriteStats(HoodieWriteMetadata<O> writeMetadata);

  /**
   * Commit Compaction and track metrics.
   */
  protected void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime, List<HoodieWriteStat> partialMetadataWriteStats) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction: " + config.getTableName());
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    handleWriteErrors(writeStats, TableServiceType.COMPACT);
    InstantGenerator instantGenerator = table.getMetaClient().getInstantGenerator();
    final HoodieInstant compactionInstant = instantGenerator.getCompactionInflightInstant(compactionCommitTime);
    try {
      this.txnManager.beginStateChange(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      writeToMetadataTable(table, compactionCommitTime, metadata, partialMetadataWriteStats);
      LOG.info("Committing Compaction {}", compactionCommitTime);
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionCommitTime, metadata, txnManager.createCompletionInstant());
      LOG.debug("Compaction {} finished with result: {}", compactionCommitTime, metadata);
    } finally {
      this.txnManager.endStateChange(Option.of(compactionInstant));
      releaseResources(compactionCommitTime);
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, compactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      TimelineUtils.parseDateFromInstantTimeSafely(compactionCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, COMPACTION_ACTION)
      );
    }
    LOG.info("Compacted successfully on commit {}", compactionCommitTime);
  }

  protected void writeToMetadataTable(HoodieTable table, String instantTime, HoodieCommitMetadata metadata, List<HoodieWriteStat> partialMetadataWriteStats) {
    // legacy write DAG to metadata table.
    writeTableMetadata(table, instantTime, metadata);
  }

  public void commitLogCompaction(String compactionInstantTime, HoodieWriteMetadata<O> writeMetadata, Option<HoodieTable> tableOpt) {
    // dereferencing the write dag for log compaction for the first time.
    TableWriteStats tableWriteStats = triggerWritesAndFetchWriteStats(writeMetadata);
    // fetch HoodieCommitMetadata and update HoodieWriteStat
    CommonClientUtils.stitchCompactionHoodieWriteStats(writeMetadata, tableWriteStats.getDataTableWriteStats());
    metrics.emitCompactionCompleted();
    HoodieTable table = tableOpt.orElseGet(() -> createTable(config, context.getStorageConf()));
    completeLogCompaction(writeMetadata.getCommitMetadata().get(), table, compactionInstantTime, tableWriteStats.getMetadataTableWriteStats());
  }

  /**
   * Schedules a new log compaction instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleLogCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(extraMetadata, TableServiceType.LOG_COMPACT);
  }

  /**
   * Performs Log Compaction for the workload stored in instant-time.
   *
   * @param logCompactionInstantTime Log Compaction Instant Time
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public HoodieWriteMetadata<O> logCompact(String logCompactionInstantTime) {
    return logCompact(logCompactionInstantTime, false);
  }

  /**
   * Commit Log Compaction and track metrics.
   */
  protected void completeLogCompaction(HoodieCommitMetadata metadata, HoodieTable table, String logCompactionCommitTime, List<HoodieWriteStat> partialMetadataWriteStats) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect log compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    handleWriteErrors(writeStats, TableServiceType.LOG_COMPACT);
    final HoodieInstant logCompactionInstant = table.getMetaClient().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION,
        logCompactionCommitTime);
    try {
      this.txnManager.beginStateChange(Option.of(logCompactionInstant), Option.empty());
      preCommit(metadata);
      finalizeWrite(table, logCompactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      writeToMetadataTable(table, logCompactionCommitTime, metadata, partialMetadataWriteStats);
      LOG.info("Committing Log Compaction {}", logCompactionCommitTime);
      CompactHelpers.getInstance().completeInflightLogCompaction(table, logCompactionCommitTime, metadata, txnManager.createCompletionInstant());
      LOG.debug("Log Compaction {} finished with result {}", logCompactionCommitTime, metadata);
    } finally {
      this.txnManager.endStateChange(Option.of(logCompactionInstant));
      releaseResources(logCompactionCommitTime);
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, logCompactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (logCompactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(logCompactionTimer.stop());
      TimelineUtils.parseDateFromInstantTimeSafely(logCompactionCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, HoodieActiveTimeline.LOG_COMPACTION_ACTION)
      );
    }
    LOG.info("Log Compacted successfully on commit {}", logCompactionCommitTime);
  }

  /**
   * Schedules a new clustering instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleClustering(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(extraMetadata, TableServiceType.CLUSTER);
  }

  /**
   * Ensures clustering instant is in expected state and performs clustering for the plan stored in metadata.
   *
   * @param clusteringInstant Clustering Instant Time
   * @return Collection of Write Status
   */
  public HoodieWriteMetadata<O> cluster(String clusteringInstant, boolean shouldComplete) {
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf());
    HoodieTimeline pendingClusteringTimeline = table.getActiveTimeline().filterPendingReplaceOrClusteringTimeline();
    Option<HoodieInstant> inflightInstantOpt = ClusteringUtils.getInflightClusteringInstant(clusteringInstant, table.getActiveTimeline(),
        table.getMetaClient().getInstantGenerator());
    if (inflightInstantOpt.isPresent()) {
      if (pendingClusteringTimeline.isPendingClusteringInstant(inflightInstantOpt.get().requestedTime())) {
        table.rollbackInflightClustering(inflightInstantOpt.get(), commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false), txnManager);
        table.getMetaClient().reloadActiveTimeline();
      } else {
        throw new HoodieClusteringException("Non clustering replace-commit inflight at timestamp " + clusteringInstant);
      }
    }

    clusteringTimer = metrics.getClusteringCtx();
    LOG.info("Starting clustering at {} for table {}", clusteringInstant, table.getConfig().getBasePath());
    HoodieWriteMetadata<T> writeMetadata = table.cluster(context, clusteringInstant);
    HoodieWriteMetadata<T> updatedWriteMetadata = partialUpdateTableMetadata(table, writeMetadata, clusteringInstant);
    HoodieWriteMetadata<O> clusteringMetadata = convertToOutputMetadata(updatedWriteMetadata);

    // TODO : Where is shouldComplete used ?
    if (shouldComplete) {
      commitClustering(clusteringMetadata, table, clusteringInstant);
    }
    return clusteringMetadata;
  }

  public boolean purgePendingClustering(String clusteringInstant) {
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf());
    Option<HoodieInstant> inflightInstantOpt = ClusteringUtils.getInflightClusteringInstant(clusteringInstant, table.getActiveTimeline(),
        table.getMetaClient().getInstantGenerator());
    if (inflightInstantOpt.isPresent()) {
      table.rollbackInflightClustering(inflightInstantOpt.get(), commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false), true, txnManager);
      table.getMetaClient().reloadActiveTimeline();
      return true;
    }
    return false;
  }

  /**
   * The API changes the input write metadata from type T to O.
   */
  protected abstract HoodieWriteMetadata<O> convertToOutputMetadata(HoodieWriteMetadata<T> writeMetadata);

  /**
   * Check if any validators are configured and run those validations. If any of the validations fail, throws HoodieValidationException.
   */
  protected void runPrecommitValidationForClustering(HoodieWriteMetadata<O> writeMetadata, HoodieTable table, String instantTime) {
    if (StringUtils.isNullOrEmpty(config.getPreCommitValidators())) {
      return;
    }
    throw new HoodieIOException("Precommit validation not implemented for all engines yet");
  }

  private void commitClustering(HoodieWriteMetadata<O> clusteringWriteMetadata, HoodieTable table, String clusteringCommitTime) {
    // triggering the dag for the first time for clustering
    TableWriteStats tableWriteStats = triggerWritesAndFetchWriteStats(clusteringWriteMetadata);
    clusteringWriteMetadata.setWriteStats(tableWriteStats.getDataTableWriteStats());
    // Fetch Replace commit metadata and update HoodieWriteStats annd Partition to Replace FileIds
    HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) clusteringWriteMetadata.getCommitMetadata().get();
    for (HoodieWriteStat writeStat: tableWriteStats.getDataTableWriteStats()) {
      replaceCommitMetadata.addWriteStat(writeStat.getPartitionPath(), writeStat);
    }
    HoodieClusteringPlan clusteringPlan = ClusteringUtils.getPendingClusteringPlan(table.getMetaClient(), clusteringCommitTime);
    Map<String, List<String>> partitionToReplaceFileIds = CommonClientUtils.getPartitionToReplacedFileIds(clusteringPlan, clusteringWriteMetadata, config);
    clusteringWriteMetadata.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
    replaceCommitMetadata.setPartitionToReplaceFileIds(partitionToReplaceFileIds);

    runPrecommitValidationForClustering(clusteringWriteMetadata, table, clusteringCommitTime);
    // Publish file creation metrics for clustering.
    if (config.isMetricsOn()) {
      clusteringWriteMetadata.getWriteStats()
          .ifPresent(hoodieWriteStats -> hoodieWriteStats.stream()
              .filter(hoodieWriteStat -> hoodieWriteStat.getRuntimeStats() != null)
              .map(hoodieWriteStat -> hoodieWriteStat.getRuntimeStats().getTotalCreateTime())
              .forEach(metrics::updateClusteringFileCreationMetrics));
    }

    completeClustering(replaceCommitMetadata, tableWriteStats.getDataTableWriteStats(), table, clusteringCommitTime,
        tableWriteStats.getMetadataTableWriteStats());
  }

  private void completeClustering(HoodieReplaceCommitMetadata replaceCommitMetadata,
                                  List<HoodieWriteStat> writeStats,
                                  HoodieTable table,
                                  String clusteringCommitTime,
                                  List<HoodieWriteStat> partialMetadataWriteStats) {

    handleWriteErrors(writeStats, TableServiceType.CLUSTER);
    final HoodieInstant clusteringInstant = ClusteringUtils.getInflightClusteringInstant(clusteringCommitTime,
        table.getActiveTimeline(), table.getMetaClient().getInstantGenerator()).get();
    try {
      this.txnManager.beginStateChange(Option.of(clusteringInstant), Option.empty());

      finalizeWrite(table, clusteringCommitTime, writeStats);
      // Only in some cases conflict resolution needs to be performed.
      // So, check if preCommit method that does conflict resolution needs to be triggered.
      if (isPreCommitRequired()) {
        preCommit(replaceCommitMetadata);
      }
      // Update table's metadata (table)
      writeToMetadataTable(table, clusteringInstant.requestedTime(), replaceCommitMetadata, partialMetadataWriteStats);

      LOG.info("Committing Clustering {} for table {}", clusteringCommitTime, table.getConfig().getBasePath());

      ClusteringUtils.transitionClusteringOrReplaceInflightToComplete(clusteringInstant, replaceCommitMetadata, table.getActiveTimeline(), txnManager.createCompletionInstant(), completedInstant -> table.getMetaClient().getTableFormat().commit(replaceCommitMetadata, completedInstant, table.getContext(), table.getMetaClient(), table.getViewManager()));
      LOG.debug("Clustering {} finished with result {}", clusteringCommitTime, replaceCommitMetadata);
    } catch (Exception e) {
      throw new HoodieClusteringException("unable to transition clustering inflight to complete: " + clusteringCommitTime, e);
    } finally {
      this.txnManager.endStateChange(Option.of(clusteringInstant));
      releaseResources(clusteringCommitTime);
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, clusteringCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (clusteringTimer != null) {
      long durationInMs = metrics.getDurationInMs(clusteringTimer.stop());
      TimelineUtils.parseDateFromInstantTimeSafely(clusteringCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, replaceCommitMetadata, HoodieActiveTimeline.CLUSTERING_ACTION)
      );
    }
    LOG.info("Clustering successfully on commit {} for table {}", clusteringCommitTime, table.getConfig().getBasePath());
  }

  protected void runTableServicesInline(HoodieTable table, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    if (!tableServicesEnabled(config)) {
      return;
    }

    if (!config.areAnyTableServicesExecutedInline() && !config.areAnyTableServicesScheduledInline()) {
      return;
    }

    // Do an inline compaction if enabled
    if (config.inlineCompactionEnabled()) {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
      inlineCompaction(table, extraMetadata);
    } else {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT.key(), "false");
    }

    // if just inline schedule is enabled
    if (!config.inlineCompactionEnabled() && config.scheduleInlineCompaction()
        && table.getActiveTimeline().getWriteTimeline().filterPendingCompactionTimeline().empty()) {
      // proceed only if there are no pending compactions
      metadata.addMetadata(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key(), "true");
      inlineScheduleCompaction(extraMetadata);
    }

    // Do an inline log compaction if enabled
    if (config.inlineLogCompactionEnabled()) {
      runAnyPendingLogCompactions(table);
      metadata.addMetadata(HoodieCompactionConfig.INLINE_LOG_COMPACT.key(), "true");
      inlineLogCompact(extraMetadata);
    } else {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_LOG_COMPACT.key(), "false");
    }

    // Do an inline clustering if enabled
    if (config.inlineClusteringEnabled()) {
      metadata.addMetadata(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
      inlineClustering(table, extraMetadata);
    } else {
      metadata.addMetadata(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "false");
    }

    // if just inline schedule is enabled
    if (!config.inlineClusteringEnabled() && config.scheduleInlineClustering()
        && !table.getActiveTimeline().getLastPendingClusterInstant().isPresent()) {
      // proceed only if there are no pending clustering
      metadata.addMetadata(HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING.key(), "true");
      inlineScheduleClustering(extraMetadata);
    }

    //  Do an inline partition ttl management if enabled
    if (config.isInlinePartitionTTLEnable()) {
      String instantTime = startDeletePartitionCommit(table.getMetaClient()).requestedTime();
      table.managePartitionTTL(table.getContext(), instantTime);
    }
  }

  /**
   * Schedule table services such as clustering, compaction & cleaning.
   *
   * @param extraMetadata    Metadata to pass onto the scheduled service instant
   * @param tableServiceType Type of table service to schedule
   * @return the requested instant time if the service was scheduled
   */
  public Option<String> scheduleTableService(Option<Map<String, String>> extraMetadata,
                                             TableServiceType tableServiceType) {
    return scheduleTableServiceInternal(Option.empty(), extraMetadata, tableServiceType);
  }

  Option<String> scheduleTableServiceInternal(Option<String> providedInstantTime, Option<Map<String, String>> extraMetadata,
                                              TableServiceType tableServiceType) {
    if (!tableServicesEnabled(config)) {
      return Option.empty();
    }
    if (tableServiceType == TableServiceType.ARCHIVE) {
      LOG.info("Archival does not need scheduling. Skipping.");
      return Option.empty();
    }
    if (tableServiceType == TableServiceType.CLEAN) {
      // Cleaning is a frequent operation that does not conflict with other operations and is idempotent,
      // so it is handled differently to avoid locking for planning.
      return scheduleCleaning(createTable(config, storageConf), providedInstantTime);
    }
    return txnManager.executeStateChangeWithInstant(providedInstantTime, instantTime -> {
      Option<String> option;
      HoodieTable<?, ?, ?, ?> table = createTable(config, storageConf);

      switch (tableServiceType) {
        case CLUSTER:
          LOG.info("Scheduling clustering at instant time: {} for table {}", instantTime, config.getBasePath());
          Option<HoodieClusteringPlan> clusteringPlan = table
              .scheduleClustering(context, instantTime, extraMetadata);
          option = clusteringPlan.map(plan -> instantTime);
          break;
        case COMPACT:
          LOG.info("Scheduling compaction at instant time: {} for table {}", instantTime, config.getBasePath());
          Option<HoodieCompactionPlan> compactionPlan = table
              .scheduleCompaction(context, instantTime, extraMetadata);
          option = compactionPlan.map(plan -> instantTime);
          break;
        case LOG_COMPACT:
          LOG.info("Scheduling log compaction at instant time: {} for table {}", instantTime, config.getBasePath());
          Option<HoodieCompactionPlan> logCompactionPlan = table
              .scheduleLogCompaction(context, instantTime, extraMetadata);
          option = logCompactionPlan.map(plan -> instantTime);
          break;
        default:
          throw new IllegalArgumentException("Invalid TableService " + tableServiceType);
      }

      Option<String> instantRange = delegateToTableServiceManager(tableServiceType, table);
      if (instantRange.isPresent()) {
        LOG.info("Delegate instant [{}] to table service manager", instantRange.get());
      }

      return option;
    });
  }

  HoodieInstant startDeletePartitionCommit(HoodieTableMetaClient metaClient) {
    return txnManager.executeStateChangeWithInstant(instantTime -> {
      HoodieInstant dropPartitionsInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime);
      HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
          .setOperationType(WriteOperationType.DELETE_PARTITION.name()).setExtraMetadata(Collections.emptyMap()).build();
      metaClient.getActiveTimeline().saveToPendingReplaceCommit(dropPartitionsInstant, requestedReplaceMetadata);
      return dropPartitionsInstant;
    });
  }

  protected HoodieTable createTableAndValidate(HoodieWriteConfig config,
                                               BiFunction<HoodieWriteConfig,
                                                   HoodieEngineContext, HoodieTable> createTableFn,
                                               boolean skipValidation) {
    HoodieTable table = createTableFn.apply(config, context);
    if (!skipValidation) {
      CommonClientUtils.validateTableVersion(table.getMetaClient().getTableConfig(), config);
    }
    return table;
  }

  protected HoodieTable<?, I, ?, T> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf) {
    return createTable(config, storageConf, false);
  }

  protected abstract HoodieTable<?, I, ?, T> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation);

  /**
   * Executes a clustering plan on a table, serially before or after an insert/upsert action.
   * Schedules and executes clustering inline.
   */
  protected Option<String> inlineClustering(Option<Map<String, String>> extraMetadata) {
    Option<String> clusteringInstantOpt = inlineScheduleClustering(extraMetadata);
    clusteringInstantOpt.ifPresent(clusteringInstant -> {
      // inline cluster should auto commit as the user is never given control
      cluster(clusteringInstant, true);
    });
    return clusteringInstantOpt;
  }

  private void inlineClustering(HoodieTable table, Option<Map<String, String>> extraMetadata) {
    if (shouldDelegateToTableServiceManager(config, ActionType.clustering)) {
      scheduleClustering(extraMetadata);
    } else {
      runAnyPendingClustering(table);
      inlineClustering(extraMetadata);
    }
  }

  /**
   * Schedules clustering inline.
   *
   * @param extraMetadata extra metadata to use.
   * @return clustering instant if scheduled.
   */
  protected Option<String> inlineScheduleClustering(Option<Map<String, String>> extraMetadata) {
    return scheduleClustering(extraMetadata);
  }

  protected void runAnyPendingClustering(HoodieTable table) {
    table.getActiveTimeline().filterPendingReplaceOrClusteringTimeline().getInstants().forEach(instant -> {
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlan = ClusteringUtils.getClusteringPlan(table.getMetaClient(), instant);
      if (instantPlan.isPresent()) {
        LOG.info("Running pending clustering at instant {} for table {}", instantPlan.get().getLeft(), config.getBasePath());
        cluster(instant.requestedTime(), true);
      }
    });
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned). This API provides the flexibility to schedule clean instant asynchronously via
   * {@link BaseHoodieTableServiceClient#scheduleTableService(Option, TableServiceType)} and disable inline scheduling
   * of clean.
   *
   * @param scheduleInline   true if needs to be scheduled inline. false otherwise.
   */
  @Nullable
  HoodieCleanMetadata clean(Option<String> suppliedCleanInstant, boolean scheduleInline) throws HoodieIOException {
    if (!tableServicesEnabled(config)) {
      return null;
    }
    final Timer.Context timerContext = metrics.getCleanCtx();
    HoodieTable initialTable = createTable(config, storageConf);
    HoodieTable table;
    if (CleanerUtils.rollbackFailedWrites(config.getFailedWritesCleanPolicy(),
        HoodieTimeline.CLEAN_ACTION, () -> rollbackFailedWrites(initialTable.getMetaClient()))) {
      // if rollback occurred, reload the table
      table = createTable(config, storageConf);
    } else {
      table = initialTable;
    }
    Option<String> inflightClean = table.getActiveTimeline().getCleanerTimeline().filterInflightsAndRequested().firstInstant().map(HoodieInstant::requestedTime);
    Option<String> cleanInstantTime = Option.empty();
    if (config.allowMultipleCleans() || inflightClean.isEmpty()) {
      LOG.info("Cleaner started for table {}", config.getBasePath());
      // proceed only if multiple clean schedules are enabled or if there are no pending cleans.
      if (scheduleInline) {
        cleanInstantTime = scheduleCleaning(table, suppliedCleanInstant);
      }

      if (shouldDelegateToTableServiceManager(config, ActionType.clean)) {
        LOG.warn("Cleaning is not yet supported with Table Service Manager.");
        return null;
      }
    }

    if (inflightClean.isPresent() || cleanInstantTime.isPresent()) {
      table.getMetaClient().reloadActiveTimeline();
      // Proceeds to execute any requested or inflight clean instances in the timeline
      String cleanInstantToExecute = cleanInstantTime.isPresent() ? cleanInstantTime.get() : inflightClean.get();
      HoodieCleanMetadata metadata = table.clean(context, cleanInstantToExecute);
      if (timerContext != null && metadata != null) {
        long durationMs = metrics.getDurationInMs(timerContext.stop());
        metrics.updateCleanMetrics(durationMs, metadata.getTotalFilesDeleted());
        LOG.info("Cleaned {} files Earliest Retained Instant :{} cleanerElapsedMs: {}",
            metadata.getTotalFilesDeleted(), metadata.getEarliestCommitToRetain(), durationMs);
      }
      releaseResources(cleanInstantToExecute);
      return metadata;
    }
    return null;
  }

  /**
   * Computes a cleaner plan and persists it to the timeline if cleaning is required.
   *
   * @param table table to schedule cleaning on.
   * @param suppliedCleanInstant Optional supplied clean instant time that overrides the generated time. This can only be used for testing.
   * @return the requested instant time if the service was scheduled
   */
  private Option<String> scheduleCleaning(HoodieTable<?, ?, ?, ?> table, Option<String> suppliedCleanInstant) {
    Option<HoodieCleanerPlan> cleanerPlan = table.createCleanerPlan(context, Option.empty());
    if (cleanerPlan.isPresent()) {
      return txnManager.executeStateChangeWithInstant(suppliedCleanInstant, cleanInstantTime -> {
        final HoodieInstant cleanInstant = table.getMetaClient().createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanInstantTime);
        // Save to both aux and timeline folder
        table.getActiveTimeline().saveToCleanRequested(cleanInstant, cleanerPlan);
        LOG.info("Requesting Cleaning with instant time {}", cleanInstant);
        Option<String> instantRange = delegateToTableServiceManager(TableServiceType.CLEAN, table);
        if (instantRange.isPresent()) {
          LOG.info("Delegate instant [{}] to table service manager", instantRange.get());
        }
        return Option.of(cleanInstantTime);
      });
    }
    return Option.empty();
  }

  /**
   * Trigger archival for the table. This ensures that the number of commits do not explode
   * and keep increasing unbounded over time.
   *
   * @param table table to commit on.
   */
  protected void archive(HoodieTable table) {
    if (!tableServicesEnabled(config)) {
      return;
    }
    try {
      final Timer.Context timerContext = metrics.getArchiveCtx();
      // We cannot have unbounded commit files. Archive commits if we have to archive.
      HoodieTimelineArchiver archiver = TimelineArchivers.getInstance(table.getMetaClient().getTimelineLayoutVersion(), config, table);
      int instantsToArchive = archiver.archiveIfRequired(context, true);
      if (timerContext != null) {
        long durationMs = metrics.getDurationInMs(timerContext.stop());
        this.metrics.updateArchiveMetrics(durationMs, instantsToArchive);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to archive", ioe);
    }
  }

  /**
   * Get inflight timeline excluding compaction and clustering.
   *
   * @param metaClient
   * @return
   */
  private HoodieTimeline getInflightTimelineExcludeCompactionAndClustering(HoodieTableMetaClient metaClient) {
    HoodieTimeline inflightTimelineExcludingCompaction = metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
    return inflightTimelineExcludingCompaction.filter(instant -> !ClusteringUtils.isClusteringInstant(
        inflightTimelineExcludingCompaction, instant, metaClient.getInstantGenerator()));
  }

  protected Option<HoodiePendingRollbackInfo> getPendingRollbackInfo(HoodieTableMetaClient metaClient, String commitToRollback) {
    return getPendingRollbackInfo(metaClient, commitToRollback, true);
  }

  public Option<HoodiePendingRollbackInfo> getPendingRollbackInfo(HoodieTableMetaClient metaClient, String commitToRollback, boolean ignoreCompactionAndClusteringInstants) {
    return getPendingRollbackInfos(metaClient, ignoreCompactionAndClusteringInstants).getOrDefault(commitToRollback, Option.empty());
  }

  protected Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient) {
    return getPendingRollbackInfos(metaClient, true);
  }

  /**
   * Fetch map of pending commits to be rolled-back to {@link HoodiePendingRollbackInfo}.
   *
   * @param metaClient instance of {@link HoodieTableMetaClient} to use.
   * @return map of pending commits to be rolled-back instants to Rollback Instant and Rollback plan Pair.
   */
  protected Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient, boolean ignoreCompactionAndClusteringInstants) {
    List<HoodieInstant> instants = metaClient.getActiveTimeline().filterPendingRollbackTimeline().getInstants();
    Map<String, Option<HoodiePendingRollbackInfo>> infoMap = new HashMap<>();
    for (HoodieInstant rollbackInstant : instants) {
      HoodieRollbackPlan rollbackPlan;
      try {
        rollbackPlan = RollbackUtils.getRollbackPlan(metaClient, rollbackInstant);
      } catch (Exception e) {
        if (rollbackInstant.isRequested()) {
          LOG.warn("Fetching rollback plan failed for " + rollbackInstant + ", deleting the plan since it's in REQUESTED state", e);
          try {
            metaClient.getActiveTimeline().deletePending(rollbackInstant);
          } catch (HoodieIOException he) {
            LOG.warn("Cannot delete " + rollbackInstant, he);
            continue;
          }
        } else {
          // Here we assume that if the rollback is inflight, the rollback plan is intact
          // in instant.rollback.requested.  The exception here can be due to other reasons.
          LOG.warn("Fetching rollback plan failed for " + rollbackInstant + ", skip the plan", e);
        }
        continue;
      }

      try {
        String action = rollbackPlan.getInstantToRollback().getAction();
        String instantToRollback = rollbackPlan.getInstantToRollback().getCommitTime();
        if (ignoreCompactionAndClusteringInstants) {
          if (!HoodieTimeline.COMPACTION_ACTION.equals(action)) {
            InstantGenerator instantGenerator = metaClient.getInstantGenerator();
            boolean isClustering = ClusteringUtils.isClusteringInstant(metaClient.getActiveTimeline(),
                instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, action, instantToRollback), instantGenerator);
            if (!isClustering) {
              infoMap.putIfAbsent(instantToRollback, Option.of(new HoodiePendingRollbackInfo(rollbackInstant, rollbackPlan)));
            }
          }
        } else {
          infoMap.putIfAbsent(instantToRollback, Option.of(new HoodiePendingRollbackInfo(rollbackInstant, rollbackPlan)));
        }
      } catch (Exception e) {
        LOG.warn("Processing rollback plan failed for " + rollbackInstant + ", skip the plan", e);
      }
    }
    return infoMap;
  }

  /**
   * Rolls back the failed delta commits corresponding to the indexing action.
   * <p>
   * TODO(HUDI-5733): This should be cleaned up once the proper fix of rollbacks
   *  in the metadata table is landed.
   *
   * @return {@code true} if rollback happens; {@code false} otherwise.
   */
  protected boolean rollbackFailedIndexingCommits() {
    HoodieTable table = createTable(config, storageConf);
    List<String> instantsToRollback = getFailedIndexingCommitsToRollbackForMetadataTable(table.getMetaClient());
    Map<String, Option<HoodiePendingRollbackInfo>> pendingRollbacks = getPendingRollbackInfos(table.getMetaClient());
    instantsToRollback.forEach(entry -> pendingRollbacks.putIfAbsent(entry, Option.empty()));
    rollbackFailedWrites(pendingRollbacks);
    return !pendingRollbacks.isEmpty();
  }

  private List<String> getFailedIndexingCommitsToRollbackForMetadataTable(HoodieTableMetaClient metaClient) {
    if (!isMetadataTable(metaClient.getBasePath())) {
      return Collections.emptyList();
    }
    HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(HoodieTableMetadata.getDatasetBasePath(config.getBasePath()))
        .setConf(metaClient.getStorageConf().newInstance())
        .build();
    HoodieTimeline dataIndexTimeline = dataMetaClient.getActiveTimeline().filter(instant -> instant.getAction().equals(HoodieTimeline.INDEXING_ACTION));

    Stream<HoodieInstant> inflightInstantsStream = metaClient.getCommitsTimeline()
        .filter(instant -> !instant.isCompleted()
            && isIndexingCommit(dataIndexTimeline, instant.requestedTime()))
        .getInstantsAsStream();
    return inflightInstantsStream.filter(instant -> {
      try {
        return heartbeatClient.isHeartbeatExpired(instant.requestedTime());
      } catch (IOException io) {
        throw new HoodieException("Failed to check heartbeat for instant " + instant, io);
      }
    }).map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  /**
   * Rollback all failed writes.
   *
   * @return true if rollback was triggered. false otherwise.
   */
  protected boolean rollbackFailedWrites(HoodieTableMetaClient metaClient) {
    List<String> instantsToRollback = getInstantsToRollback(metaClient, config.getFailedWritesCleanPolicy(), Option.empty());
    Map<String, Option<HoodiePendingRollbackInfo>> pendingRollbacks = getPendingRollbackInfos(metaClient);
    instantsToRollback.forEach(entry -> pendingRollbacks.putIfAbsent(entry, Option.empty()));
    rollbackFailedWrites(pendingRollbacks);
    return !pendingRollbacks.isEmpty();
  }

  protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback) {
    rollbackFailedWrites(instantsToRollback, false, false);
  }

  protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback, boolean skipLocking, boolean skipVersionCheck) {
    // sort in reverse order of commit times
    LinkedHashMap<String, Option<HoodiePendingRollbackInfo>> reverseSortedRollbackInstants = instantsToRollback.entrySet()
        .stream().sorted((i1, i2) -> i2.getKey().compareTo(i1.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    boolean isMetadataTable = isMetadataTable(basePath);
    for (Map.Entry<String, Option<HoodiePendingRollbackInfo>> entry : reverseSortedRollbackInstants.entrySet()) {
      if (!isMetadataTable
          && compareTimestamps(entry.getKey(), LESSER_THAN_OR_EQUALS,
          HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
        // do we need to handle failed rollback of a bootstrap
        rollbackFailedBootstrap();
        HeartbeatUtils.deleteHeartbeatFile(storage, basePath, entry.getKey(), config);
        break;
      } else {
        rollback(entry.getKey(), entry.getValue(), skipLocking, skipVersionCheck);
        HeartbeatUtils.deleteHeartbeatFile(storage, basePath, entry.getKey(), config);
      }
    }
  }

  protected List<String> getInstantsToRollback(HoodieTableMetaClient metaClient, HoodieFailedWritesCleaningPolicy cleaningPolicy, Option<String> curInstantTime) {
    Stream<HoodieInstant> inflightInstantsStream = getInflightTimelineExcludeCompactionAndClustering(metaClient)
        .getReverseOrderedInstants();
    if (cleaningPolicy.isEager()) {
      // Metadata table uses eager cleaning policy, but we need to exclude inflight delta commits
      // from the async indexer (`HoodieIndexer`).
      // TODO(HUDI-5733): This should be cleaned up once the proper fix of rollbacks in the
      //  metadata table is landed.
      if (metaClient.isMetadataTable()) {
        HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
            .setBasePath(HoodieTableMetadata.getDatasetBasePath(config.getBasePath()))
            .setConf(metaClient.getStorageConf().newInstance())
            .build();
        HoodieTimeline dataIndexTimeline = dataMetaClient.getActiveTimeline().filter(instant -> instant.getAction().equals(HoodieTimeline.INDEXING_ACTION));
        return inflightInstantsStream.map(HoodieInstant::requestedTime).filter(entry -> {
          if (curInstantTime.isPresent()) {
            return !entry.equals(curInstantTime.get());
          } else {
            return !isIndexingCommit(dataIndexTimeline, entry);
          }
        }).collect(Collectors.toList());
      }
      return inflightInstantsStream.map(HoodieInstant::requestedTime).filter(entry -> {
        if (curInstantTime.isPresent()) {
          return !entry.equals(curInstantTime.get());
        } else {
          return true;
        }
      }).collect(Collectors.toList());
    } else if (cleaningPolicy.isLazy()) {
      return getInstantsToRollbackForLazyCleanPolicy(metaClient, inflightInstantsStream);
    } else if (cleaningPolicy.isNever()) {
      return Collections.emptyList();
    } else {
      throw new IllegalArgumentException("Invalid Failed Writes Cleaning Policy " + config.getFailedWritesCleanPolicy());
    }
  }

  private List<String> getInstantsToRollbackForLazyCleanPolicy(HoodieTableMetaClient metaClient,
                                                               Stream<HoodieInstant> inflightInstantsStream) {
    // Get expired instants, must store them into list before double-checking
    List<String> expiredInstants = inflightInstantsStream.filter(instant -> {
      try {
        // An instant transformed from inflight to completed have no heartbeat file and will be detected as expired instant here
        return heartbeatClient.isHeartbeatExpired(instant.requestedTime());
      } catch (IOException io) {
        throw new HoodieException("Failed to check heartbeat for instant " + instant, io);
      }
    }).map(HoodieInstant::requestedTime).collect(Collectors.toList());

    if (!expiredInstants.isEmpty()) {
      // Only return instants that haven't been completed by other writers
      metaClient.reloadActiveTimeline();
      HoodieTimeline refreshedInflightTimeline = getInflightTimelineExcludeCompactionAndClustering(metaClient);
      return expiredInstants.stream().filter(refreshedInflightTimeline::containsInstant).collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * @param commitInstantTime   Instant time of the commit
   * @param pendingRollbackInfo pending rollback instant and plan if rollback failed from previous attempt.
   * @param skipLocking         if this is triggered by another parent transaction, locking can be skipped.
   * @throws HoodieRollbackException if rollback cannot be performed successfully
   * @Deprecated Rollback the inflight record changes with the given commit time. This
   * will be removed in future in favor of {@link BaseHoodieWriteClient#restoreToInstant(String, boolean)
   */
  @Deprecated
  public boolean rollback(final String commitInstantTime, Option<HoodiePendingRollbackInfo> pendingRollbackInfo,
                          boolean skipLocking, boolean skipVersionCheck) throws HoodieRollbackException {
    return rollback(commitInstantTime, pendingRollbackInfo, Option.empty(), skipLocking, skipVersionCheck);
  }

  /**
   * @param commitInstantTime   Instant time of the commit
   * @param pendingRollbackInfo pending rollback instant and plan if rollback failed from previous attempt.
   * @param suppliedRollbackInstantTime the user provided instant to use for the rollback. This is only set for rolling back instants on the metadata table.
   * @param skipLocking         if this is triggered by another parent transaction, locking can be skipped.
   * @throws HoodieRollbackException if rollback cannot be performed successfully
   * @Deprecated Rollback the inflight record changes with the given commit time. This
   * will be removed in future in favor of {@link BaseHoodieWriteClient#restoreToInstant(String, boolean)
   */
  @Deprecated
  public boolean rollback(final String commitInstantTime, Option<HoodiePendingRollbackInfo> pendingRollbackInfo, Option<String> suppliedRollbackInstantTime,
                          boolean skipLocking, boolean skipVersionCheck) throws HoodieRollbackException {
    LOG.info("Begin rollback of instant {} for table {}", commitInstantTime, config.getBasePath());
    final Timer.Context timerContext = this.metrics.getRollbackCtx();
    try {
      HoodieTable table = createTable(config, storageConf, skipVersionCheck);
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstantsAsStream()
          .filter(instant -> EQUALS.test(instant.requestedTime(), commitInstantTime))
          .findFirst());
      Option<HoodieRollbackPlan> rollbackPlanOption;
      String rollbackInstantTime;
      if (pendingRollbackInfo.isPresent()) {
        rollbackPlanOption = Option.of(pendingRollbackInfo.get().getRollbackPlan());
        rollbackInstantTime = pendingRollbackInfo.get().getRollbackInstant().requestedTime();
      } else {
        if (commitInstantOpt.isEmpty()) {
          LOG.warn("Cannot find instant {} in the timeline of table {} for rollback", commitInstantTime, config.getBasePath());
          return false;
        }
        Pair<String, Option<HoodieRollbackPlan>> result = txnManager.executeStateChangeWithInstant(!skipLocking, suppliedRollbackInstantTime, instant ->
            Pair.of(instant, table.scheduleRollback(context, instant, commitInstantOpt.get(), false, config.shouldRollbackUsingMarkers(), false)));
        rollbackInstantTime = result.getLeft();
        rollbackPlanOption = result.getRight();
      }

      if (rollbackPlanOption.isPresent()) {
        // There can be a case where the inflight rollback failed after the instant files
        // are deleted for commitInstantTime, so that commitInstantOpt is empty as it is
        // not present in the timeline.  In such a case, the hoodie instant instance
        // is reconstructed to allow the rollback to be reattempted, and the deleteInstants
        // is set to false since they are already deleted.
        // Execute rollback
        HoodieRollbackMetadata rollbackMetadata = commitInstantOpt.isPresent()
            ? table.rollback(context, rollbackInstantTime, commitInstantOpt.get(), true, skipLocking)
            : table.rollback(context, rollbackInstantTime, table.getMetaClient().createNewInstant(
                HoodieInstant.State.INFLIGHT, rollbackPlanOption.get().getInstantToRollback().getAction(), commitInstantTime),
            false, skipLocking);
        if (timerContext != null) {
          long durationInMs = metrics.getDurationInMs(timerContext.stop());
          metrics.updateRollbackMetrics(durationInMs, rollbackMetadata.getTotalFilesDeleted());
        }
        return true;
      } else {
        throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime);
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime, e);
    }
  }

  /**
   * Main API to rollback failed bootstrap.
   */
  public void rollbackFailedBootstrap() {
    LOG.info("Rolling back pending bootstrap if present");
    HoodieTable table = createTable(config, storageConf);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompactionAndLogCompaction();
    Option<String> instant = Option.fromJavaOptional(
        inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::requestedTime).findFirst());
    if (instant.isPresent() && compareTimestamps(instant.get(), LESSER_THAN_OR_EQUALS,
        HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
      LOG.info("Found pending bootstrap instants. Rolling them back");
      txnManager.executeStateChangeWithInstant(instantTime -> {
        table.rollbackBootstrap(context, instantTime);
        return null;
      });
      LOG.info("Finished rolling back pending bootstrap");
    }

    // if bootstrap failed, lets delete metadata and restart from scratch
    HoodieTableMetadataUtil.deleteMetadataTable(config.getBasePath(), context);
  }

  /**
   * Some writers use SparkAllowUpdateStrategy and treat replacecommit plan as revocable plan.
   * In those cases, their ConflictResolutionStrategy implementation should run conflict resolution
   * even for clustering operations.
   *
   * @return boolean
   */
  protected boolean isPreCommitRequired() {
    return this.config.getWriteConflictResolutionStrategy().isPreCommitRequired();
  }

  private Option<String> delegateToTableServiceManager(TableServiceType tableServiceType, HoodieTable table) {
    if (!config.getTableServiceManagerConfig().isEnabledAndActionSupported(ActionType.compaction)) {
      return Option.empty();
    }
    HoodieTableServiceManagerClient tableServiceManagerClient = new HoodieTableServiceManagerClient(table.getMetaClient(), config.getTableServiceManagerConfig());
    switch (tableServiceType) {
      case COMPACT:
        return tableServiceManagerClient.executeCompaction();
      case CLUSTER:
        return tableServiceManagerClient.executeClustering();
      case CLEAN:
        return tableServiceManagerClient.executeClean();
      default:
        LOG.info("Not supported delegate to table service manager, tableServiceType : " + tableServiceType.getAction());
        return Option.empty();
    }
  }

  @Override
  public void close() {
    AsyncArchiveService.forceShutdown(asyncArchiveService);
    asyncArchiveService = null;
    AsyncCleanerService.forceShutdown(asyncCleanerService);
    asyncCleanerService = null;
    // Stop timeline-server if running
    super.close();
  }

  protected void handleWriteErrors(List<HoodieWriteStat> writeStats, TableServiceType tableServiceType) {
    if (writeStats.stream().mapToLong(HoodieWriteStat::getTotalWriteErrors).sum() > 0) {
      String message = tableServiceType + " failed to write to files:"
          + writeStats.stream().filter(s -> s.getTotalWriteErrors() > 0L).map(HoodieWriteStat::getFileId).collect(Collectors.joining(","));
      switch (tableServiceType) {
        case CLUSTER:
          throw new HoodieClusteringException(message);
        case LOG_COMPACT:
          throw new HoodieLogCompactException(message);
        case COMPACT:
          throw new HoodieCompactionException(message);
        default:
          throw new HoodieException(message);
      }
    }
  }

  protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    // no op
  }

  /**
   * Called after each commit of a compaction or clustering table service,
   * to release any resources used.
   */
  protected void releaseResources(String instantTime) {
    // do nothing here
  }
}
