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
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HeartbeatUtils;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.data.HoodieData;
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
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
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
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.action.rollback.RollbackUtils;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.HoodieTableMetadata.isMetadataTable;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.isIndexingCommit;

/**
 * Base class for all shared logic between table service clients regardless of engine.
 * @param <I> The {@link HoodieTable} implementation's input type
 * @param <T> The {@link HoodieTable} implementation's output type
 * @param <O> The {@link BaseHoodieWriteClient} implementation's output type (differs in case of spark)
 */
public abstract class BaseHoodieTableServiceClient<I, T, O> extends BaseHoodieClient implements RunsTableService {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHoodieWriteClient.class);

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
    HoodieTable table = createTable(config, hadoopConf);
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
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf().unwrapAs(Configuration.class));

    // Check if a commit or compaction instant with a greater timestamp is on the timeline.
    // If an instant is found then abort log compaction, since it is no longer needed.
    Set<String> actions = CollectionUtils.createSet(COMMIT_ACTION, COMPACTION_ACTION);
    Option<HoodieInstant> compactionInstantWithGreaterTimestamp =
        Option.fromJavaOptional(table.getActiveTimeline().getInstantsAsStream()
            .filter(hoodieInstant -> actions.contains(hoodieInstant.getAction()))
            .filter(hoodieInstant -> HoodieTimeline.compareTimestamps(hoodieInstant.getTimestamp(),
                GREATER_THAN, logCompactionInstantTime))
            .findFirst());
    if (compactionInstantWithGreaterTimestamp.isPresent()) {
      throw new HoodieLogCompactException(String.format("Cannot log compact since a compaction instant with greater "
          + "timestamp exists. Instant details %s", compactionInstantWithGreaterTimestamp.get()));
    }

    HoodieTimeline pendingLogCompactionTimeline = table.getActiveTimeline().filterPendingLogCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getLogCompactionInflightInstant(logCompactionInstantTime);
    if (pendingLogCompactionTimeline.containsInstant(inflightInstant)) {
      LOG.info("Found Log compaction inflight file. Rolling back the commit and exiting.");
      table.rollbackInflightLogCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false),
          Option.of(txnManager));
      table.getMetaClient().reloadActiveTimeline();
      throw new HoodieException("Execution is aborted since it found an Inflight logcompaction,"
          + "log compaction plans are mutable plans, so reschedule another logcompaction.");
    }
    logCompactionTimer = metrics.getLogCompactionCtx();
    WriteMarkersFactory.get(config.getMarkersType(), table, logCompactionInstantTime);
    HoodieWriteMetadata<T> writeMetadata = table.logCompact(context, logCompactionInstantTime);
    HoodieWriteMetadata<O> logCompactionMetadata = convertToOutputMetadata(writeMetadata);
    if (shouldComplete && logCompactionMetadata.getCommitMetadata().isPresent()) {
      completeLogCompaction(logCompactionMetadata.getCommitMetadata().get(), table, logCompactionInstantTime);
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
          compact(instant.getTimestamp(), true);
        });
  }

  protected void runAnyPendingLogCompactions(HoodieTable table) {
    table.getActiveTimeline().getWriteTimeline().filterPendingLogCompactionTimeline().getInstantsAsStream()
        .forEach(instant -> {
          LOG.info("Running previously failed inflight log compaction at instant {}", instant);
          logCompact(instant.getTimestamp(), true);
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
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleCompactionAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of Write Status
   */
  protected HoodieWriteMetadata<O> compact(String compactionInstantTime, boolean shouldComplete) {
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf().unwrapAs(Configuration.class));
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false),
          Option.of(txnManager));
      table.getMetaClient().reloadActiveTimeline();
    }
    compactionTimer = metrics.getCompactionCtx();
    HoodieWriteMetadata<T> writeMetadata = table.compact(context, compactionInstantTime);
    HoodieWriteMetadata<O> compactionMetadata = convertToOutputMetadata(writeMetadata);
    if (shouldComplete && compactionMetadata.getCommitMetadata().isPresent()) {
      completeCompaction(compactionMetadata.getCommitMetadata().get(), table, compactionInstantTime);
    }
    return compactionMetadata;
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param metadata              All the metadata that gets stored along with a commit
   * @param extraMetadata         Extra Metadata to be stored
   */
  public void commitCompaction(String compactionInstantTime, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, createTable(config, context.getStorageConf().unwrapAs(Configuration.class)), compactionInstantTime);
  }

  /**
   * Commit Compaction and track metrics.
   */
  protected void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction: " + config.getTableName());
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    handleWriteErrors(writeStats, TableServiceType.COMPACT);
    final HoodieInstant compactionInstant = HoodieTimeline.getCompactionInflightInstant(compactionCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      writeTableMetadata(table, compactionCommitTime, metadata, context.emptyHoodieData());
      LOG.info("Committing Compaction {}", compactionCommitTime);
      LOG.debug("Compaction {} finished with result: {}", compactionCommitTime, metadata);
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionCommitTime, metadata);
    } finally {
      this.txnManager.endTransaction(Option.of(compactionInstant));
      releaseResources(compactionCommitTime);
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, compactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      HoodieActiveTimeline.parseDateFromInstantTimeSafely(compactionCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, COMPACTION_ACTION)
      );
    }
    LOG.info("Compacted successfully on commit {}", compactionCommitTime);
  }

  /**
   * Schedules a new log compaction instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleLogCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleLogCompactionAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new log compaction instant with passed-in instant time.
   *
   * @param instantTime   Log Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleLogCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(instantTime, extraMetadata, TableServiceType.LOG_COMPACT).isPresent();
  }

  /**
   * Performs Log Compaction for the workload stored in instant-time.
   *
   * @param logCompactionInstantTime Log Compaction Instant Time
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public HoodieWriteMetadata<O> logCompact(String logCompactionInstantTime) {
    return logCompact(logCompactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit Log Compaction and track metrics.
   */
  protected void completeLogCompaction(HoodieCommitMetadata metadata, HoodieTable table, String logCompactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect log compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    handleWriteErrors(writeStats, TableServiceType.LOG_COMPACT);
    final HoodieInstant logCompactionInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, logCompactionCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(logCompactionInstant), Option.empty());
      preCommit(metadata);
      finalizeWrite(table, logCompactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      writeTableMetadata(table, logCompactionCommitTime, metadata, context.emptyHoodieData());
      LOG.info("Committing Log Compaction {}", logCompactionCommitTime);
      LOG.debug("Log Compaction {} finished with result {}", logCompactionCommitTime, metadata);
      CompactHelpers.getInstance().completeInflightLogCompaction(table, logCompactionCommitTime, metadata);
    } finally {
      this.txnManager.endTransaction(Option.of(logCompactionInstant));
      releaseResources(logCompactionCommitTime);
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, logCompactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (logCompactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(logCompactionTimer.stop());
      HoodieActiveTimeline.parseDateFromInstantTimeSafely(logCompactionCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, HoodieActiveTimeline.LOG_COMPACTION_ACTION)
      );
    }
    LOG.info("Log Compacted successfully on commit {}", logCompactionCommitTime);
  }

  /**
   * Schedules a new compaction instant with passed-in instant time.
   *
   * @param instantTime   Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(instantTime, extraMetadata, TableServiceType.COMPACT).isPresent();
  }

  /**
   * Schedules a new clustering instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleClustering(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleClusteringAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new clustering instant with passed-in instant time.
   *
   * @param instantTime   clustering Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleClusteringAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(instantTime, extraMetadata, TableServiceType.CLUSTER).isPresent();
  }

  /**
   * Ensures clustering instant is in expected state and performs clustering for the plan stored in metadata.
   *
   * @param clusteringInstant Clustering Instant Time
   * @return Collection of Write Status
   */
  public HoodieWriteMetadata<O> cluster(String clusteringInstant, boolean shouldComplete) {
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf().unwrapAs(Configuration.class));
    HoodieTimeline pendingClusteringTimeline = table.getActiveTimeline().filterPendingReplaceTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getReplaceCommitInflightInstant(clusteringInstant);
    if (pendingClusteringTimeline.containsInstant(inflightInstant)) {
      if (pendingClusteringTimeline.isPendingClusterInstant(inflightInstant.getTimestamp())) {
        table.rollbackInflightClustering(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false),
            Option.of(txnManager));
        table.getMetaClient().reloadActiveTimeline();
      } else {
        throw new HoodieClusteringException("Non clustering replace-commit inflight at timestamp " + clusteringInstant);
      }
    }
    clusteringTimer = metrics.getClusteringCtx();
    LOG.info("Starting clustering at {}", clusteringInstant);
    HoodieWriteMetadata<T> writeMetadata = table.cluster(context, clusteringInstant);
    HoodieWriteMetadata<O> clusteringMetadata = convertToOutputMetadata(writeMetadata);
    // Validation has to be done after cloning. if not, it could result in referencing the write status twice which means clustering could get executed twice.
    validateClusteringCommit(clusteringMetadata, clusteringInstant, table);

    // Publish file creation metrics for clustering.
    if (config.isMetricsOn()) {
      clusteringMetadata.getWriteStats()
          .ifPresent(hoodieWriteStats -> hoodieWriteStats.stream()
              .filter(hoodieWriteStat -> hoodieWriteStat.getRuntimeStats() != null)
              .map(hoodieWriteStat -> hoodieWriteStat.getRuntimeStats().getTotalCreateTime())
              .forEach(metrics::updateClusteringFileCreationMetrics));
    }

    // TODO : Where is shouldComplete used ?
    if (shouldComplete && clusteringMetadata.getCommitMetadata().isPresent()) {
      completeClustering((HoodieReplaceCommitMetadata) clusteringMetadata.getCommitMetadata().get(), table, clusteringInstant, Option.ofNullable(convertToWriteStatus(writeMetadata)));
    }
    return clusteringMetadata;
  }

  public boolean purgePendingClustering(String clusteringInstant) {
    HoodieTable<?, I, ?, T> table = createTable(config, context.getStorageConf().unwrapAs(Configuration.class));
    HoodieTimeline pendingClusteringTimeline = table.getActiveTimeline().filterPendingReplaceTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getReplaceCommitInflightInstant(clusteringInstant);
    if (pendingClusteringTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightClustering(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false), true,
          Option.of(txnManager));
      table.getMetaClient().reloadActiveTimeline();
      return true;
    }
    return false;
  }

  protected abstract void validateClusteringCommit(HoodieWriteMetadata<O> clusteringMetadata, String clusteringCommitTime, HoodieTable table);

  protected abstract HoodieWriteMetadata<O> convertToOutputMetadata(HoodieWriteMetadata<T> writeMetadata);

  protected abstract HoodieData<WriteStatus> convertToWriteStatus(HoodieWriteMetadata<T> writeMetadata);

  private void completeClustering(HoodieReplaceCommitMetadata metadata,
                                  HoodieTable table,
                                  String clusteringCommitTime,
                                  Option<HoodieData<WriteStatus>> writeStatuses) {
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    handleWriteErrors(writeStats, TableServiceType.CLUSTER);
    final HoodieInstant clusteringInstant = HoodieTimeline.getReplaceCommitInflightInstant(clusteringCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(clusteringInstant), Option.empty());

      finalizeWrite(table, clusteringCommitTime, writeStats);
      // Only in some cases conflict resolution needs to be performed.
      // So, check if preCommit method that does conflict resolution needs to be triggered.
      if (isPreCommitRequired()) {
        preCommit(metadata);
      }
      // Update table's metadata (table)
      writeTableMetadata(table, clusteringInstant.getTimestamp(), metadata, writeStatuses.orElseGet(context::emptyHoodieData));

      LOG.info("Committing Clustering {}", clusteringCommitTime);
      LOG.debug("Clustering {} finished with result {}", clusteringCommitTime, metadata);

      table.getActiveTimeline().transitionReplaceInflightToComplete(
          clusteringInstant,
          Option.of(getUTF8Bytes(metadata.toJsonString())));
    } catch (Exception e) {
      throw new HoodieClusteringException("unable to transition clustering inflight to complete: " + clusteringCommitTime, e);
    } finally {
      this.txnManager.endTransaction(Option.of(clusteringInstant));
      releaseResources(clusteringCommitTime);
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, clusteringCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (clusteringTimer != null) {
      long durationInMs = metrics.getDurationInMs(clusteringTimer.stop());
      HoodieActiveTimeline.parseDateFromInstantTimeSafely(clusteringCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, HoodieActiveTimeline.REPLACE_COMMIT_ACTION)
      );
    }
    LOG.info("Clustering successfully on commit {}", clusteringCommitTime);
  }

  protected void runTableServicesInline(HoodieTable table, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    if (!tableServicesEnabled(config)) {
      return;
    }

    if (!config.areAnyTableServicesExecutedInline() && !config.areAnyTableServicesScheduledInline()) {
      return;
    }

    if (config.isMetadataTableEnabled()) {
      table.getHoodieView().sync();
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
  }

  /**
   * Schedule table services such as clustering, compaction & cleaning.
   *
   * @param extraMetadata    Metadata to pass onto the scheduled service instant
   * @param tableServiceType Type of table service to schedule
   * @return
   */
  public Option<String> scheduleTableService(String instantTime, Option<Map<String, String>> extraMetadata,
                                             TableServiceType tableServiceType) {
    // A lock is required to guard against race conditions between an ongoing writer and scheduling a table service.
    final Option<HoodieInstant> inflightInstant = Option.of(new HoodieInstant(HoodieInstant.State.REQUESTED,
        tableServiceType.getAction(), instantTime));
    try {
      this.txnManager.beginTransaction(inflightInstant, Option.empty());
      LOG.info("Scheduling table service {}", tableServiceType);
      return scheduleTableServiceInternal(instantTime, extraMetadata, tableServiceType);
    } finally {
      this.txnManager.endTransaction(inflightInstant);
    }
  }

  protected Option<String> scheduleTableServiceInternal(String instantTime, Option<Map<String, String>> extraMetadata,
                                                        TableServiceType tableServiceType) {
    if (!tableServicesEnabled(config)) {
      return Option.empty();
    }

    Option<String> option = Option.empty();
    HoodieTable<?, ?, ?, ?> table = createTable(config, hadoopConf);

    switch (tableServiceType) {
      case ARCHIVE:
        LOG.info("Scheduling archiving is not supported. Skipping.");
        break;
      case CLUSTER:
        LOG.info("Scheduling clustering at instant time: {}", instantTime);
        Option<HoodieClusteringPlan> clusteringPlan = table
            .scheduleClustering(context, instantTime, extraMetadata);
        option = clusteringPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      case COMPACT:
        LOG.info("Scheduling compaction at instant time: {}", instantTime);
        Option<HoodieCompactionPlan> compactionPlan = table
            .scheduleCompaction(context, instantTime, extraMetadata);
        option = compactionPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      case LOG_COMPACT:
        LOG.info("Scheduling log compaction at instant time: {}", instantTime);
        Option<HoodieCompactionPlan> logCompactionPlan = table
            .scheduleLogCompaction(context, instantTime, extraMetadata);
        option = logCompactionPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      case CLEAN:
        LOG.info("Scheduling cleaning at instant time: {}", instantTime);
        Option<HoodieCleanerPlan> cleanerPlan = table
            .scheduleCleaning(context, instantTime, extraMetadata);
        option = cleanerPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      default:
        throw new IllegalArgumentException("Invalid TableService " + tableServiceType);
    }

    Option<String> instantRange = delegateToTableServiceManager(tableServiceType, table);
    if (instantRange.isPresent()) {
      LOG.info("Delegate instant [{}] to table service manager", instantRange.get());
    }

    return option;
  }

  protected abstract HoodieTable<?, I, ?, T> createTable(HoodieWriteConfig config, Configuration hadoopConf);

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
    if (shouldDelegateToTableServiceManager(config, ActionType.replacecommit)) {
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
    table.getActiveTimeline().filterPendingReplaceTimeline().getInstants().forEach(instant -> {
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlan = ClusteringUtils.getClusteringPlan(table.getMetaClient(), instant);
      if (instantPlan.isPresent()) {
        LOG.info("Running pending clustering at instant {}", instantPlan.get().getLeft());
        cluster(instant.getTimestamp(), true);
      }
    });
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned). This API provides the flexibility to schedule clean instant asynchronously via
   * {@link BaseHoodieTableServiceClient#scheduleTableService(String, Option, TableServiceType)} and disable inline scheduling
   * of clean.
   *
   * @param cleanInstantTime instant time for clean.
   * @param scheduleInline   true if needs to be scheduled inline. false otherwise.
   */
  @Nullable
  @Deprecated
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline, boolean skipLocking) throws HoodieIOException {
    return clean(cleanInstantTime, scheduleInline);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned). This API provides the flexibility to schedule clean instant asynchronously via
   * {@link BaseHoodieTableServiceClient#scheduleTableService(String, Option, TableServiceType)} and disable inline scheduling
   * of clean.
   *
   * @param cleanInstantTime instant time for clean.
   * @param scheduleInline   true if needs to be scheduled inline. false otherwise.
   */
  @Nullable
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline) throws HoodieIOException {
    if (!tableServicesEnabled(config)) {
      return null;
    }
    final Timer.Context timerContext = metrics.getCleanCtx();
    CleanerUtils.rollbackFailedWrites(config.getFailedWritesCleanPolicy(),
        HoodieTimeline.CLEAN_ACTION, () -> rollbackFailedWrites());

    HoodieTable table = createTable(config, hadoopConf);
    if (config.allowMultipleCleans() || !table.getActiveTimeline().getCleanerTimeline().filterInflightsAndRequested().firstInstant().isPresent()) {
      LOG.info("Cleaner started");
      // proceed only if multiple clean schedules are enabled or if there are no pending cleans.
      if (scheduleInline) {
        scheduleClean(cleanInstantTime);
        table.getMetaClient().reloadActiveTimeline();
      }

      if (shouldDelegateToTableServiceManager(config, ActionType.clean)) {
        LOG.warn("Cleaning is not yet supported with Table Service Manager.");
        return null;
      }
    }

    // Proceeds to execute any requested or inflight clean instances in the timeline
    HoodieCleanMetadata metadata = table.clean(context, cleanInstantTime);
    if (timerContext != null && metadata != null) {
      long durationMs = metrics.getDurationInMs(timerContext.stop());
      metrics.updateCleanMetrics(durationMs, metadata.getTotalFilesDeleted());
      LOG.info("Cleaned " + metadata.getTotalFilesDeleted() + " files"
          + " Earliest Retained Instant :" + metadata.getEarliestCommitToRetain()
          + " cleanerElapsedMs" + durationMs);
    }
    releaseResources(cleanInstantTime);
    return metadata;
  }

  private void scheduleClean(String cleanInstantTime) {
    HoodieInstant cleanInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, CLEAN_ACTION, cleanInstantTime);
    try {
      txnManager.beginTransaction(Option.of(cleanInstant), Option.empty());
      scheduleTableServiceInternal(cleanInstantTime, Option.empty(), TableServiceType.CLEAN);
    } finally {
      txnManager.endTransaction(Option.of(cleanInstant));
    }
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
      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(config, table);
      archiver.archiveIfRequired(context, true);
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
    HoodieTimeline inflightTimelineWithReplaceCommit = metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
    HoodieTimeline inflightTimelineExcludeClusteringCommit = inflightTimelineWithReplaceCommit.filter(instant -> {
      if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
        Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlan = ClusteringUtils.getClusteringPlan(metaClient, instant);
        return !instantPlan.isPresent();
      } else {
        return true;
      }
    });
    return inflightTimelineExcludeClusteringCommit;
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
            boolean isClustering = HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)
                && ClusteringUtils.getClusteringPlan(metaClient, new HoodieInstant(true, action, instantToRollback)).isPresent();
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
   * Such delta commits are identified based on the suffix `METADATA_INDEXER_TIME_SUFFIX` ("004").
   * <p>
   * TODO(HUDI-5733): This should be cleaned up once the proper fix of rollbacks
   *  in the metadata table is landed.
   *
   * @return {@code true} if rollback happens; {@code false} otherwise.
   */
  protected boolean rollbackFailedIndexingCommits() {
    HoodieTable table = createTable(config, hadoopConf);
    List<String> instantsToRollback = getFailedIndexingCommitsToRollback(table.getMetaClient());
    Map<String, Option<HoodiePendingRollbackInfo>> pendingRollbacks = getPendingRollbackInfos(table.getMetaClient());
    instantsToRollback.forEach(entry -> pendingRollbacks.putIfAbsent(entry, Option.empty()));
    rollbackFailedWrites(pendingRollbacks);
    return !pendingRollbacks.isEmpty();
  }

  protected List<String> getFailedIndexingCommitsToRollback(HoodieTableMetaClient metaClient) {
    Stream<HoodieInstant> inflightInstantsStream = metaClient.getCommitsTimeline()
        .filter(instant -> !instant.isCompleted()
            && isIndexingCommit(instant.getTimestamp()))
        .getInstantsAsStream();
    return inflightInstantsStream.filter(instant -> {
      try {
        return heartbeatClient.isHeartbeatExpired(instant.getTimestamp());
      } catch (IOException io) {
        throw new HoodieException("Failed to check heartbeat for instant " + instant, io);
      }
    }).map(HoodieInstant::getTimestamp).collect(Collectors.toList());
  }

  /**
   * Rollback all failed writes.
   *
   * @return true if rollback was triggered. false otherwise.
   */
  protected Boolean rollbackFailedWrites() {
    HoodieTable table = createTable(config, hadoopConf);
    List<String> instantsToRollback = getInstantsToRollback(table.getMetaClient(), config.getFailedWritesCleanPolicy(), Option.empty());
    Map<String, Option<HoodiePendingRollbackInfo>> pendingRollbacks = getPendingRollbackInfos(table.getMetaClient());
    instantsToRollback.forEach(entry -> pendingRollbacks.putIfAbsent(entry, Option.empty()));
    rollbackFailedWrites(pendingRollbacks);
    return !pendingRollbacks.isEmpty();
  }

  protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback) {
    rollbackFailedWrites(instantsToRollback, false);
  }

  protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback, boolean skipLocking) {
    // sort in reverse order of commit times
    LinkedHashMap<String, Option<HoodiePendingRollbackInfo>> reverseSortedRollbackInstants = instantsToRollback.entrySet()
        .stream().sorted((i1, i2) -> i2.getKey().compareTo(i1.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    boolean isMetadataTable = isMetadataTable(basePath);
    for (Map.Entry<String, Option<HoodiePendingRollbackInfo>> entry : reverseSortedRollbackInstants.entrySet()) {
      if (!isMetadataTable
          && HoodieTimeline.compareTimestamps(entry.getKey(), HoodieTimeline.LESSER_THAN_OR_EQUALS,
          HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
        // do we need to handle failed rollback of a bootstrap
        rollbackFailedBootstrap();
        HeartbeatUtils.deleteHeartbeatFile(storage, basePath, entry.getKey(), config);
        break;
      } else {
        rollback(entry.getKey(), entry.getValue(), skipLocking);
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
      if (isMetadataTable(metaClient.getBasePath().toString())) {
        return inflightInstantsStream.map(HoodieInstant::getTimestamp).filter(entry -> {
          if (curInstantTime.isPresent()) {
            return !entry.equals(curInstantTime.get());
          } else {
            return !isIndexingCommit(entry);
          }
        }).collect(Collectors.toList());
      }
      return inflightInstantsStream.map(HoodieInstant::getTimestamp).filter(entry -> {
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
        return heartbeatClient.isHeartbeatExpired(instant.getTimestamp());
      } catch (IOException io) {
        throw new HoodieException("Failed to check heartbeat for instant " + instant, io);
      }
    }).map(HoodieInstant::getTimestamp).collect(Collectors.toList());

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
  public boolean rollback(final String commitInstantTime, Option<HoodiePendingRollbackInfo> pendingRollbackInfo, boolean skipLocking) throws HoodieRollbackException {
    final String rollbackInstantTime = pendingRollbackInfo.map(entry -> entry.getRollbackInstant().getTimestamp())
        .orElseGet(HoodieActiveTimeline::createNewInstantTime);
    return rollback(commitInstantTime, pendingRollbackInfo, rollbackInstantTime, skipLocking);
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
  public boolean rollback(final String commitInstantTime, Option<HoodiePendingRollbackInfo> pendingRollbackInfo, String rollbackInstantTime,
                          boolean skipLocking) throws HoodieRollbackException {
    LOG.info("Begin rollback of instant " + commitInstantTime);
    final Timer.Context timerContext = this.metrics.getRollbackCtx();
    try {
      HoodieTable table = createTable(config, hadoopConf);
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstantsAsStream()
          .filter(instant -> HoodieActiveTimeline.EQUALS.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent() || pendingRollbackInfo.isPresent()) {
        LOG.info(String.format("Scheduling Rollback at instant time : %s "
                + "(exists in active timeline: %s), with rollback plan: %s",
            rollbackInstantTime, commitInstantOpt.isPresent(), pendingRollbackInfo.isPresent()));
        Option<HoodieRollbackPlan> rollbackPlanOption = pendingRollbackInfo.map(entry -> Option.of(entry.getRollbackPlan()))
            .orElseGet(() -> scheduleRollback(table, rollbackInstantTime, commitInstantOpt, skipLocking));
        if (rollbackPlanOption.isPresent()) {
          // There can be a case where the inflight rollback failed after the instant files
          // are deleted for commitInstantTime, so that commitInstantOpt is empty as it is
          // not present in the timeline.  In such a case, the hoodie instant instance
          // is reconstructed to allow the rollback to be reattempted, and the deleteInstants
          // is set to false since they are already deleted.
          // Execute rollback
          HoodieRollbackMetadata rollbackMetadata = commitInstantOpt.isPresent()
              ? table.rollback(context, rollbackInstantTime, commitInstantOpt.get(), true, skipLocking)
              : table.rollback(context, rollbackInstantTime, new HoodieInstant(
                  true, rollbackPlanOption.get().getInstantToRollback().getAction(), commitInstantTime),
              false, skipLocking);
          if (timerContext != null) {
            long durationInMs = metrics.getDurationInMs(timerContext.stop());
            metrics.updateRollbackMetrics(durationInMs, rollbackMetadata.getTotalFilesDeleted());
          }
          return true;
        } else {
          throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime);
        }
      } else {
        LOG.warn("Cannot find instant " + commitInstantTime + " in the timeline, for rollback");
        return false;
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime, e);
    }
  }

  private Option<HoodieRollbackPlan> scheduleRollback(HoodieTable table, String rollbackInstantTime, Option<HoodieInstant> commitInstantOpt,
                                                      boolean skipLocking) {
    HoodieInstant rollbackInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, rollbackInstantTime, HoodieTimeline.ROLLBACK_ACTION);
    try {
      if (!skipLocking) {
        txnManager.beginTransaction(Option.of(rollbackInstant), Option.empty());
      }
      return table.scheduleRollback(context, rollbackInstantTime, commitInstantOpt.get(), false, config.shouldRollbackUsingMarkers(),
          false);
    } finally {
      if (!skipLocking) {
        txnManager.endTransaction(Option.of(rollbackInstant));
      }
    }
  }

  /**
   * Main API to rollback failed bootstrap.
   */
  public void rollbackFailedBootstrap() {
    LOG.info("Rolling back pending bootstrap if present");
    HoodieTable table = createTable(config, hadoopConf);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingMajorAndMinorCompaction();
    Option<String> instant = Option.fromJavaOptional(
        inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp).findFirst());
    if (instant.isPresent() && HoodieTimeline.compareTimestamps(instant.get(), HoodieTimeline.LESSER_THAN_OR_EQUALS,
        HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
      LOG.info("Found pending bootstrap instants. Rolling them back");
      table.rollbackBootstrap(context, HoodieActiveTimeline.createNewInstantTime());
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

  /**
   * Called after each commit of a compaction or clustering table service,
   * to release any resources used.
   */
  protected void releaseResources(String instantTime) {
    // do nothing here
  }
}
