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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.async.AsyncArchiveService;
import org.apache.hudi.async.AsyncCleanerService;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HeartbeatUtils;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.rollback.RollbackUtils;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.isIndexingCommit;

public abstract class BaseHoodieTableServiceClient<O> extends BaseHoodieClient implements RunsTableService {

  private static final Logger LOG = LogManager.getLogger(BaseHoodieWriteClient.class);

  protected transient Timer.Context compactionTimer;
  protected transient Timer.Context clusteringTimer;
  protected transient Timer.Context logCompactionTimer;

  protected transient AsyncCleanerService asyncCleanerService;
  protected transient AsyncArchiveService asyncArchiveService;

  protected Set<String> pendingInflightAndRequestedInstants;
  protected Set<String> userSpecificPartitions;

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
   * @param metadata commit metadata for which pre commit is being invoked.
   */
  protected void preCommit(HoodieCommitMetadata metadata) {
    // To be overridden by specific engines to perform conflict resolution if any.
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
   * @param compactionInstantTime Compaction Instant Time
   * @return Collection of Write Status
   */
  protected HoodieWriteMetadata<O> logCompact(String compactionInstantTime, boolean shouldComplete) {
    throw new UnsupportedOperationException("Log compaction is not supported yet.");
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
          LOG.info("Running previously failed inflight compaction at instant " + instant);
          compact(instant.getTimestamp(), true);
        });
  }

  protected void runAnyPendingLogCompactions(HoodieTable table) {
    table.getActiveTimeline().getWriteTimeline().filterPendingLogCompactionTimeline().getInstantsAsStream()
        .forEach(instant -> {
          LOG.info("Running previously failed inflight log compaction at instant " + instant);
          logCompact(instant.getTimestamp(), true);
        });
  }

  /***
   * Schedules compaction inline.
   * @param extraMetadata extrametada to be used.
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
  protected abstract HoodieWriteMetadata<O> compact(String compactionInstantTime, boolean shouldComplete);

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param metadata              All the metadata that gets stored along with a commit
   * @param extraMetadata         Extra Metadata to be stored
   */
  public abstract void commitCompaction(String compactionInstantTime, HoodieCommitMetadata metadata,
                                        Option<Map<String, String>> extraMetadata);

  /**
   * Commit Compaction and track metrics.
   */
  protected abstract void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime);

  /**
   * Schedules a new log compaction instant.
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleLogCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleLogCompactionAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new log compaction instant with passed-in instant time.
   * @param instantTime Log Compaction Instant Time
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
    throw new UnsupportedOperationException("Log compaction is not supported yet.");
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
  public abstract HoodieWriteMetadata<O> cluster(String clusteringInstant, boolean shouldComplete);

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

    if (table.getMetaClient().getTableConfig().isLSMBasedLogFormat()) {
      // Do an inline lsm clustering if enabled
      if (config.inlineLSMClusteringEnabled()) {
        metadata.addMetadata(HoodieClusteringConfig.LSM_INLINE_CLUSTERING.key(), "true");
        inlineClustering(table, extraMetadata);
      } else {
        metadata.addMetadata(HoodieClusteringConfig.LSM_INLINE_CLUSTERING.key(), "false");
      }

      // if just inline schedule lsm clustering is enabled
      if (!config.inlineLSMClusteringEnabled() && config.scheduleInlineLSMClustering()
          && table.getActiveTimeline().filterPendingReplaceTimeline().empty()) {
        // proceed only if there are no pending clustering
        metadata.addMetadata(HoodieClusteringConfig.LSM_SCHEDULE_INLINE_CLUSTERING.key(), "true");
        inlineScheduleClustering(extraMetadata);
      }
    } else {
      // Do an inline clustering if enabled
      if (config.inlineClusteringEnabled()) {
        metadata.addMetadata(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
        inlineClustering(table, extraMetadata);
      } else {
        metadata.addMetadata(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "false");
      }

      // if just inline schedule is enabled
      if (!config.inlineClusteringEnabled() && config.scheduleInlineClustering()
          && table.getActiveTimeline().filterPendingReplaceTimeline().empty()) {
        // proceed only if there are no pending clustering
        metadata.addMetadata(HoodieClusteringConfig.SCHEDULE_INLINE_CLUSTERING.key(), "true");
        inlineScheduleClustering(extraMetadata);
      }
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
    // A lock is required to guard against race conditions between an on-going writer and scheduling a table service.
    final Option<HoodieInstant> inflightInstant = Option.of(new HoodieInstant(HoodieInstant.State.REQUESTED,
        tableServiceType.getAction(), instantTime));
    try {
      this.txnManager.beginTransaction(inflightInstant, Option.empty());
      LOG.info("Scheduling table service " + tableServiceType);
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
        LOG.info("Scheduling clustering at instant time :" + instantTime);
        Option<HoodieClusteringPlan> clusteringPlan = table
            .scheduleClustering(context, instantTime, extraMetadata);
        option = clusteringPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      case COMPACT:
        LOG.info("Scheduling compaction at instant time :" + instantTime);
        Option<HoodieCompactionPlan> compactionPlan = userSpecificPartitions != null ? table.scheduleCompaction(context, instantTime, extraMetadata, userSpecificPartitions) :
            table.scheduleCompaction(context, instantTime, extraMetadata);
        option = compactionPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      case LOG_COMPACT:
        LOG.info("Scheduling log compaction at instant time :" + instantTime);
        Option<HoodieCompactionPlan> logCompactionPlan = table
            .scheduleLogCompaction(context, instantTime, extraMetadata);
        option = logCompactionPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      case CLEAN:
        LOG.info("Scheduling cleaning at instant time :" + instantTime);
        Option<HoodieCleanerPlan> cleanerPlan = table.getMetaClient().getTableConfig().isLSMBasedLogFormat()
            ? table.scheduleLSMCleaning(context, instantTime, extraMetadata)
            : table.scheduleCleaning(context, instantTime, extraMetadata);
        option = cleanerPlan.isPresent() ? Option.of(instantTime) : Option.empty();
        break;
      default:
        throw new IllegalArgumentException("Invalid TableService " + tableServiceType);
    }

    Option<String> instantRange = delegateToTableServiceManager(tableServiceType, table);
    if (instantRange.isPresent()) {
      LOG.info("Delegate instant [" + instantRange.get() + "] to table service manager");
    }

    return option;
  }

  protected abstract HoodieTable<?, ?, ?, ?> createTable(HoodieWriteConfig config, Configuration hadoopConf);

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
   * @param extraMetadata extrametadata to use.
   * @return clustering instant if scheduled.
   */
  protected Option<String> inlineScheduleClustering(Option<Map<String, String>> extraMetadata) {
    return scheduleClustering(extraMetadata);
  }

  protected void runAnyPendingClustering(HoodieTable table) {
    table.getActiveTimeline().filterPendingReplaceTimeline().getInstants().forEach(instant -> {
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlan = ClusteringUtils.getClusteringPlan(table.getMetaClient(), instant);
      if (instantPlan.isPresent()) {
        LOG.info("Running pending clustering at instant " + instantPlan.get().getLeft());
        cluster(instant.getTimestamp(), true);
      }
    });
  }

  /**
   * Write the HoodieCommitMetadata to metadata table if available.
   *
   * @param table       {@link HoodieTable} of interest.
   * @param instantTime instant time of the commit.
   * @param actionType  action type of the commit.
   * @param metadata    instance of {@link HoodieCommitMetadata}.
   */
  protected void writeTableMetadata(HoodieTable table, String instantTime, String actionType, HoodieCommitMetadata metadata) {
    checkArgument(table.isTableServiceAction(actionType, instantTime), String.format("Unsupported action: %s.%s is not table service.", actionType, instantTime));
    context.setJobStatus(this.getClass().getSimpleName(), "Committing to metadata table: " + config.getTableName());
    table.getMetadataWriter(instantTime).ifPresent(w -> ((HoodieTableMetadataWriter) w).update(metadata, instantTime, true));
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
    if (config.getAsyncRollbackEnable()) {
      LOG.info("Rollback will not be executed when clean, the rolllback async enable.");
    } else {
      CleanerUtils.rollbackFailedWrites(config.getFailedWritesCleanPolicy(),
          HoodieTimeline.CLEAN_ACTION, () -> rollbackFailedWrites());
    }

    HoodieTable table = createTable(config, hadoopConf);
    if (config.allowMultipleCleans() || !table.getActiveTimeline().getCleanerTimeline().filterInflightsAndRequested().firstInstant().isPresent()) {
      LOG.info("Cleaner started");
      // proceed only if multiple clean schedules are enabled or if there are no pending cleans.
      if (scheduleInline) {
        scheduleTableServiceInternal(cleanInstantTime, Option.empty(), TableServiceType.CLEAN);
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
    return metadata;
  }

  /**
   * Trigger archival for the table. This ensures that the number of commits do not explode
   * and keep increasing unbounded over time.
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
   * Get inflight time line exclude compaction and clustering.
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

  public Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient) {
    return getPendingRollbackInfos(metaClient, true);
  }

  /**
   * Fetch map of pending commits to be rolled-back to {@link HoodiePendingRollbackInfo}.
   * @param metaClient instance of {@link HoodieTableMetaClient} to use.
   * @return map of pending commits to be rolled-back instants to Rollback Instant and Rollback plan Pair.
   */
  protected Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient, boolean ignoreCompactionAndClusteringInstants) {
    List<HoodieInstant> instants = metaClient.getActiveTimeline().filterPendingRollbackTimeline().getInstants();
    Map<String, Option<HoodiePendingRollbackInfo>> infoMap = new HashMap<>();
    Map<String, Path> tempRollbackInstantFileMap = getTempRollbackInstantFile(fs, metaClient);

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
        HoodieInstantInfo instantToRollback = rollbackPlan.getInstantToRollback();

        if (tempRollbackInstantFileMap.containsKey(rollbackInstant.getTimestamp())) {
          // rollback.uuid 文件存在
          Path tmpPath = tempRollbackInstantFileMap.get(rollbackInstant.getTimestamp());
          if (metaClient.getActiveTimeline().containsInstant(instantToRollback.getCommitTime())) {
            // instantToRollback 元数据文件存在, rollback未完成, 需要重新执行, 删除该 rollback.uuid 文件
            LOG.info("Rollback for " + rollbackInstant + " is uncompleted. Should re-execute this rollback. "
                    + "File path: " + tmpPath);
            try {
              fs.delete(tmpPath);
            } catch (IOException e) {
              throw new HoodieIOException("Delete temp rollback instant file failed for " + rollbackInstant, e);
            }
          } else {
            // instantToRollback 元数据文件不存在, rollback已经完成, 不需要重新执行, 重命名该 rollback.uuid 文件
            LOG.info("Rollback for " + rollbackInstant + " is completed. Just rename this temp file. "
                    + "File path: " + tmpPath);
            HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, rollbackInstant.getTimestamp());
            Path targetPath = new Path(tmpPath.getParent(), commitInstant.getFileName());
            try {
              fs.rename(tmpPath, targetPath);
            } catch (IOException e) {
              LOG.warn("Rename temp rollback instant file failed for " + rollbackInstant);
            }
            continue;
          }
        }

        String action = instantToRollback.getAction();
        if (ignoreCompactionAndClusteringInstants) {
          if (!HoodieTimeline.COMPACTION_ACTION.equals(action)) {
            boolean isClustering = HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)
                && ClusteringUtils.getClusteringPlan(metaClient, new HoodieInstant(true, rollbackPlan.getInstantToRollback().getAction(),
                rollbackPlan.getInstantToRollback().getCommitTime())).isPresent();
            if (!isClustering) {
              infoMap.putIfAbsent(instantToRollback.getCommitTime(), Option.of(new HoodiePendingRollbackInfo(rollbackInstant, rollbackPlan)));
            }
          }
        } else {
          infoMap.putIfAbsent(rollbackPlan.getInstantToRollback().getCommitTime(), Option.of(new HoodiePendingRollbackInfo(rollbackInstant, rollbackPlan)));
        }
      } catch (Exception e) {
        LOG.warn("Processing rollback plan failed for " + rollbackInstant + ", skip the plan", e);
      }
    }
    return infoMap;
  }

  /**
   * 获取.hoodie目录下的临时 rollback instant 文件 (instant.rollback.uuid)
   *
   * @param fs
   * @param metaClient
   * @return tempRollbackInstantFileMap [instant timestamp, file_path]
   */
  private Map<String, Path> getTempRollbackInstantFile(FileSystem fs, HoodieTableMetaClient metaClient) {
    Map<String, Path> tempRollbackInstantFileMap = new HashMap<>();
    try {
      FileStatus[] fileStatuses = HoodieTableMetaClient.scanFiles(fs, new Path(metaClient.getMetaPath()), path -> {
        return path.getName().contains(HoodieTimeline.ROLLBACK_ACTION)
                && !path.getName().endsWith(HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION)
                && !path.getName().endsWith(HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION)
                && !path.getName().endsWith(HoodieTimeline.ROLLBACK_EXTENSION);
      });

      Arrays.stream(fileStatuses).forEach(fileStatus -> {
        int dotIndex = fileStatus.getPath().getName().indexOf(".");
        String instantTimestamp = fileStatus.getPath().getName().substring(0, dotIndex);
        tempRollbackInstantFileMap.put(instantTimestamp, fileStatus.getPath());
      });
    } catch (IOException e) {
      throw new HoodieIOException("Failed to scan temp rollback instant meta file", e);
    }
    LOG.info("Find " + tempRollbackInstantFileMap.keySet().size() + " temp rollback instant files: " + tempRollbackInstantFileMap.values());
    return tempRollbackInstantFileMap;
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
    for (Map.Entry<String, Option<HoodiePendingRollbackInfo>> entry : reverseSortedRollbackInstants.entrySet()) {
      if (HoodieTimeline.compareTimestamps(entry.getKey(), HoodieTimeline.LESSER_THAN_OR_EQUALS,
          HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS)) {
        // do we need to handle failed rollback of a bootstrap
        rollbackFailedBootstrap();
        HeartbeatUtils.deleteHeartbeatFile(fs, basePath, entry.getKey(), config);
        break;
      } else {
        rollback(entry.getKey(), entry.getValue(), skipLocking);
        HeartbeatUtils.deleteHeartbeatFile(fs, basePath, entry.getKey(), config);
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
      if (HoodieTableMetadata.isMetadataTable(metaClient.getBasePathV2().toString())) {
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
      List<String> allWriteInstants = getAllWriteInstants(metaClient);
      if (!allWriteInstants.isEmpty() && allWriteInstants.get(0).equals(expiredInstants.get(0))) {
        expiredInstants.remove(0);
      }
      HoodieTimeline refreshedInflightTimeline = getInflightTimelineExcludeCompactionAndClustering(metaClient);
      return expiredInstants.stream().filter(refreshedInflightTimeline::containsInstant).collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  private List<String> getAllWriteInstants(HoodieTableMetaClient metaClient) {
    HoodieTableType tableType = metaClient.getTableConfig().getTableType();
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      return metaClient.getActiveTimeline().getCommitTimeline().filter(instant -> instant.getAction().equals(HoodieTimeline.COMMIT_ACTION))
        .getReverseOrderedInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    }
    return metaClient.getActiveTimeline().getDeltaCommitTimeline().getReverseOrderedInstants()
      .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
  }

  /**
   * @Deprecated
   * Rollback the inflight record changes with the given commit time. This
   * will be removed in future in favor of {@link BaseHoodieWriteClient#restoreToInstant(String, boolean)
   *
   * @param commitInstantTime Instant time of the commit
   * @param pendingRollbackInfo pending rollback instant and plan if rollback failed from previous attempt.
   * @param skipLocking if this is triggered by another parent transaction, locking can be skipped.
   * @throws HoodieRollbackException if rollback cannot be performed successfully
   */
  @Deprecated
  public Pair<Boolean, Option<HoodieRollbackMetadata>> rollback(final String commitInstantTime,
                                                                Option<HoodiePendingRollbackInfo> pendingRollbackInfo, boolean skipLocking) throws HoodieRollbackException {
    LOG.info("Begin rollback of instant " + commitInstantTime);
    final String rollbackInstantTime = pendingRollbackInfo.map(entry -> entry.getRollbackInstant().getTimestamp()).orElse(HoodieActiveTimeline.createNewInstantTime());
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
            .orElseGet(() -> table.scheduleRollback(context, rollbackInstantTime, commitInstantOpt.get(), false, config.shouldRollbackUsingMarkers()));
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
          return Pair.of(true, Option.of(rollbackMetadata));
        } else {
          throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime);
        }
      } else {
        LOG.warn("Cannot find instant " + commitInstantTime + " in the timeline, for rollback");
        return Pair.of(false, Option.empty());
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

  public void setUserSpecificPartitions(Set<String> userSpecificPartitions) {
    this.userSpecificPartitions = userSpecificPartitions;
  }
}
