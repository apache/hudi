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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
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
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

public abstract class BaseTableServiceClient<O> extends CommonHoodieClient {

  private static final Logger LOG = LogManager.getLogger(BaseHoodieWriteClient.class);

  protected BaseTableServiceClient(HoodieEngineContext context, HoodieWriteConfig clientConfig, HoodieMetrics metrics) {
    super(context, clientConfig, Option.empty());
  }

  protected boolean tableServicesEnabled(HoodieWriteConfig config) {
    boolean enabled = config.areTableServicesEnabled();
    if (!enabled) {
      LOG.warn(String.format("Table services are disabled. Set `%s` to enable.", HoodieWriteConfig.TABLE_SERVICES_ENABLED));
    }
    return enabled;
  }

  protected boolean delegateToTableManagerService(HoodieWriteConfig config, ActionType actionType) {
    boolean supportsAction = config.getTableManagerConfig().isTableManagerSupportsAction(actionType);
    if (supportsAction) {
      LOG.warn(actionType.name() + " delegate to table manager service!");
    }
    return supportsAction;
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
    if (delegateToTableManagerService(config, ActionType.compaction)) {
      scheduleCompaction(extraMetadata);
    } else {
      runAnyPendingCompactions(table);
      inlineCompaction(extraMetadata);
    }
  }

  protected void runAnyPendingCompactions(HoodieTable table) {
    table.getActiveTimeline().getWriteTimeline().filterPendingCompactionTimeline().getInstants()
        .forEach(instant -> {
          LOG.info("Running previously failed inflight compaction at instant " + instant);
          compact(instant.getTimestamp(), true);
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
   * Schedules a new cleaning instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  protected Option<String> scheduleCleaning(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleCleaningAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new cleaning instant with passed-in instant time.
   *
   * @param instantTime   cleaning Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  protected boolean scheduleCleaningAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return scheduleTableService(instantTime, extraMetadata, TableServiceType.CLEAN).isPresent();
  }

  /**
   * Ensures clustering instant is in expected state and performs clustering for the plan stored in metadata.
   *
   * @param clusteringInstant Clustering Instant Time
   * @return Collection of Write Status
   */
  public abstract HoodieWriteMetadata<O> cluster(String clusteringInstant, boolean shouldComplete);

  protected void runTableServicesInline(HoodieTable table, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
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
        && !table.getActiveTimeline().getWriteTimeline().filterPendingCompactionTimeline().getInstants().findAny().isPresent()) {
      // proceed only if there are no pending compactions
      metadata.addMetadata(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key(), "true");
      inlineScheduleCompaction(extraMetadata);
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
        && !table.getActiveTimeline().filterPendingReplaceTimeline().getInstants().findAny().isPresent()) {
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
  public Option<String> scheduleTableService(Option<Map<String, String>> extraMetadata, TableServiceType tableServiceType) {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    return scheduleTableService(instantTime, extraMetadata, tableServiceType);
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
    switch (tableServiceType) {
      case ARCHIVE:
        LOG.info("Scheduling archiving is not supported. Skipping.");
        return Option.empty();
      case CLUSTER:
        LOG.info("Scheduling clustering at instant time :" + instantTime);
        Option<HoodieClusteringPlan> clusteringPlan = createTable(config, hadoopConf)
            .scheduleClustering(context, instantTime, extraMetadata);
        return clusteringPlan.isPresent() ? Option.of(instantTime) : Option.empty();
      case COMPACT:
        LOG.info("Scheduling compaction at instant time :" + instantTime);
        Option<HoodieCompactionPlan> compactionPlan = createTable(config, hadoopConf)
            .scheduleCompaction(context, instantTime, extraMetadata);
        return compactionPlan.isPresent() ? Option.of(instantTime) : Option.empty();
      case CLEAN:
        LOG.info("Scheduling cleaning at instant time :" + instantTime);
        Option<HoodieCleanerPlan> cleanerPlan = createTable(config, hadoopConf)
            .scheduleCleaning(context, instantTime, extraMetadata);
        return cleanerPlan.isPresent() ? Option.of(instantTime) : Option.empty();
      default:
        throw new IllegalArgumentException("Invalid TableService " + tableServiceType);
    }
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
    if (delegateToTableManagerService(config, ActionType.replacecommit)) {
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
   * Finalize Write operation.
   *
   * @param table       HoodieTable
   * @param instantTime Instant Time
   * @param stats       Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(context, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          LOG.info("Finalize write elapsed time (milliseconds): " + duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
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
    context.setJobStatus(this.getClass().getSimpleName(), "Committing to metadata table: " + config.getTableName());
    table.getMetadataWriter(instantTime).ifPresent(w -> ((HoodieTableMetadataWriter) w).update(metadata, instantTime,
        table.isTableServiceAction(actionType)));
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned). This API provides the flexibility to schedule clean instant asynchronously via
   * {@link BaseTableServiceClient#scheduleTableService(String, Option, TableServiceType)} and disable inline scheduling
   * of clean.
   *
   * @param cleanInstantTime instant time for clean.
   * @param scheduleInline   true if needs to be scheduled inline. false otherwise.
   * @param skipLocking      if this is triggered by another parent transaction, locking can be skipped.
   */
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline, boolean skipLocking) throws HoodieIOException {
    if (!tableServicesEnabled(config)) {
      return null;
    }
    final Timer.Context timerContext = metrics.getCleanCtx();
    CleanerUtils.rollbackFailedWrites(config.getFailedWritesCleanPolicy(),
        HoodieTimeline.CLEAN_ACTION, () -> rollbackFailedWrites(skipLocking));

    HoodieCleanMetadata metadata = null;
    HoodieTable table = createTable(config, hadoopConf);
    if (config.allowMultipleCleans() || !table.getActiveTimeline().getCleanerTimeline().filterInflightsAndRequested().firstInstant().isPresent()) {
      LOG.info("Cleaner started");
      // proceed only if multiple clean schedules are enabled or if there are no pending cleans.
      if (scheduleInline) {
        scheduleTableServiceInternal(cleanInstantTime, Option.empty(), TableServiceType.CLEAN);
        table.getMetaClient().reloadActiveTimeline();
      }

      if (delegateToTableManagerService(config, ActionType.clean)) {
        return null;
      }
      metadata = table.clean(context, cleanInstantTime, skipLocking);
      if (timerContext != null && metadata != null) {
        long durationMs = metrics.getDurationInMs(timerContext.stop());
        metrics.updateCleanMetrics(durationMs, metadata.getTotalFilesDeleted());
        LOG.info("Cleaned " + metadata.getTotalFilesDeleted() + " files"
            + " Earliest Retained Instant :" + metadata.getEarliestCommitToRetain()
            + " cleanerElapsedMs" + durationMs);
      }
    }
    return metadata;
  }
}
