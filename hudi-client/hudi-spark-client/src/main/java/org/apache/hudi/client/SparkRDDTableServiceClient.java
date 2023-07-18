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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieLogCompactException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;

public class SparkRDDTableServiceClient<T> extends BaseHoodieTableServiceClient<JavaRDD<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRDDTableServiceClient.class);

  protected SparkRDDTableServiceClient(HoodieEngineContext context,
                                       HoodieWriteConfig clientConfig,
                                       Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  @Override
  protected HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
      table.getMetaClient().reloadActiveTimeline();
    }
    compactionTimer = metrics.getCompactionCtx();
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = table.compact(context, compactionInstantTime);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = writeMetadata.clone(HoodieJavaRDD.getJavaRDD(writeMetadata.getWriteStatuses()));
    if (shouldComplete && compactionMetadata.getCommitMetadata().isPresent()) {
      completeTableService(TableServiceType.COMPACT, compactionMetadata.getCommitMetadata().get(), table, compactionInstantTime,
          Option.ofNullable(HoodieJavaRDD.of(compactionMetadata.getWriteStatuses())));
    }
    return compactionMetadata;
  }

  @Override
  protected HoodieWriteMetadata<JavaRDD<WriteStatus>> logCompact(String logCompactionInstantTime, boolean shouldComplete) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);

    // Check if a commit or compaction instant with a greater timestamp is on the timeline.
    // If a instant is found then abort log compaction, since it is no longer needed.
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
      table.rollbackInflightLogCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
      table.getMetaClient().reloadActiveTimeline();
      throw new HoodieException("Execution is aborted since it found an Inflight logcompaction,"
          + "log compaction plans are mutable plans, so reschedule another logcompaction.");
    }
    logCompactionTimer = metrics.getLogCompactionCtx();
    WriteMarkersFactory.get(config.getMarkersType(), table, logCompactionInstantTime);
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = table.logCompact(context, logCompactionInstantTime);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> logCompactionMetadata = writeMetadata.clone(HoodieJavaRDD.getJavaRDD(writeMetadata.getWriteStatuses()));
    if (shouldComplete && logCompactionMetadata.getCommitMetadata().isPresent()) {
      completeTableService(TableServiceType.LOG_COMPACT, logCompactionMetadata.getCommitMetadata().get(), table, logCompactionInstantTime,
          Option.ofNullable(HoodieJavaRDD.of(logCompactionMetadata.getWriteStatuses())));
    }
    return logCompactionMetadata;
  }

  @Override
  public void commitCompaction(String compactionInstantTime, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, table, compactionInstantTime);
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction: " + config.getTableName());
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    handleWriteErrors(writeStats, TableServiceType.COMPACT);
    final HoodieInstant compactionInstant = HoodieTimeline.getCompactionInflightInstant(compactionCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      updateTableMetadata(table, metadata, compactionInstant, context.emptyHoodieData());
      LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionCommitTime, metadata);
    } finally {
      this.txnManager.endTransaction(Option.of(compactionInstant));
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, compactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      HoodieActiveTimeline.parseDateFromInstantTimeSafely(compactionCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, COMPACTION_ACTION)
      );
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
  }

  @Override
  protected void completeLogCompaction(HoodieCommitMetadata metadata,
                                       HoodieTable table,
                                       String logCompactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect log compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    handleWriteErrors(writeStats, TableServiceType.LOG_COMPACT);
    final HoodieInstant logCompactionInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, logCompactionCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(logCompactionInstant), Option.empty());
      preCommit(metadata);
      finalizeWrite(table, logCompactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      updateTableMetadata(table, metadata, logCompactionInstant, context.emptyHoodieData());
      LOG.info("Committing Log Compaction " + logCompactionCommitTime + ". Finished with result " + metadata);
      CompactHelpers.getInstance().completeInflightLogCompaction(table, logCompactionCommitTime, metadata);
    } finally {
      this.txnManager.endTransaction(Option.of(logCompactionInstant));
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, logCompactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (logCompactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(logCompactionTimer.stop());
      HoodieActiveTimeline.parseDateFromInstantTimeSafely(logCompactionCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, HoodieActiveTimeline.LOG_COMPACTION_ACTION)
      );
    }
    LOG.info("Log Compacted successfully on commit " + logCompactionCommitTime);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> cluster(String clusteringInstant, boolean shouldComplete) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);
    HoodieTimeline pendingClusteringTimeline = table.getActiveTimeline().filterPendingReplaceTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getReplaceCommitInflightInstant(clusteringInstant);
    if (pendingClusteringTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightClustering(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
      table.getMetaClient().reloadActiveTimeline();
    }
    clusteringTimer = metrics.getClusteringCtx();
    LOG.info("Starting clustering at " + clusteringInstant);
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = table.cluster(context, clusteringInstant);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> clusteringMetadata = writeMetadata.clone(HoodieJavaRDD.getJavaRDD(writeMetadata.getWriteStatuses()));
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
      completeTableService(TableServiceType.CLUSTER, clusteringMetadata.getCommitMetadata().get(), table, clusteringInstant,
          Option.ofNullable(HoodieJavaRDD.of(clusteringMetadata.getWriteStatuses())));
    }
    return clusteringMetadata;
  }

  // TODO : To enforce priority between table service and ingestion writer, use transactions here and invoke strategy
  private void completeTableService(TableServiceType tableServiceType,
                                    HoodieCommitMetadata metadata,
                                    HoodieTable table,
                                    String commitInstant,
                                    Option<HoodieData<WriteStatus>> writeStatuses) {

    switch (tableServiceType) {
      case CLUSTER:
        completeClustering((HoodieReplaceCommitMetadata) metadata, table, commitInstant, writeStatuses);
        break;
      case COMPACT:
        completeCompaction(metadata, table, commitInstant);
        break;
      case LOG_COMPACT:
        completeLogCompaction(metadata, table, commitInstant);
        break;
      default:
        throw new IllegalArgumentException("This table service is not valid " + tableServiceType);
    }
  }

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
      updateTableMetadata(table, metadata, clusteringInstant, writeStatuses.orElse(context.emptyHoodieData()));

      LOG.info("Committing Clustering " + clusteringCommitTime + ". Finished with result " + metadata);

      table.getActiveTimeline().transitionReplaceInflightToComplete(
          clusteringInstant,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new HoodieClusteringException("unable to transition clustering inflight to complete: " + clusteringCommitTime, e);
    } finally {
      this.txnManager.endTransaction(Option.of(clusteringInstant));
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, clusteringCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (clusteringTimer != null) {
      long durationInMs = metrics.getDurationInMs(clusteringTimer.stop());
      HoodieActiveTimeline.parseDateFromInstantTimeSafely(clusteringCommitTime).ifPresent(parsedInstant ->
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, HoodieActiveTimeline.REPLACE_COMMIT_ACTION)
      );
    }
    LOG.info("Clustering successfully on commit " + clusteringCommitTime);
  }

  private void handleWriteErrors(List<HoodieWriteStat> writeStats, TableServiceType tableServiceType) {
    if (writeStats.stream().mapToLong(HoodieWriteStat::getTotalWriteErrors).sum() > 0) {
      throw new HoodieClusteringException(tableServiceType + " failed to write to files:"
          + writeStats.stream().filter(s -> s.getTotalWriteErrors() > 0L).map(HoodieWriteStat::getFileId).collect(Collectors.joining(",")));
    }
  }

  private void validateClusteringCommit(HoodieWriteMetadata<JavaRDD<WriteStatus>> clusteringMetadata, String clusteringCommitTime, HoodieTable table) {
    if (clusteringMetadata.getWriteStatuses().isEmpty()) {
      HoodieClusteringPlan clusteringPlan = ClusteringUtils.getClusteringPlan(
              table.getMetaClient(), HoodieTimeline.getReplaceCommitRequestedInstant(clusteringCommitTime))
          .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException(
              "Unable to read clustering plan for instant: " + clusteringCommitTime));
      throw new HoodieClusteringException("Clustering plan produced 0 WriteStatus for " + clusteringCommitTime
          + " #groups: " + clusteringPlan.getInputGroups().size() + " expected at least "
          + clusteringPlan.getInputGroups().stream().mapToInt(HoodieClusteringGroup::getNumOutputFileGroups).sum()
          + " write statuses");
    }
  }

  private void updateTableMetadata(HoodieTable table, HoodieCommitMetadata commitMetadata,
                                   HoodieInstant hoodieInstant,
                                   HoodieData<WriteStatus> writeStatuses) {
    // Do not do any conflict resolution here as we do with regular writes. We take the lock here to ensure all writes to metadata table happens within a
    // single lock (single writer). Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
    table.getMetadataWriter(hoodieInstant.getTimestamp())
        .ifPresent(writer -> ((HoodieTableMetadataWriter) writer).update(commitMetadata, writeStatuses, hoodieInstant.getTimestamp()));
  }

  @Override
  protected HoodieTable<?, ?, ?, ?> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieSparkTable.create(config, context);
  }

  @Override
  protected void preCommit(HoodieCommitMetadata metadata) {
    // Create a Hoodie table after startTxn which encapsulated the commits and files visible.
    // Important to create this after the lock to ensure the latest commits show up in the timeline without need for reload
    HoodieTable table = createTable(config, hadoopConf);
    resolveWriteConflict(table, metadata, this.pendingInflightAndRequestedInstants);
  }
}
