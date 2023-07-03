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

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieLogCompactException;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.FlinkClientUtil;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;

public class HoodieFlinkTableServiceClient<T> extends BaseHoodieTableServiceClient<List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkTableServiceClient.class);

  protected HoodieFlinkTableServiceClient(HoodieEngineContext context,
                                          HoodieWriteConfig clientConfig,
                                          Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    // only used for metadata table, the compaction happens in single thread
    HoodieWriteMetadata<List<WriteStatus>> compactionMetadata = getHoodieTable().compact(context, compactionInstantTime);
    commitCompaction(compactionInstantTime, compactionMetadata.getCommitMetadata().get(), Option.empty());
    return compactionMetadata;
  }

  @Override
  public void commitCompaction(String compactionInstantTime, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, getHoodieTable(), compactionInstantTime);
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction: " + config.getTableName());
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    final HoodieInstant compactionInstant = HoodieTimeline.getCompactionInflightInstant(compactionCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      // Do not do any conflict resolution here as we do with regular writes. We take the lock here to ensure all writes to metadata table happens within a
      // single lock (single writer). Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
      writeTableMetadata(table, compactionCommitTime, compactionInstant.getAction(), metadata, context.emptyHoodieData());
      LOG.info("Committing Compaction {} finished with result {}.", compactionCommitTime, metadata);
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionCommitTime, metadata);
    } finally {
      this.txnManager.endTransaction(Option.of(compactionInstant));
    }
    WriteMarkersFactory
        .get(config.getMarkersType(), table, compactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      try {
        metrics.updateCommitMetrics(HoodieActiveTimeline.parseDateFromInstantTime(compactionCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException("Commit time is not of valid format. Failed to commit compaction "
            + config.getBasePath() + " at time " + compactionCommitTime, e);
      }
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> logCompact(String logCompactionInstantTime, boolean shouldComplete) {
    HoodieFlinkTable<T> table = HoodieFlinkTable.create(config, context);

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
      table.rollbackInflightLogCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
      table.getMetaClient().reloadActiveTimeline();
      throw new HoodieException("Execution is aborted since it found an Inflight logcompaction,"
          + "log compaction plans are mutable plans, so reschedule another logcompaction.");
    }
    logCompactionTimer = metrics.getLogCompactionCtx();
    WriteMarkersFactory.get(config.getMarkersType(), table, logCompactionInstantTime);
    HoodieWriteMetadata<List<WriteStatus>> writeMetadata = table.logCompact(context, logCompactionInstantTime);
    if (shouldComplete && writeMetadata.getCommitMetadata().isPresent()) {
      completeLogCompaction(writeMetadata.getCommitMetadata().get(), table, logCompactionInstantTime);
    }
    return writeMetadata;
  }

  @Override
  protected void completeLogCompaction(HoodieCommitMetadata metadata,
                                       HoodieTable table,
                                       String logCompactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect log compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    final HoodieInstant logCompactionInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, logCompactionCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(logCompactionInstant), Option.empty());
      preCommit(metadata);
      finalizeWrite(table, logCompactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      writeTableMetadata(table, logCompactionCommitTime, HoodieTimeline.LOG_COMPACTION_ACTION, metadata, context.emptyHoodieData());
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

  protected void completeClustering(
      HoodieReplaceCommitMetadata metadata,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String clusteringCommitTime,
      Option<HoodieData<WriteStatus>> writeStatuses) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect clustering write status and commit clustering");
    HoodieInstant clusteringInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, clusteringCommitTime);
    List<HoodieWriteStat> writeStats = metadata.getPartitionToWriteStats().entrySet().stream().flatMap(e ->
        e.getValue().stream()).collect(Collectors.toList());
    if (writeStats.stream().mapToLong(HoodieWriteStat::getTotalWriteErrors).sum() > 0) {
      throw new HoodieClusteringException("Clustering failed to write to files:"
          + writeStats.stream().filter(s -> s.getTotalWriteErrors() > 0L).map(HoodieWriteStat::getFileId).collect(Collectors.joining(",")));
    }

    try {
      this.txnManager.beginTransaction(Option.of(clusteringInstant), Option.empty());
      finalizeWrite(table, clusteringCommitTime, writeStats);
      // Only in some cases conflict resolution needs to be performed.
      // So, check if preCommit method that does conflict resolution needs to be triggered.
      if (isPreCommitRequired()) {
        preCommit(metadata);
      }
      // commit to data table after committing to metadata table.
      // We take the lock here to ensure all writes to metadata table happens within a single lock (single writer).
      // Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
      writeTableMetadata(table, clusteringCommitTime, clusteringInstant.getAction(), metadata, writeStatuses.orElse(context.emptyHoodieData()));

      LOG.info("Committing Clustering {} finished with result {}.", clusteringCommitTime, metadata);
      table.getActiveTimeline().transitionReplaceInflightToComplete(
          HoodieTimeline.getReplaceCommitInflightInstant(clusteringCommitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieClusteringException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + clusteringCommitTime, e);
    } finally {
      this.txnManager.endTransaction(Option.of(clusteringInstant));
    }

    WriteMarkersFactory.get(config.getMarkersType(), table, clusteringCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (clusteringTimer != null) {
      long durationInMs = metrics.getDurationInMs(clusteringTimer.stop());
      try {
        metrics.updateCommitMetrics(HoodieActiveTimeline.parseDateFromInstantTime(clusteringCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.REPLACE_COMMIT_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException("Commit time is not of valid format. Failed to commit compaction "
            + config.getBasePath() + " at time " + clusteringCommitTime, e);
      }
    }
    LOG.info("Clustering successfully on commit " + clusteringCommitTime);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(String clusteringInstant, boolean shouldComplete) {
    return null;
  }

  @Override
  protected HoodieTable<?, ?, ?, ?> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieFlinkTable.create(config, context);
  }

  public HoodieFlinkTable<?> getHoodieTable() {
    return HoodieFlinkTable.create(config, context);
  }

  @Override
  public void writeTableMetadata(HoodieTable table, String instantTime, String actionType, HoodieCommitMetadata metadata, HoodieData<WriteStatus> writeStatuses) {
    try (HoodieBackedTableMetadataWriter metadataWriter = initMetadataWriter(Option.empty())) {
      metadataWriter.update(metadata, writeStatuses, instantTime);
    } catch (Exception e) {
      throw new HoodieException("Failed to update metadata", e);
    }
  }

  @Override
  protected void preCommit(HoodieCommitMetadata metadata) {
    // Create a Hoodie table after startTxn which encapsulated the commits and files visible.
    // Important to create this after the lock to ensure the latest commits show up in the timeline without need for reload
    HoodieTable table = createTable(config, hadoopConf);
    resolveWriteConflict(table, metadata, this.pendingInflightAndRequestedInstants);
  }

  /**
   * Initialize the table metadata writer, for e.g, bootstrap the metadata table
   * from the filesystem if it does not exist.
   */
  private HoodieBackedTableMetadataWriter initMetadataWriter(Option<String> latestPendingInstant) {
    return (HoodieBackedTableMetadataWriter) FlinkHoodieBackedTableMetadataWriter.create(
        FlinkClientUtil.getHadoopConf(), this.config, HoodieFlinkEngineContext.DEFAULT, latestPendingInstant);
  }

  public void initMetadataTable() {
    HoodieFlinkTable<?> table = getHoodieTable();
    if (config.isMetadataTableEnabled()) {
      Option<String> latestPendingInstant = table.getActiveTimeline()
          .filterInflightsAndRequested().lastInstant().map(HoodieInstant::getTimestamp);
      try {
        // initialize the metadata table path
        // guard the metadata writer with concurrent lock
        this.txnManager.getLockManager().lock();
        try (HoodieBackedTableMetadataWriter metadataWriter = initMetadataWriter(latestPendingInstant)) {
          metadataWriter.performTableServices(Option.empty());
        }
      } catch (Exception e) {
        throw new HoodieException("Failed to initialize metadata table", e);
      } finally {
        this.txnManager.getLockManager().unlock();
      }
      // clean the obsolete index stats
      table.deleteMetadataIndexIfNecessary();
    } else {
      // delete the metadata table if it was enabled but is now disabled
      table.maybeDeleteMetadataTable();
    }
  }
}
