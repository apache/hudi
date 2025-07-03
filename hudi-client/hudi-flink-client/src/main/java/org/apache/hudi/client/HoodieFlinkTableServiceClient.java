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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.FlinkClientUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;

public class HoodieFlinkTableServiceClient<T> extends BaseHoodieTableServiceClient<List<HoodieRecord<T>>, List<WriteStatus>, List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkTableServiceClient.class);

  protected HoodieFlinkTableServiceClient(HoodieEngineContext context,
                                          HoodieWriteConfig clientConfig,
                                          Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    // only used for metadata table, the compaction happens in single thread
    HoodieWriteMetadata<List<WriteStatus>> compactionMetadata = createTable(config, storageConf).compact(context, compactionInstantTime);
    commitCompaction(compactionInstantTime, compactionMetadata, Option.empty());
    return compactionMetadata;
  }

  @Override
  protected TableWriteStats triggerWritesAndFetchWriteStats(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return new TableWriteStats(writeMetadata.getWriteStatuses().stream().map(writeStatus -> writeStatus.getStat()).collect(Collectors.toList()));
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime, List<HoodieWriteStat> partialMetadataWriteStats) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction: " + config.getTableName());
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    final HoodieInstant compactionInstant = table.getInstantGenerator().getCompactionInflightInstant(compactionCommitTime);
    try {
      this.txnManager.beginStateChange(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      // Do not do any conflict resolution here as we do with regular writes. We take the lock here to ensure all writes to metadata table happens within a
      // single lock (single writer). Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
      writeTableMetadata(table, compactionCommitTime, metadata);
      LOG.info("Committing Compaction {} finished with result {}.", compactionCommitTime, metadata);
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionCommitTime, metadata);
    } finally {
      this.txnManager.endStateChange(Option.of(compactionInstant));
    }
    WriteMarkersFactory
        .get(config.getMarkersType(), table, compactionCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      try {
        metrics.updateCommitMetrics(TimelineUtils.parseDateFromInstantTime(compactionCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException("Commit time is not of valid format. Failed to commit compaction "
            + config.getBasePath() + " at time " + compactionCommitTime, e);
      }
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
  }

  protected void completeClustering(
      HoodieReplaceCommitMetadata metadata,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String clusteringCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect clustering write status and commit clustering");
    HoodieInstant clusteringInstant = ClusteringUtils.getInflightClusteringInstant(clusteringCommitTime, table.getActiveTimeline(), table.getInstantGenerator()).get();
    List<HoodieWriteStat> writeStats = metadata.getPartitionToWriteStats().entrySet().stream().flatMap(e ->
        e.getValue().stream()).collect(Collectors.toList());
    if (writeStats.stream().mapToLong(HoodieWriteStat::getTotalWriteErrors).sum() > 0) {
      throw new HoodieClusteringException("Clustering failed to write to files:"
          + writeStats.stream().filter(s -> s.getTotalWriteErrors() > 0L).map(HoodieWriteStat::getFileId).collect(Collectors.joining(",")));
    }

    try {
      this.txnManager.beginStateChange(Option.of(clusteringInstant), Option.empty());
      finalizeWrite(table, clusteringCommitTime, writeStats);
      // Only in some cases conflict resolution needs to be performed.
      // So, check if preCommit method that does conflict resolution needs to be triggered.
      if (isPreCommitRequired()) {
        preCommit(metadata);
      }
      // commit to data table after committing to metadata table.
      // We take the lock here to ensure all writes to metadata table happens within a single lock (single writer).
      // Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
      writeTableMetadata(table, clusteringCommitTime, metadata);

      LOG.info("Committing Clustering {} finished with result {}.", clusteringCommitTime, metadata);
      ClusteringUtils.transitionClusteringOrReplaceInflightToComplete(
          false,
          clusteringInstant,
          metadata,
          table.getActiveTimeline(),
          completedInstant -> table.getMetaClient().getTableFormat().commit(metadata, completedInstant, table.getContext(), table.getMetaClient(), table.getViewManager()));
    } catch (HoodieIOException e) {
      throw new HoodieClusteringException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + clusteringCommitTime, e);
    } finally {
      this.txnManager.endStateChange(Option.of(clusteringInstant));
    }

    WriteMarkersFactory.get(config.getMarkersType(), table, clusteringCommitTime)
        .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    if (clusteringTimer != null) {
      long durationInMs = metrics.getDurationInMs(clusteringTimer.stop());
      try {
        metrics.updateCommitMetrics(TimelineUtils.parseDateFromInstantTime(clusteringCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.CLUSTERING_ACTION);
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
  protected HoodieWriteMetadata<List<WriteStatus>> convertToOutputMetadata(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return writeMetadata;
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
    return createTableAndValidate(config, HoodieFlinkTable::create, skipValidation);
  }

  /**
   * Initialize the table metadata writer, for e.g, bootstrap the metadata table
   * from the filesystem if it does not exist.
   */
  private HoodieBackedTableMetadataWriter initMetadataWriter(Option<String> latestPendingInstant) {
    return (HoodieBackedTableMetadataWriter) FlinkHoodieBackedTableMetadataWriter.create(
        HadoopFSUtils.getStorageConf(FlinkClientUtil.getHadoopConf()), this.config, HoodieFlinkEngineContext.DEFAULT, latestPendingInstant);
  }

  public void initMetadataTable() {
    HoodieFlinkTable<?> table = (HoodieFlinkTable<?>) createTable(config, storageConf, false);
    if (config.isMetadataTableEnabled()) {
      Option<String> latestPendingInstant = table.getActiveTimeline()
          .filterInflightsAndRequested().lastInstant().map(HoodieInstant::requestedTime);
      try {
        // initialize the metadata table path
        // guard the metadata writer with concurrent lock
        this.txnManager.getLockManager().lock();
        try (HoodieBackedTableMetadataWriter metadataWriter = initMetadataWriter(latestPendingInstant)) {
          if (metadataWriter.isInitialized()) {
            metadataWriter.performTableServices(Option.empty());
          }
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

  @Override
  protected void handleWriteErrors(List<HoodieWriteStat> writeStats, TableServiceType tableServiceType) {
    // No-op
  }
}
