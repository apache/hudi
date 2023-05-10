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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
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
import java.util.stream.Collectors;

public class HoodieFlinkTableServiceClient<T> extends BaseHoodieTableServiceClient<List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkTableServiceClient.class);

  /**
   * Cached metadata writer for coordinator to reuse for each commit.
   */
  private HoodieBackedTableMetadataWriter metadataWriter;

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
      writeTableMetadata(table, compactionCommitTime, compactionInstant.getAction(), metadata);
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

  protected void completeClustering(
      HoodieReplaceCommitMetadata metadata,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String clusteringCommitTime) {
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
      // commit to data table after committing to metadata table.
      // Do not do any conflict resolution here as we do with regular writes. We take the lock here to ensure all writes to metadata table happens within a
      // single lock (single writer). Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
      writeTableMetadata(table, clusteringCommitTime, clusteringInstant.getAction(), metadata);
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
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  public HoodieFlinkTable<?> getHoodieTable() {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  @Override
  public void writeTableMetadata(HoodieTable table, String instantTime, String actionType, HoodieCommitMetadata metadata) {
    try (HoodieBackedTableMetadataWriter metadataWriter = initMetadataWriter()) {
      metadataWriter.update(metadata, instantTime);
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
  private HoodieBackedTableMetadataWriter initMetadataWriter() {
    return (HoodieBackedTableMetadataWriter) FlinkHoodieBackedTableMetadataWriter.create(
        FlinkClientUtil.getHadoopConf(), this.config, HoodieFlinkEngineContext.DEFAULT);
  }

  public void initMetadataTable() {
    HoodieFlinkTable<?> table = getHoodieTable();
    if (config.isMetadataTableEnabled()) {
      // initialize the metadata table path
      // guard the metadata writer with concurrent lock
      try {
        this.txnManager.getLockManager().lock();
        try (HoodieBackedTableMetadataWriter metadataWriter = initMetadataWriter()) {
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
