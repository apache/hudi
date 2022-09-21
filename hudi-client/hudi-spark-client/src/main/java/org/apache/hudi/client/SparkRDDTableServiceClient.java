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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metrics.HoodieMetrics;
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
import java.util.stream.Collectors;

public class SparkRDDTableServiceClient<T extends HoodieRecordPayload> extends BaseTableServiceClient<JavaRDD<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRDDTableServiceClient.class);

  protected SparkRDDTableServiceClient(HoodieEngineContext context, HoodieWriteConfig clientConfig, HoodieMetrics metrics) {
    super(context, clientConfig, metrics);
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
      completeTableService(TableServiceType.COMPACT, compactionMetadata.getCommitMetadata().get(), table, compactionInstantTime);
    }
    return compactionMetadata;
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
    final HoodieInstant compactionInstant = HoodieTimeline.getCompactionInflightInstant(compactionCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      updateTableMetadata(table, metadata, compactionInstant);
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
          metrics.updateCommitMetrics(parsedInstant.getTime(), durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION)
      );
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
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
    // TODO : Where is shouldComplete used ?
    if (shouldComplete && clusteringMetadata.getCommitMetadata().isPresent()) {
      completeTableService(TableServiceType.CLUSTER, clusteringMetadata.getCommitMetadata().get(), table, clusteringInstant);
    }
    return clusteringMetadata;
  }

  // TODO : To enforce priority between table service and ingestion writer, use transactions here and invoke strategy
  private void completeTableService(TableServiceType tableServiceType, HoodieCommitMetadata metadata,
                                    HoodieTable table,
                                    String commitInstant) {

    switch (tableServiceType) {
      case CLUSTER:
        completeClustering((HoodieReplaceCommitMetadata) metadata, table, commitInstant);
        break;
      case COMPACT:
        completeCompaction(metadata, table, commitInstant);
        break;
      default:
        throw new IllegalArgumentException("This table service is not valid " + tableServiceType);
    }
  }

  private void completeClustering(HoodieReplaceCommitMetadata metadata,
                                  HoodieTable table,
                                  String clusteringCommitTime) {
    List<HoodieWriteStat> writeStats = metadata.getPartitionToWriteStats().entrySet().stream().flatMap(e ->
        e.getValue().stream()).collect(Collectors.toList());

    if (writeStats.stream().mapToLong(s -> s.getTotalWriteErrors()).sum() > 0) {
      throw new HoodieClusteringException("Clustering failed to write to files:"
          + writeStats.stream().filter(s -> s.getTotalWriteErrors() > 0L).map(s -> s.getFileId()).collect(Collectors.joining(",")));
    }

    final HoodieInstant clusteringInstant = HoodieTimeline.getReplaceCommitInflightInstant(clusteringCommitTime);
    try {
      this.txnManager.beginTransaction(Option.of(clusteringInstant), Option.empty());

      finalizeWrite(table, clusteringCommitTime, writeStats);
      // Update table's metadata (table)
      updateTableMetadata(table, metadata, clusteringInstant);

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

  private void updateTableMetadata(HoodieTable table, HoodieCommitMetadata commitMetadata,
                                   HoodieInstant hoodieInstant) {
    boolean isTableServiceAction = table.isTableServiceAction(hoodieInstant.getAction());
    // Do not do any conflict resolution here as we do with regular writes. We take the lock here to ensure all writes to metadata table happens within a
    // single lock (single writer). Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
    table.getMetadataWriter(hoodieInstant.getTimestamp())
        .ifPresent(writer -> ((HoodieTableMetadataWriter) writer).update(commitMetadata, hoodieInstant.getTimestamp(), isTableServiceAction));
  }

  /**
   * Initialize the metadata table if needed. Creating the metadata table writer
   * will trigger the initial bootstrapping from the data table.
   *
   * @param inFlightInstantTimestamp - The in-flight action responsible for the metadata table initialization
   */
  protected void initializeMetadataTable(Option<String> inFlightInstantTimestamp) {
    if (config.isMetadataTableEnabled()) {
      SparkHoodieBackedTableMetadataWriter.create(context.getHadoopConf().get(), config,
          context, Option.empty(), inFlightInstantTimestamp);
    }
  }

  @Override
  protected HoodieTable<?, ?, ?, ?> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieSparkTable.create(config, context);
  }
}
