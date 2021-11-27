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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metrics.DistributedRegistry;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class SparkRDDWriteClient<T extends HoodieRecordPayload> extends
    AbstractHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkRDDWriteClient.class);

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, Option.empty());
  }

  @Deprecated
  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending) {
    this(context, writeConfig, Option.empty());
  }

  @Deprecated
  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending,
                             Option<EmbeddedTimelineService> timelineService) {
    this(context, writeConfig, timelineService);
  }

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig,
                             Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService);
  }

  /**
   * Register hudi classes for Kryo serialization.
   *
   * @param conf instance of SparkConf
   * @return SparkConf
   */
  public static SparkConf registerClasses(SparkConf conf) {
    conf.registerKryoClasses(new Class[]{HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
    return conf;
  }

  @Override
  protected HoodieIndex createIndex(HoodieWriteConfig writeConfig) {
    return SparkHoodieIndexFactory.createIndex(config);
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata,
                        String commitActionType, Map<String, List<String>> partitionToReplacedFileIds) {
    List<HoodieWriteStat> writeStats = writeStatuses.map(WriteStatus::getStat).collect();
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  protected HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> createTable(HoodieWriteConfig config,
                                                                                                           Configuration hadoopConf,
                                                                                                           boolean refreshTimeline) {
    return HoodieSparkTable.create(config, context, refreshTimeline);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    JavaRDD<HoodieRecord<T>> recordsWithLocation = HoodieJavaRDD.getJavaRDD(
        getIndex().tagLocation(HoodieJavaRDD.of(hoodieRecords), context, table));
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Main API to run bootstrap to hudi.
   */
  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    getTableAndInitCtx(WriteOperationType.UPSERT, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS).bootstrap(context, extraMetadata);
  }

  @Override
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.upsert(context, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.UPSERT_PREPPED, instantTime);
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.upsertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.insert(context,instantTime, records);
    return postWrite(result, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT_PREPPED, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.insertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  /**
   * Removes all existing records from the partitions affected and inserts the given HoodieRecords, into the table.

   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwrite(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable table = getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE, table.getMetaClient());
    HoodieWriteMetadata result = table.insertOverwrite(context, instantTime, records);
    return new HoodieWriteResult(postWrite(result, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  /**
   * Removes all existing records of the Hoodie table and inserts the given HoodieRecords, into the table.

   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwriteTable(JavaRDD<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable table = getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE_TABLE, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE, table.getMetaClient());
    HoodieWriteMetadata result = table.insertOverwriteTable(context, instantTime, records);
    return new HoodieWriteResult(postWrite(result, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return bulkInsert(records, instantTime, Option.empty());
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.BULK_INSERT, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.bulkInsert(context,instantTime, records, userDefinedBulkInsertPartitioner);
    return postWrite(result, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> bulkInsertPartitioner) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.BULK_INSERT_PREPPED, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.bulkInsertPrepped(context,instantTime, preppedRecords, bulkInsertPartitioner);
    return postWrite(result, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> delete(JavaRDD<HoodieKey> keys, String instantTime) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table = getTableAndInitCtx(WriteOperationType.DELETE, instantTime);
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.delete(context,instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  public HoodieWriteResult deletePartitions(List<String> partitions, String instantTime) {
    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table = getTableAndInitCtx(WriteOperationType.DELETE_PARTITION, instantTime);
    preWrite(instantTime, WriteOperationType.DELETE_PARTITION, table.getMetaClient());
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = table.deletePartitions(context, instantTime, partitions);
    return new HoodieWriteResult(postWrite(result, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  @Override
  protected JavaRDD<WriteStatus> postWrite(HoodieWriteMetadata<JavaRDD<WriteStatus>> result,
                                           String instantTime,
                                           HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    if (result.isCommitted()) {
      // Perform post commit operations.
      if (result.getFinalizeDuration().isPresent()) {
        metrics.updateFinalizeWriteMetrics(result.getFinalizeDuration().get().toMillis(),
            result.getWriteStats().get().size());
      }

      postCommit(hoodieTable, result.getCommitMetadata().get(), instantTime, Option.empty());

      emitCommitMetrics(instantTime, result.getCommitMetadata().get(), hoodieTable.getMetaClient().getCommitActionType());
    }
    return result.getWriteStatuses();
  }

  @Override
  public void commitCompaction(String compactionInstantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context);
    HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(
        table, compactionInstantTime, HoodieJavaRDD.of(writeStatuses), config.getSchema());
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, writeStatuses, table, compactionInstantTime);
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, JavaRDD<WriteStatus> writeStatuses,
                                    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                    String compactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = writeStatuses.map(WriteStatus::getStat).collect();
    try {
      HoodieInstant compactionInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime);
      this.txnManager.beginTransaction(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      writeTableMetadataForTableServices(table, metadata, compactionInstant);
      LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionCommitTime, metadata);
    } finally {
      this.txnManager.endTransaction();
    }
    WriteMarkersFactory.get(config.getMarkersType(), table, compactionCommitTime)
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
  protected JavaRDD<WriteStatus> compact(String compactionInstantTime, boolean shouldComplete) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context, true);
    preWrite(compactionInstantTime, WriteOperationType.COMPACT, table.getMetaClient());
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightCompaction(inflightInstant);
      table.getMetaClient().reloadActiveTimeline();
    }
    compactionTimer = metrics.getCompactionCtx();
    HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata =
        table.compact(context, compactionInstantTime);
    JavaRDD<WriteStatus> statuses = compactionMetadata.getWriteStatuses();
    if (shouldComplete && compactionMetadata.getCommitMetadata().isPresent()) {
      completeTableService(TableServiceType.COMPACT, compactionMetadata.getCommitMetadata().get(), statuses, table, compactionInstantTime);
    }
    return statuses;
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> cluster(String clusteringInstant, boolean shouldComplete) {
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, context, config.isMetadataTableEnabled());
    preWrite(clusteringInstant, WriteOperationType.CLUSTER, table.getMetaClient());
    HoodieTimeline pendingClusteringTimeline = table.getActiveTimeline().filterPendingReplaceTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getReplaceCommitInflightInstant(clusteringInstant);
    if (pendingClusteringTimeline.containsInstant(inflightInstant)) {
      rollbackInflightClustering(inflightInstant, table);
      table.getMetaClient().reloadActiveTimeline();
    }
    clusteringTimer = metrics.getClusteringCtx();
    LOG.info("Starting clustering at " + clusteringInstant);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> clusteringMetadata = table.cluster(context, clusteringInstant);
    JavaRDD<WriteStatus> statuses = clusteringMetadata.getWriteStatuses();
    // TODO : Where is shouldComplete used ?
    if (shouldComplete && clusteringMetadata.getCommitMetadata().isPresent()) {
      completeTableService(TableServiceType.CLUSTER, clusteringMetadata.getCommitMetadata().get(), statuses, table, clusteringInstant);
    }
    return clusteringMetadata;
  }

  private void completeClustering(HoodieReplaceCommitMetadata metadata, JavaRDD<WriteStatus> writeStatuses,
                                    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                    String clusteringCommitTime) {

    List<HoodieWriteStat> writeStats = metadata.getPartitionToWriteStats().entrySet().stream().flatMap(e ->
        e.getValue().stream()).collect(Collectors.toList());

    if (writeStats.stream().mapToLong(s -> s.getTotalWriteErrors()).sum() > 0) {
      throw new HoodieClusteringException("Clustering failed to write to files:"
          + writeStats.stream().filter(s -> s.getTotalWriteErrors() > 0L).map(s -> s.getFileId()).collect(Collectors.joining(",")));
    }
    try {
      HoodieInstant clusteringInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, clusteringCommitTime);
      this.txnManager.beginTransaction(Option.of(clusteringInstant), Option.empty());
      finalizeWrite(table, clusteringCommitTime, writeStats);
      writeTableMetadataForTableServices(table, metadata,clusteringInstant);
      // Update outstanding metadata indexes
      if (config.isLayoutOptimizationEnabled()
          && !config.getClusteringSortColumns().isEmpty()) {
        table.updateMetadataIndexes(context, writeStats, clusteringCommitTime);
      }
      LOG.info("Committing Clustering " + clusteringCommitTime + ". Finished with result " + metadata);
      table.getActiveTimeline().transitionReplaceInflightToComplete(
          HoodieTimeline.getReplaceCommitInflightInstant(clusteringCommitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new HoodieClusteringException("unable to transition clustering inflight to complete: " + clusteringCommitTime, e);
    } finally {
      this.txnManager.endTransaction();
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

  private void writeTableMetadataForTableServices(HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table, HoodieCommitMetadata commitMetadata,
                                  HoodieInstant hoodieInstant) {
    boolean isTableServiceAction = table.isTableServiceAction(hoodieInstant.getAction());
    // Do not do any conflict resolution here as we do with regular writes. We take the lock here to ensure all writes to metadata table happens within a
    // single lock (single writer). Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
    table.getMetadataWriter(hoodieInstant.getTimestamp()).ifPresent(
        w -> w.update(commitMetadata, hoodieInstant.getTimestamp(), isTableServiceAction));
  }

  @Override
  protected HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    UpgradeDowngrade upgradeDowngrade = new UpgradeDowngrade(
        metaClient, config, context, SparkUpgradeDowngradeHelper.getInstance());
    try {
      this.txnManager.beginTransaction();
      if (upgradeDowngrade.needsUpgradeOrDowngrade(HoodieTableVersion.current())) {
        // Ensure no inflight commits by setting EAGER policy and explicitly cleaning all failed commits
        List<String> instantsToRollback = getInstantsToRollback(
            metaClient, HoodieFailedWritesCleaningPolicy.EAGER, Option.of(instantTime));
        Map<String, Option<HoodiePendingRollbackInfo>> pendingRollbacks = getPendingRollbackInfos(metaClient);
        instantsToRollback.forEach(entry -> pendingRollbacks.putIfAbsent(entry, Option.empty()));
        this.rollbackFailedWrites(pendingRollbacks, true);
        new UpgradeDowngrade(
            metaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
            .run(HoodieTableVersion.current(), instantTime);
        metaClient.reloadActiveTimeline();
        initializeMetadataTable(Option.of(instantTime));
      }
    } finally {
      this.txnManager.endTransaction();
    }
    metaClient.validateTableProperties(config.getProps(), operationType);
    return getTableAndInitCtx(metaClient, operationType, instantTime);
  }

  /**
   * Initialize the metadata table if needed. Creating the metadata table writer
   * will trigger the initial bootstrapping from the data table.
   *
   * @param inFlightInstantTimestamp - The in-flight action responsible for the metadata table initialization
   */
  private void initializeMetadataTable(Option<String> inFlightInstantTimestamp) {
    if (config.isMetadataTableEnabled()) {
      SparkHoodieBackedTableMetadataWriter.create(context.getHadoopConf().get(), config,
          context, Option.empty(), inFlightInstantTimestamp);
    }
  }

  // TODO : To enforce priority between table service and ingestion writer, use transactions here and invoke strategy
  private void completeTableService(TableServiceType tableServiceType, HoodieCommitMetadata metadata, JavaRDD<WriteStatus> writeStatuses,
                                    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                    String commitInstant) {

    switch (tableServiceType) {
      case CLUSTER:
        completeClustering((HoodieReplaceCommitMetadata) metadata, writeStatuses, table, commitInstant);
        break;
      case COMPACT:
        completeCompaction(metadata, writeStatuses, table, commitInstant);
        break;
      default:
        throw new IllegalArgumentException("This table service is not valid " + tableServiceType);
    }
  }

  private HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> getTableAndInitCtx(
      HoodieTableMetaClient metaClient, WriteOperationType operationType, String instantTime) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<T> table = HoodieSparkTable.create(config, (HoodieSparkEngineContext) context, metaClient, config.isMetadataTableEnabled());
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else {
      writeTimer = metrics.getDeltaCommitCtx();
    }
    table.getHoodieView().sync();
    return table;
  }

  @Override
  protected void preCommit(HoodieInstant inflightInstant, HoodieCommitMetadata metadata) {
    // Create a Hoodie table after startTxn which encapsulated the commits and files visible.
    // Important to create this after the lock to ensure latest commits show up in the timeline without need for reload
    HoodieTable table = createTable(config, hadoopConf);
    TransactionUtils.resolveWriteConflictIfAny(table, this.txnManager.getCurrentTransactionOwner(),
        Option.of(metadata), config, txnManager.getLastCompletedTransactionOwner());
  }

  @Override
  protected void initWrapperFSMetrics() {
    if (config.isMetricsOn()) {
      Registry registry;
      Registry registryMeta;
      JavaSparkContext jsc = ((HoodieSparkEngineContext) context).getJavaSparkContext();

      if (config.isExecutorMetricsEnabled()) {
        // Create a distributed registry for HoodieWrapperFileSystem
        registry = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName(),
            DistributedRegistry.class.getName());
        ((DistributedRegistry)registry).register(jsc);
        registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder",
            DistributedRegistry.class.getName());
        ((DistributedRegistry)registryMeta).register(jsc);
      } else {
        registry = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName());
        registryMeta = Registry.getRegistry(HoodieWrapperFileSystem.class.getSimpleName() + "MetaFolder");
      }

      HoodieWrapperFileSystem.setMetricsRegistry(registry, registryMeta);
    }
  }

  @Override
  protected void releaseResources() {
    ((HoodieSparkEngineContext) context).getJavaSparkContext().getPersistentRDDs().values()
        .forEach(rdd -> rdd.unpersist());
  }
}
