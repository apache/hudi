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

import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.JavaHoodieIndexFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.JavaUpgradeDowngradeHelper;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieJavaWriteClient<T extends HoodieRecordPayload> extends
    BaseHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieJavaWriteClient.class);

  public HoodieJavaWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig, JavaUpgradeDowngradeHelper.getInstance());
  }

  public HoodieJavaWriteClient(HoodieEngineContext context,
                               HoodieWriteConfig writeConfig,
                               boolean rollbackPending,
                               Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService, JavaUpgradeDowngradeHelper.getInstance());
  }

  @Override
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieJavaTable<T> table = HoodieJavaTable.create(config, (HoodieJavaEngineContext) context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(HoodieListData.eager(hoodieRecords), context, table).collectAsList();
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());
  }

  @Override
  protected HoodieIndex createIndex(HoodieWriteConfig writeConfig) {
    return JavaHoodieIndexFactory.createIndex(config);
  }

  @Override
  public boolean commit(String instantTime,
                        List<WriteStatus> writeStatuses,
                        Option<Map<String, String>> extraMetadata,
                        String commitActionType,
                        Map<String, List<String>> partitionToReplacedFileIds) {
    List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieJavaTable.create(config, context);
  }

  @Override
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records,
                                  String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.upsert(context, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> upsertPreppedRecords(List<HoodieRecord<T>> preppedRecords,
                                                String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.upsertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.INSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.insert(context, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insertPreppedRecords(List<HoodieRecord<T>> preppedRecords,
                                                String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.insertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records,
                                      String instantTime) {
    throw new HoodieNotSupportedException("BulkInsert is not supported in HoodieJavaClient");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records,
                                      String instantTime,
                                      Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert is not supported in HoodieJavaClient");
  }

  public void transitionInflight(String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        new HoodieInstant(HoodieInstant.State.REQUESTED, metaClient.getCommitActionType(), instantTime),
        Option.empty(), config.shouldAllowMultiWriteOnSameInstant());
  }

  @Override
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords,
                                                    String instantTime,
                                                    Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.bulkInsertPrepped(context, instantTime, preppedRecords, bulkInsertPartitioner);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> delete(List<HoodieKey> keys,
                                  String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.DELETE, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.delete(context,instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  @Override
  protected List<WriteStatus> postWrite(HoodieWriteMetadata<List<WriteStatus>> result,
                                        String instantTime,
                                        HoodieTable hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    if (result.isCommitted()) {
      // Perform post commit operations.
      if (result.getFinalizeDuration().isPresent()) {
        metrics.updateFinalizeWriteMetrics(result.getFinalizeDuration().get().toMillis(),
            result.getWriteStats().get().size());
      }

      postCommit(hoodieTable, result.getCommitMetadata().get(), instantTime, Option.empty(), true);

      emitCommitMetrics(instantTime, result.getCommitMetadata().get(), hoodieTable.getMetaClient().getCommitActionType());
    }
    return result.getWriteStatuses();
  }

  @Override
  public void commitCompaction(String compactionInstantTime,
                               HoodieCommitMetadata metadata,
                               Option<Map<String, String>> extraMetadata) {
    HoodieJavaTable<T> table = HoodieJavaTable.create(config, context);
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, table, compactionInstantTime);
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata,
                                    HoodieTable table,
                                    String compactionCommitTime) {
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

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> compact(String compactionInstantTime,
                                      boolean shouldComplete) {
    HoodieJavaTable<T> table = HoodieJavaTable.create(config, context);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      table.rollbackInflightCompaction(inflightInstant, commitToRollback -> getPendingRollbackInfo(table.getMetaClient(), commitToRollback, false));
      table.getMetaClient().reloadActiveTimeline();
    }
    compactionTimer = metrics.getCompactionCtx();
    HoodieWriteMetadata<List<WriteStatus>> writeMetadata = table.compact(context, compactionInstantTime);
    completeCompaction(writeMetadata.getCommitMetadata().get(), table, compactionInstantTime);
    return writeMetadata;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final String clusteringInstant, final boolean shouldComplete) {
    throw new HoodieNotSupportedException("Cluster is not supported in HoodieJavaClient");
  }

  @Override
  protected HoodieTable doInitTable(HoodieTableMetaClient metaClient, Option<String> instantTime, boolean initialMetadataTableIfNecessary) {
    // new JavaUpgradeDowngrade(metaClient, config, context).run(metaClient, HoodieTableVersion.current(), config, context, instantTime);

    // Create a Hoodie table which encapsulated the commits and files visible
    return HoodieJavaTable.create(config, (HoodieJavaEngineContext) context, metaClient);
  }

}
