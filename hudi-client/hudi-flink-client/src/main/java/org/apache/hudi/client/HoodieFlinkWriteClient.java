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

import org.apache.hudi.async.AsyncCleanerService;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
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
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.FlinkHoodieIndexFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.FlinkAppendHandle;
import org.apache.hudi.io.FlinkConcatAndReplaceHandle;
import org.apache.hudi.io.FlinkConcatHandle;
import org.apache.hudi.io.FlinkCreateHandle;
import org.apache.hudi.io.FlinkMergeAndReplaceHandle;
import org.apache.hudi.io.FlinkMergeHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.FlinkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.util.FlinkClientUtil;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkWriteClient<T extends HoodieRecordPayload> extends
    BaseHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkWriteClient.class);

  /**
   * FileID to write handle mapping in order to record the write handles for each file group,
   * so that we can append the mini-batch data buffer incrementally.
   */
  private final Map<String, HoodieWriteHandle<?, ?, ?, ?>> bucketToHandles;

  /**
   * Cached metadata writer for coordinator to reuse for each commit.
   */
  private Option<HoodieBackedTableMetadataWriter> metadataWriterOption = Option.empty();

  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
    super(context, writeConfig, FlinkUpgradeDowngradeHelper.getInstance());
    this.bucketToHandles = new HashMap<>();
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  protected HoodieIndex createIndex(HoodieWriteConfig writeConfig) {
    return FlinkHoodieIndexFactory.createIndex((HoodieFlinkEngineContext) context, config);
  }

  @Override
  public boolean commit(String instantTime, List<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds) {
    List<HoodieWriteStat> writeStats = writeStatuses.parallelStream().map(WriteStatus::getStat).collect(Collectors.toList());
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  @Override
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = getHoodieTable();
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = HoodieList.getList(
        getIndex().tagLocation(HoodieList.of(hoodieRecords), context, table));
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());
  }

  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Bootstrap operation is not supported yet");
  }

  @Override
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata<List<WriteStatus>> result = ((HoodieFlinkTable<T>) table).upsert(context, writeHandle, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> upsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    // only used for metadata table, the upsert happens in single thread
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(preppedRecords.get(0), getConfig(),
        instantTime, table, preppedRecords.listIterator());
    HoodieWriteMetadata<List<WriteStatus>> result = ((HoodieFlinkTable<T>) table).upsertPrepped(context, writeHandle, instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.INSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    // create the write handle if not exists
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata<List<WriteStatus>> result = ((HoodieFlinkTable<T>) table).insert(context, writeHandle, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  /**
   * Removes all existing records from the partitions affected and inserts the given HoodieRecords, into the table.
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return list of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> insertOverwrite(
      List<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.INSERT_OVERWRITE, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE, table.getMetaClient());
    // create the write handle if not exists
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata result = ((HoodieFlinkTable<T>) table).insertOverwrite(context, writeHandle, instantTime, records);
    return postWrite(result, instantTime, table);
  }

  /**
   * Removes all existing records of the Hoodie table and inserts the given HoodieRecords, into the table.
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return list of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> insertOverwriteTable(
      List<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable table = initTable(WriteOperationType.INSERT_OVERWRITE_TABLE, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE, table.getMetaClient());
    // create the write handle if not exists
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata result = ((HoodieFlinkTable<T>) table).insertOverwriteTable(context, writeHandle, instantTime, records);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    throw new HoodieNotSupportedException("InsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> delete(List<HoodieKey> keys, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.DELETE, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.delete(context, instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  @Override
  protected void preWrite(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) {
    setOperationType(writeOperationType);
    // Note: the code to read the commit metadata is not thread safe for JSON deserialization,
    // remove the table metadata sync

    // remove the async cleaning
  }

  @Override
  protected void writeTableMetadata(HoodieTable table, String instantTime, String actionType, HoodieCommitMetadata metadata) {
    this.metadataWriterOption.ifPresent(w -> {
      w.initTableMetadata(); // refresh the timeline
      w.update(metadata, instantTime, getHoodieTable().isTableServiceAction(actionType));
    });
  }

  /**
   * Initialize the table metadata writer, for e.g, bootstrap the metadata table
   * from the filesystem if it does not exist.
   */
  public void initMetadataWriter() {
    HoodieBackedTableMetadataWriter metadataWriter = (HoodieBackedTableMetadataWriter) FlinkHoodieBackedTableMetadataWriter.create(
        FlinkClientUtil.getHadoopConf(), this.config, HoodieFlinkEngineContext.DEFAULT);
    this.metadataWriterOption = Option.of(metadataWriter);
  }

  /**
   * Starts async cleaning service for finished commits.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   */
  public void startAsyncCleaning() {
    if (this.asyncCleanerService == null) {
      this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this);
    } else {
      this.asyncCleanerService.start(null);
    }
  }

  /**
   * Blocks and wait for the async cleaning service to finish.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   */
  public void waitForCleaningFinish() {
    if (this.asyncCleanerService != null) {
      LOG.info("Cleaner has been spawned already. Waiting for it to finish");
      AsyncCleanerService.waitForCompletion(asyncCleanerService);
      LOG.info("Cleaner has finished");
    }
  }

  @Override
  protected List<WriteStatus> postWrite(HoodieWriteMetadata<List<WriteStatus>> result,
                                        String instantTime,
                                        HoodieTable hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    return result.getWriteStatuses();
  }

  /**
   * Post commit is rewrite to be invoked after a successful commit.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   *
   * @param table         Table to commit on
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   */
  @Override
  protected void postCommit(HoodieTable table,
                            HoodieCommitMetadata metadata,
                            String instantTime,
                            Option<Map<String, String>> extraMetadata,
                            boolean acquireLockForArchival) {
    try {
      // Delete the marker directory for the instant.
      WriteMarkersFactory.get(config.getMarkersType(), createTable(config, hadoopConf), instantTime)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
      autoArchiveOnCommit(table, acquireLockForArchival);
    } finally {
      this.heartbeatClient.stop(instantTime);
    }
  }

  @Override
  public void commitCompaction(
      String compactionInstantTime,
      HoodieCommitMetadata metadata,
      Option<Map<String, String>> extraMetadata) {
    HoodieFlinkTable<T> table = getHoodieTable();
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, table, compactionInstantTime);
  }

  @Override
  public void completeCompaction(
      HoodieCommitMetadata metadata,
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
  protected HoodieWriteMetadata<List<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    // only used for metadata table, the compaction happens in single thread
    HoodieWriteMetadata<List<WriteStatus>> compactionMetadata = getHoodieTable().compact(context, compactionInstantTime);
    commitCompaction(compactionInstantTime, compactionMetadata.getCommitMetadata().get(), Option.empty());
    return compactionMetadata;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final String clusteringInstant, final boolean shouldComplete) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  private void completeClustering(
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
  protected HoodieTable doInitTable(HoodieTableMetaClient metaClient, Option<String> instantTime, boolean initialMetadataTableIfNecessary) {
    // Create a Hoodie table which encapsulated the commits and files visible
    return getHoodieTable();
  }

  @Override
  protected void tryUpgrade(HoodieTableMetaClient metaClient, Option<String> instantTime) {
    // do nothing.
    // flink executes the upgrade/downgrade once when initializing the first instant on start up,
    // no need to execute the upgrade/downgrade on each write in streaming.
  }

  public void completeTableService(
      TableServiceType tableServiceType,
      HoodieCommitMetadata metadata,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
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

  /**
   * Upgrade downgrade the Hoodie table.
   *
   * <p>This action should only be executed once for each commit.
   * The modification of the table properties is not thread safe.
   */
  public void upgradeDowngrade(String instantTime, HoodieTableMetaClient metaClient) {
    new UpgradeDowngrade(metaClient, config, context, FlinkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.current(), instantTime);
  }

  /**
   * Clean the write handles within a checkpoint interval.
   * All the handles should have been closed already.
   */
  public void cleanHandles() {
    this.bucketToHandles.clear();
  }

  /**
   * Clean the write handles within a checkpoint interval, this operation
   * would close the underneath file handles, if any error happens, clean the
   * corrupted data file.
   */
  public void cleanHandlesGracefully() {
    this.bucketToHandles.values()
        .forEach(handle -> ((MiniBatchHandle) handle).closeGracefully());
    this.bucketToHandles.clear();
  }

  /**
   * Get or create a new write handle in order to reuse the file handles.
   *
   * @param record      The first record in the bucket
   * @param config      Write config
   * @param instantTime The instant time
   * @param table       The table
   * @param recordItr   Record iterator
   * @return Existing write handle or create a new one
   */
  private HoodieWriteHandle<?, ?, ?, ?> getOrCreateWriteHandle(
      HoodieRecord<T> record,
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      Iterator<HoodieRecord<T>> recordItr) {
    final HoodieRecordLocation loc = record.getCurrentLocation();
    final String fileID = loc.getFileId();
    final String partitionPath = record.getPartitionPath();
    final boolean insertClustering = config.allowDuplicateInserts();

    if (bucketToHandles.containsKey(fileID)) {
      MiniBatchHandle lastHandle = (MiniBatchHandle) bucketToHandles.get(fileID);
      if (lastHandle.shouldReplace()) {
        HoodieWriteHandle<?, ?, ?, ?> writeHandle = insertClustering
            ? new FlinkConcatAndReplaceHandle<>(config, instantTime, table, recordItr, partitionPath, fileID,
                table.getTaskContextSupplier(), lastHandle.getWritePath())
            : new FlinkMergeAndReplaceHandle<>(config, instantTime, table, recordItr, partitionPath, fileID,
                table.getTaskContextSupplier(), lastHandle.getWritePath());
        this.bucketToHandles.put(fileID, writeHandle); // override with new replace handle
        return writeHandle;
      }
    }

    final boolean isDelta = table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ);
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle;
    if (isDelta) {
      writeHandle = new FlinkAppendHandle<>(config, instantTime, table, partitionPath, fileID, recordItr,
          table.getTaskContextSupplier());
    } else if (loc.getInstantTime().equals("I")) {
      writeHandle = new FlinkCreateHandle<>(config, instantTime, table, partitionPath,
          fileID, table.getTaskContextSupplier());
    } else {
      writeHandle = insertClustering
          ? new FlinkConcatHandle<>(config, instantTime, table, recordItr, partitionPath,
              fileID, table.getTaskContextSupplier())
          : new FlinkMergeHandle<>(config, instantTime, table, recordItr, partitionPath,
              fileID, table.getTaskContextSupplier());
    }
    this.bucketToHandles.put(fileID, writeHandle);
    return writeHandle;
  }

  public HoodieFlinkTable<T> getHoodieTable() {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  public Map<String, List<String>> getPartitionToReplacedFileIds(
      WriteOperationType writeOperationType,
      List<WriteStatus> writeStatuses) {
    HoodieFlinkTable<T> table = getHoodieTable();
    switch (writeOperationType) {
      case INSERT_OVERWRITE:
        return writeStatuses.stream().map(status -> status.getStat().getPartitionPath()).distinct()
            .collect(
                Collectors.toMap(
                    partition -> partition,
                    partitionPath -> getAllExistingFileIds(table, partitionPath)));
      case INSERT_OVERWRITE_TABLE:
        Map<String, List<String>> partitionToExistingFileIds = new HashMap<>();
        List<String> partitionPaths =
            FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), table.getMetaClient().getBasePath());
        if (partitionPaths != null && partitionPaths.size() > 0) {
          context.setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of all partitions: " + config.getTableName());
          partitionToExistingFileIds = partitionPaths.stream().parallel()
              .collect(
                  Collectors.toMap(
                      partition -> partition,
                      partition -> getAllExistingFileIds(table, partition)));
        }
        return partitionToExistingFileIds;
      default:
        throw new AssertionError();
    }
  }

  private List<String> getAllExistingFileIds(HoodieFlinkTable<T> table, String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }
}
