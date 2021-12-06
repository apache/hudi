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
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
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
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkWriteClient<T extends HoodieRecordPayload> extends
    AbstractHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

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
    super(context, writeConfig);
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
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> createTable(HoodieWriteConfig config, Configuration hadoopConf,
                                                                                                  boolean refreshTimeline) {
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
        getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
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
        getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
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
        getTableAndInitCtx(WriteOperationType.INSERT, instantTime);
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
        getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE, instantTime);
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
    HoodieTable table = getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE_TABLE, instantTime);
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
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> delete(List<HoodieKey> keys, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.DELETE, instantTime);
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
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this);
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
                                        HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
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
  protected void postCommit(HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                            HoodieCommitMetadata metadata,
                            String instantTime,
                            Option<Map<String, String>> extraMetadata) {
    try {
      // Delete the marker directory for the instant.
      WriteMarkersFactory.get(config.getMarkersType(), createTable(config, hadoopConf), instantTime)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
      if (config.isAutoArchive()) {
        // We cannot have unbounded commit files. Archive commits if we have to archive
        archive(table);
      }
    } finally {
      this.heartbeatClient.stop(instantTime);
    }
  }

  @Override
  public void commitCompaction(
      String compactionInstantTime,
      List<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieFlinkTable<T> table = getHoodieTable();
    HoodieCommitMetadata metadata = CompactHelpers.getInstance().createCompactionMetadata(
        table, compactionInstantTime, HoodieList.of(writeStatuses), config.getSchema());
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, writeStatuses, table, compactionInstantTime);
  }

  @Override
  public void completeCompaction(
      HoodieCommitMetadata metadata,
      List<WriteStatus> writeStatuses,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String compactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    try {
      HoodieInstant compactionInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime);
      this.txnManager.beginTransaction(Option.of(compactionInstant), Option.empty());
      finalizeWrite(table, compactionCommitTime, writeStats);
      // commit to data table after committing to metadata table.
      // Do not do any conflict resolution here as we do with regular writes. We take the lock here to ensure all writes to metadata table happens within a
      // single lock (single writer). Because more than one write to metadata table will result in conflicts since all of them updates the same partition.
      table.getMetadataWriter(compactionInstant.getTimestamp()).ifPresent(
          w -> w.update(metadata, compactionInstant.getTimestamp(), table.isTableServiceAction(compactionInstant.getAction())));
      LOG.info("Committing Compaction {} finished with result {}.", compactionCommitTime, metadata);
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionCommitTime, metadata);
    } finally {
      this.txnManager.endTransaction();
    }
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
  protected List<WriteStatus> compact(String compactionInstantTime, boolean shouldComplete) {
    // only used for metadata table, the compaction happens in single thread
    try {
      List<WriteStatus> writeStatuses =
          getHoodieTable().compact(context, compactionInstantTime).getWriteStatuses();
      commitCompaction(compactionInstantTime, writeStatuses, Option.empty());
      return writeStatuses;
    } catch (IOException e) {
      throw new HoodieException("Error while compacting instant: " + compactionInstantTime);
    }
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final String clusteringInstant, final boolean shouldComplete) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  @Override
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    new UpgradeDowngrade(metaClient, config, context, FlinkUpgradeDowngradeHelper.getInstance())
        .run(HoodieTableVersion.current(), instantTime);
    return getTableAndInitCtx(metaClient, operationType);
  }

  /**
   * Upgrade downgrade the Hoodie table.
   *
   * <p>This action should only be executed once for each commit.
   * The modification of the table properties is not thread safe.
   */
  public void upgradeDowngrade(String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
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

  private HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(HoodieTableMetaClient metaClient, WriteOperationType operationType) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = getHoodieTable();
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else {
      writeTimer = metrics.getDeltaCommitCtx();
    }
    return table;
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
          context.setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of all partitions");
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
