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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.FlinkHoodieIndexFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.FlinkWriteHandleFactory;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.table.upgrade.FlinkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.util.WriteStatMerger;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Flink hoodie write client.
 *
 * <p>The client is used both on driver (for starting/committing transactions)
 * and executor (for writing dataset).
 *
 * @param <T> type of the payload
 */
@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkWriteClient<T> extends
    BaseHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkWriteClient.class);

  /**
   * FileID to write handle mapping in order to record the write handles for each file group,
   * so that we can append the mini-batch data buffer incrementally.
   */
  private final Map<String, Path> bucketToHandles;

  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
    super(context, writeConfig, FlinkUpgradeDowngradeHelper.getInstance());
    this.bucketToHandles = new HashMap<>();
    this.tableServiceClient = new HoodieFlinkTableServiceClient<>(context, writeConfig, getTimelineServer());
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  protected HoodieIndex createIndex(HoodieWriteConfig writeConfig) {
    return FlinkHoodieIndexFactory.createIndex((HoodieFlinkEngineContext) context, config);
  }

  @Override
  public boolean commit(String instantTime, List<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata,
                        String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                        Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
    List<HoodieWriteStat> writeStats = writeStatuses.parallelStream().map(WriteStatus::getStat).collect(Collectors.toList());
    // for eager flush, multiple write stat may share one file path.
    List<HoodieWriteStat> merged = writeStats.stream()
        .collect(Collectors.groupingBy(writeStat -> writeStat.getPartitionPath() + writeStat.getPath()))
        .values().stream()
        .map(duplicates -> duplicates.stream().reduce(WriteStatMerger::merge).get())
        .collect(Collectors.toList());
    return commitStats(instantTime, merged, extraMetadata, commitActionType, partitionToReplacedFileIds, extraPreCommitFunc);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context, metaClient);
  }

  @Override
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = getHoodieTable();
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(HoodieListData.eager(hoodieRecords), context, table).collectAsList();
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
    HoodieWriteMetadata<List<WriteStatus>> result;
    try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table)) {
      result = ((HoodieFlinkTable<T>) table).upsert(context, closeableHandle.getWriteHandle(), instantTime, records);
    }
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
    Map<String, List<HoodieRecord<T>>> preppedRecordsByFileId = preppedRecords.stream().parallel()
        .collect(Collectors.groupingBy(r -> r.getCurrentLocation().getFileId()));
    return preppedRecordsByFileId.values().stream().parallel().map(records -> {
      HoodieWriteMetadata<List<WriteStatus>> result;
      try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table)) {
        result = ((HoodieFlinkTable<T>) table).upsertPrepped(context, closeableHandle.getWriteHandle(), instantTime, records);
      }
      return postWrite(result, instantTime, table);
    }).flatMap(Collection::stream).collect(Collectors.toList());
  }

  @Override
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.INSERT, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    // create the write handle if not exists
    HoodieWriteMetadata<List<WriteStatus>> result;
    try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table)) {
      result = ((HoodieFlinkTable<T>) table).insert(context, closeableHandle.getWriteHandle(), instantTime, records);
    }
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
    HoodieWriteMetadata<List<WriteStatus>> result;
    try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table)) {
      result = ((HoodieFlinkTable<T>) table).insertOverwrite(context, closeableHandle.getWriteHandle(), instantTime, records);
    }
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
    HoodieWriteMetadata<List<WriteStatus>> result;
    try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table)) {
      result = ((HoodieFlinkTable<T>) table).insertOverwriteTable(context, closeableHandle.getWriteHandle(), instantTime, records);
    }
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

  public List<WriteStatus> deletePartitions(List<String> partitions, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.DELETE_PARTITION, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PARTITION, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.deletePartitions(context, instantTime, partitions);
    return postWrite(result, instantTime, table);
  }

  @Override
  public void preWrite(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) {
    setOperationType(writeOperationType);
    // Note: the code to read the commit metadata is not thread safe for JSON deserialization,
    // remove the table metadata sync

    // remove the async cleaning
  }

  @Override
  protected void writeTableMetadata(HoodieTable table, String instantTime, String actionType, HoodieCommitMetadata metadata) {
    tableServiceClient.writeTableMetadata(table, instantTime, actionType, metadata);
  }

  /**
   * Initialized the metadata table on start up, should only be called once on driver.
   */
  public void initMetadataTable() {
    ((HoodieFlinkTableServiceClient<T>) tableServiceClient).initMetadataTable();
  }

  /**
   * Starts async cleaning service for finished commits.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   */
  public void startAsyncCleaning() {
    tableServiceClient.startAsyncCleanerService(this);
  }

  /**
   * Blocks and wait for the async cleaning service to finish.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   */
  public void waitForCleaningFinish() {
    if (tableServiceClient.asyncCleanerService != null) {
      LOG.info("Cleaner has been spawned already. Waiting for it to finish");
      tableServiceClient.asyncClean();
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
                            Option<Map<String, String>> extraMetadata) {
    try {
      // Delete the marker directory for the instant.
      WriteMarkersFactory.get(config.getMarkersType(), createTable(config, hadoopConf), instantTime)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
    } finally {
      this.heartbeatClient.stop(instantTime);
    }
  }

  @Override
  protected void mayBeCleanAndArchive(HoodieTable table) {
    autoArchiveOnCommit(table);
  }

  @Override
  public void commitCompaction(
      String compactionInstantTime,
      HoodieCommitMetadata metadata,
      Option<Map<String, String>> extraMetadata) {
    tableServiceClient.commitCompaction(compactionInstantTime, metadata, extraMetadata);
  }

  @Override
  public void completeCompaction(
      HoodieCommitMetadata metadata,
      HoodieTable table,
      String compactionCommitTime) {
    tableServiceClient.completeCompaction(metadata, table, compactionCommitTime);
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    // only used for metadata table, the compaction happens in single thread
    return tableServiceClient.compact(compactionInstantTime, shouldComplete);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final String clusteringInstant, final boolean shouldComplete) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  private void completeClustering(
      HoodieReplaceCommitMetadata metadata,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String clusteringCommitTime) {
    ((HoodieFlinkTableServiceClient<T>) tableServiceClient).completeClustering(metadata, table, clusteringCommitTime);
  }

  @Override
  protected void doInitTable(HoodieTableMetaClient metaClient, Option<String> instantTime, boolean initialMetadataTableIfNecessary) {
    // do nothing.

    // flink executes the upgrade/downgrade once when initializing the first instant on start up,
    // no need to execute the upgrade/downgrade on each write in streaming.

    // flink performs metadata table bootstrap on the coordinator when it starts up.
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

  @Override
  public void close() {
    super.close();
    cleanHandles();
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
    // caution: it's not a good practice to modify the handles internal.
    FlinkWriteHandleFactory.Factory<T,
        List<HoodieRecord<T>>,
        List<HoodieKey>,
        List<WriteStatus>> writeHandleFactory = FlinkWriteHandleFactory.getFactory(table.getMetaClient().getTableConfig(), config);
    return writeHandleFactory.create(this.bucketToHandles, record, config, instantTime, table, recordItr);
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

  private final class AutoCloseableWriteHandle implements AutoCloseable {
    private final HoodieWriteHandle<?, ?, ?, ?> writeHandle;

    AutoCloseableWriteHandle(
        List<HoodieRecord<T>> records,
        String instantTime,
        HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table
    ) {
      this.writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(), instantTime, table, records.listIterator());
    }

    HoodieWriteHandle<?, ?, ?, ?> getWriteHandle() {
      return writeHandle;
    }

    @Override
    public void close() {
      ((MiniBatchHandle) writeHandle).closeGracefully();
    }
  }
}
