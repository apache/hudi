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
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.data.HoodieData;
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
import org.apache.hudi.table.upgrade.FlinkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.util.WriteStatMerger;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
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
    return commitStats(instantTime, HoodieListData.eager(writeStatuses), merged, extraMetadata, commitActionType, partitionToReplacedFileIds, extraPreCommitFunc);
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
  protected void validateTimestamp(HoodieTableMetaClient metaClient, String instantTime) {
    // no op
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
    try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table, true)) {
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
    try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table, true)) {
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
    // only used for metadata table, the bulk_insert happens in single JVM process
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT_PREPPED, table.getMetaClient());
    Map<String, List<HoodieRecord<T>>> preppedRecordsByFileId = preppedRecords.stream().parallel()
        .collect(Collectors.groupingBy(r -> r.getCurrentLocation().getFileId()));
    return preppedRecordsByFileId.values().stream().parallel().map(records -> {
      records.sort(Comparator.comparing(HoodieRecord::getRecordKey));
      HoodieWriteMetadata<List<WriteStatus>> result;
      records.get(0).getCurrentLocation().setInstantTime("I");
      try (AutoCloseableWriteHandle closeableHandle = new AutoCloseableWriteHandle(records, instantTime, table, true)) {
        result = ((HoodieFlinkTable<T>) table).bulkInsertPrepped(context, closeableHandle.getWriteHandle(), instantTime, records);
      }
      return postWrite(result, instantTime, table);
    }).flatMap(Collection::stream).collect(Collectors.toList());
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
  public List<WriteStatus> deletePrepped(List<HoodieRecord<T>> preppedRecords, final String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.DELETE_PREPPED, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.deletePrepped(context, instantTime, preppedRecords);
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

  /**
   * Refresh the last transaction metadata,
   * should be called before the Driver starts a new transaction.
   */
  public void preTxn(HoodieTableMetaClient metaClient) {
    if (txnManager.isLockRequired()) {
      // refresh the meta client which is reused
      metaClient.reloadActiveTimeline();
      this.lastCompletedTxnAndMetadata = TransactionUtils.getLastCompletedTxnInstantAndMetadata(metaClient);
      this.pendingInflightAndRequestedInstants = TransactionUtils.getInflightAndRequestedInstants(metaClient);
    }
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
  public List<WriteStatus> postWrite(HoodieWriteMetadata<List<WriteStatus>> result,
                                     String instantTime,
                                     HoodieTable hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    return result.getWriteStatuses();
  }

  @Override
  protected void mayBeCleanAndArchive(HoodieTable table) {
    autoArchiveOnCommit(table);
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
      String clusteringCommitTime,
      Option<HoodieData<WriteStatus>> writeStatuses) {
    ((HoodieFlinkTableServiceClient<T>) tableServiceClient).completeClustering(metadata, table, clusteringCommitTime, writeStatuses);
  }

  @Override
  protected void doInitTable(WriteOperationType operationType, HoodieTableMetaClient metaClient, Option<String> instantTime) {
    // do nothing.

    // flink executes the upgrade/downgrade once when initializing the first instant on start up,
    // no need to execute the upgrade/downgrade on each write in streaming.

    // flink performs metadata table bootstrap on the coordinator when it starts up.
  }

  public void completeTableService(
      TableServiceType tableServiceType,
      HoodieCommitMetadata metadata,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String commitInstant,
      Option<HoodieData<WriteStatus>> writeStatuses) {
    switch (tableServiceType) {
      case CLUSTER:
        completeClustering((HoodieReplaceCommitMetadata) metadata, table, commitInstant, writeStatuses);
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
   * @param overwrite   Whether this is an overwrite operation
   * @return Existing write handle or create a new one
   */
  private HoodieWriteHandle<?, ?, ?, ?> getOrCreateWriteHandle(
      HoodieRecord<T> record,
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      Iterator<HoodieRecord<T>> recordItr,
      boolean overwrite) {
    // caution: it's not a good practice to modify the handles internal.
    FlinkWriteHandleFactory.Factory<T,
        List<HoodieRecord<T>>,
        List<HoodieKey>,
        List<WriteStatus>> writeHandleFactory = FlinkWriteHandleFactory.getFactory(table.getMetaClient().getTableConfig(), config, overwrite);
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
            FSUtils.getAllPartitionPaths(context, table.getStorage(), config.getMetadataConfig(), table.getMetaClient().getBasePath());
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
        HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table) {
      this(records, instantTime, table, false);
    }

    AutoCloseableWriteHandle(
        List<HoodieRecord<T>> records,
        String instantTime,
        HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
        boolean overwrite) {
      this.writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(), instantTime, table, records.listIterator(), overwrite);
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
