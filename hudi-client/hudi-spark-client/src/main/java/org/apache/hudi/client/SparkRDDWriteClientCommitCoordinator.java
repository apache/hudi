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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.callback.common.WriteStatusHandlerCallback;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRestoreException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.action.TableChange;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class SparkRDDWriteClientCommitCoordinator<T> extends SparkRDDWriteClient<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkRDDWriteClientCommitCoordinator.class);

  private final SparkRDDWriteClient writeClient;

  // Cached HoodieTableMetadataWriter for each action in data table. This will be cleaned up when action is completed or when write client is closed.
  protected Map<String, Option<HoodieTableMetadataWriter>> metadataWriterMap = new ConcurrentHashMap<>();

  public SparkRDDWriteClientCommitCoordinator(HoodieEngineContext context, HoodieWriteConfig writeConfig, SparkRDDWriteClient writeClient) {
    this(context, writeConfig, writeClient.getTimelineServer(), writeClient);
  }

  public SparkRDDWriteClientCommitCoordinator(HoodieEngineContext context, HoodieWriteConfig writeConfig, Option<EmbeddedTimelineService> timelineService, SparkRDDWriteClient writeClient) {
    super(context, writeConfig, timelineService);
    this.writeClient = writeClient;
    this.writeClient.setWriteToMetadataTableHandler(new WriteToMetadataTableHandler() {
      @Override
      public void writeToMetadataTable(HoodieTable table, String instantTime, List<HoodieWriteStat> metadataWriteStatsSoFar, HoodieCommitMetadata metadata) {
        Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
        if (metadataWriterOpt.isPresent()) {
          try {
            metadataWriterOpt.get().wrapUpStreamingWriteToMetadataTableAndCompleteCommit(instantTime, writeClient.getEngineContext(), metadataWriteStatsSoFar, metadata);
            metadataWriterOpt.get().close();
          } catch (Exception e) {
            throw new HoodieException("Failed to close metadata writer ", e);
          } finally {
            metadataWriterMap.remove(instantTime);
          }
        } else {
          throw new HoodieException("Should not be reachable. Metadata Writer should have been instantiated by now");
        }
      }

      @Override
      public HoodieWriteMetadata<HoodieData<WriteStatus>> streamWriteToMetadataTable(HoodieTable table, HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata, String instantTime) {
        Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
        if (metadataWriterOpt.isPresent()) {
          metadataWriterOpt.get().startCommit(instantTime);
          HoodieData<WriteStatus> writeStatuses = maybeStreamWriteToMetadataTable(writeMetadata.getWriteStatuses(), metadataWriterOpt, table, instantTime);
          writeMetadata.setWriteStatuses(writeStatuses);
        } else {
          // should we throw exception if we can't get a metadata writer?
        }
        return writeMetadata;
      }
    });
  }

  protected void startHeartBeatClient(HoodieWriteConfig clientConfig) {
    // no op.
  }

  protected synchronized void startEmbeddedServerView() {
    // no op
  }

  protected BaseHoodieTableServiceClient getTableServiceClient(HoodieWriteConfig writeConfig) {
    return null;
  }

  /**
   * Creates a {@link HoodieTableMetadataWriter} instance to assist with writing to metadata table.
   *
   * @param triggeringInstantTimestamp instant time of interest.
   * @return Option of {@link HoodieTableMetadataWriter} if we could instantiate.
   */
  protected Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp, HoodieTable table) {

    if (!table.getMetaClient().getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT) || this.metadataWriterMap == null) {
      return Option.empty();
    }

    if (this.metadataWriterMap.containsKey(triggeringInstantTimestamp)) {
      return this.metadataWriterMap.get(triggeringInstantTimestamp);
    }

    Option<HoodieTableMetadataWriter> metadataWriterOpt = table.getMetadataWriter(triggeringInstantTimestamp, true);
    metadataWriterMap.put(triggeringInstantTimestamp, metadataWriterOpt); // populate this for every new instant time.
    // if metadata table does not exist, the map will contain an entry, with value Option.empty.
    // if not, it will contain the metadata writer instance.
    return metadataWriterMap.get(triggeringInstantTimestamp);
  }

  private HoodieData<WriteStatus> maybeStreamWriteToMetadataTable(HoodieData<WriteStatus> dtWriteStatuses, Option<HoodieTableMetadataWriter> metadataWriterOpt,
                                                                  HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table, String instantTime) {
    HoodieData<WriteStatus> allWriteStatus = dtWriteStatuses;
    HoodieData<WriteStatus> mdtWriteStatuses = metadataWriterOpt.get().streamWriteToMetadataPartitions(dtWriteStatuses, instantTime);
    allWriteStatus = allWriteStatus.union(mdtWriteStatuses);
    allWriteStatus.persist("MEMORY_AND_DISK_SER", writeClient.getEngineContext(), HoodieData.HoodieDataCacheKey.of(writeClient.getConfig().getBasePath(), instantTime));
    return allWriteStatus;
  }

  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType,
                        Map<String, List<String>> partitionToReplacedFileIds, Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc,
                        WriteStatusHandlerCallback writeStatusHandlerCallback) {
    context.setJobStatus(this.getClass().getSimpleName(), "Committing stats: " + config.getTableName());
    // Triggering the dag for writes.
    // If streaming writes are enabled, writes to both data table and metadata table gets triggers at this juncture.
    // If not, writes to data table gets triggered here.
    // When streaming writes are enabled, WriteStatus is expected to contain all stats required to generate metadata table records and so it could be fatter.
    // So, here we are converting it to lean WriteStatus which drops all additional stats and only retains the information required to proceed from here on.
    List<Pair<Boolean, WriteStatus>> writeStatusesList = writeStatuses.map(writeStatus -> Pair.of(writeStatus.isMetadataTable(), writeStatus.removeMetadataStatsAndErrorRecords())).collect();
    // Compute stats for the writes and invoke callback
    AtomicLong totalRecords = new AtomicLong(0);
    AtomicLong totalErrorRecords = new AtomicLong(0);
    writeStatusesList.forEach(pair -> {
      totalRecords.getAndAdd(pair.getValue().getTotalRecords());
      totalErrorRecords.getAndAdd(pair.getValue().getTotalErrorRecords());
    });
    boolean canProceed = writeStatusHandlerCallback.processWriteStatuses(totalRecords.get(), totalErrorRecords.get(),
        HoodieJavaRDD.of(writeStatuses.map(WriteStatus::removeMetadataStats)));

    // only if callback returns true, lets proceed. If not, bail out.
    if (canProceed) {
      // when streaming writes are enabled, writeStatuses is a mix of data table write status and mdt write status
      List<HoodieWriteStat> dataTableWriteStats = writeStatusesList.stream().filter(entry -> !entry.getKey()).map(leanWriteStatus -> leanWriteStatus.getValue().getStat()).collect(Collectors.toList());
      List<HoodieWriteStat> metadataTableWriteStats = writeStatusesList.stream().filter(Pair::getKey).map(leanWriteStatus -> leanWriteStatus.getValue().getStat()).collect(Collectors.toList());
      if (isMetadataTable) {
        // incase the current table is metadata table, for new partition instantiation we end up calling this commit method. On which case,
        // we could only see metadataTableWriteStats and no dataTableWriteStats. So, we need to reverse the list here so that we can proceed onto commit in current table as a
        // data table (where current is actually referring to a metadata table).
        ValidationUtils.checkArgument(dataTableWriteStats.isEmpty(), "For new partition initialization in Metadata,"
            + "we do not expect any writes having WriteStatus referring to data table. ");
        dataTableWriteStats.clear();
        dataTableWriteStats.addAll(metadataTableWriteStats);
        metadataTableWriteStats.clear();
      }

      return writeClient.commitStats(instantTime, dataTableWriteStats, metadataTableWriteStats, extraMetadata, commitActionType,
          partitionToReplacedFileIds, extraPreCommitFunc, false);
    } else {
      LOG.error("Exiting early due to errors with write operation ");
      return false;
    }
  }

  @Override
  public HoodieWriteConfig getConfig() {
    return writeClient.getConfig();
  }

  @Override
  public HoodieEngineContext getEngineContext() {
    return writeClient.getEngineContext();
  }

  @Override
  public String createNewInstantTime() {
    return writeClient.createNewInstantTime();
  }

  @Override
  public String createNewInstantTime(boolean shouldLock) {
    return writeClient.createNewInstantTime(shouldLock);
  }

  @Override
  public Option<EmbeddedTimelineService> getTimelineServer() {
    return writeClient.getTimelineServer();
  }

  @Override
  public HoodieHeartbeatClient getHeartbeatClient() {
    return writeClient.getHeartbeatClient();
  }

  @Override
  public void setOperationType(WriteOperationType operationType) {
    writeClient.setOperationType(operationType);
  }

  @Override
  public WriteOperationType getOperationType() {
    return writeClient.getOperationType();
  }

  @Override
  public BaseHoodieTableServiceClient<?, ?, JavaRDD<WriteStatus>> getTableServiceClient() {
    return writeClient.getTableServiceClient();
  }

  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses) {
    return writeClient.commit(instantTime, writeStatuses);
  }

  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata) {
    return writeClient.commit(instantTime, writeStatuses, extraMetadata);
  }

  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType,
                        Map<String, List<String>> partitionToReplacedFileIds) {
    return writeClient.commit(instantTime, writeStatuses, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  public boolean commitStats(String instantTime, List<HoodieWriteStat> stats, Option<Map<String, String>> extraMetadata, String commitActionType) {
    return writeClient.commitStats(instantTime, stats, extraMetadata, commitActionType);
  }

  @Override
  public boolean commitStats(String instantTime, List<HoodieWriteStat> stats, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplaceFileIds,
                             Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
    return writeClient.commitStats(instantTime, stats, extraMetadata, commitActionType, partitionToReplaceFileIds, extraPreCommitFunc);
  }

  @Override
  public void preWrite(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) {
    writeClient.preWrite(instantTime, writeOperationType, metaClient);
  }

  @Override
  public JavaRDD<WriteStatus> postWrite(HoodieWriteMetadata<JavaRDD<WriteStatus>> result, String instantTime, HoodieTable hoodieTable) {
    return (JavaRDD<WriteStatus>) writeClient.postWrite(result, instantTime, hoodieTable);
  }

  @Override
  public void runAnyPendingCompactions() {
    writeClient.runAnyPendingCompactions();
  }

  @Override
  public void runAnyPendingLogCompactions() {
    writeClient.runAnyPendingLogCompactions();
  }

  @Override
  public void savepoint(String user, String comment) {
    writeClient.savepoint(user, comment);
  }

  @Override
  public void savepoint(String instantTime, String user, String comment) {
    writeClient.savepoint(instantTime, user, comment);
  }

  @Override
  public void deleteSavepoint() {
    writeClient.deleteSavepoint();
  }

  @Override
  public void deleteSavepoint(String savepointTime) {
    writeClient.deleteSavepoint(savepointTime);
  }

  @Override
  public void restoreToSavepoint() {
    writeClient.restoreToSavepoint();
  }

  @Override
  public void restoreToSavepoint(String savepointTime) {
    writeClient.restoreToSavepoint(savepointTime);
  }

  @Override
  public boolean rollback(String commitInstantTime) throws HoodieRollbackException {
    return writeClient.rollback(commitInstantTime);
  }

  @Override
  public boolean rollback(String commitInstantTime, String rollbackInstantTimestamp) throws HoodieRollbackException {
    return writeClient.rollback(commitInstantTime, rollbackInstantTimestamp);
  }

  @Override
  public HoodieRestoreMetadata restoreToInstant(String savepointToRestoreTimestamp, boolean initialMetadataTableIfNecessary) throws HoodieRestoreException {
    return writeClient.restoreToInstant(savepointToRestoreTimestamp, initialMetadataTableIfNecessary);
  }

  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    return writeClient.clean(cleanInstantTime);
  }

  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean skipLocking) throws HoodieIOException {
    return writeClient.clean(cleanInstantTime, skipLocking);
  }

  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline, boolean skipLocking) throws HoodieIOException {
    return writeClient.clean(cleanInstantTime, scheduleInline, skipLocking);
  }

  @Override
  public HoodieCleanMetadata clean() {
    return writeClient.clean();
  }

  @Override
  public HoodieCleanMetadata clean(boolean skipLocking) {
    return writeClient.clean(skipLocking);
  }

  @Override
  public void archive() {
    writeClient.archive();
  }

  @Override
  public String startCommit() {
    return writeClient.startCommit();
  }

  @Override
  public String startCommit(String actionType, HoodieTableMetaClient metaClient) {
    return writeClient.startCommit(actionType, metaClient);
  }

  @Override
  public void startCommitWithTime(String instantTime) {
    writeClient.startCommitWithTime(instantTime);
  }

  @Override
  public void startCommitWithTime(String instantTime, String actionType) {
    writeClient.startCommitWithTime(instantTime, actionType);
  }

  @Override
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return writeClient.scheduleCompaction(extraMetadata);
  }

  @Override
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return writeClient.scheduleCompactionAtInstant(instantTime, extraMetadata);
  }

  @Override
  public Option<String> scheduleIndexing(List<MetadataPartitionType> partitionTypes, List<String> partitionPaths) {
    return writeClient.scheduleIndexing(partitionTypes, partitionPaths);
  }

  @Override
  public Option<HoodieIndexCommitMetadata> index(String indexInstantTime) {
    return writeClient.index(indexInstantTime);
  }

  @Override
  public void dropIndex(List<String> metadataPartitions) {
    writeClient.dropIndex(metadataPartitions);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> cluster(String clusteringInstantTime) {
    return writeClient.cluster(clusteringInstantTime);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(String compactionInstantTime) {
    return writeClient.compact(compactionInstantTime);
  }

  @Override
  public void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime) {
    writeClient.completeCompaction(metadata, table, compactionCommitTime);
  }

  @Override
  public void commitCompaction(String compactionInstantTime, HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionWriteMetadata, Option<HoodieTable> tableOpt) {
    writeClient.commitCompaction(compactionInstantTime, compactionWriteMetadata, tableOpt);
  }

  @Override
  public Option<String> scheduleLogCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return writeClient.scheduleLogCompaction(extraMetadata);
  }

  @Override
  public boolean scheduleLogCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return writeClient.scheduleLogCompactionAtInstant(instantTime, extraMetadata);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> logCompact(String logCompactionInstantTime) {
    return writeClient.logCompact(logCompactionInstantTime);
  }

  @Override
  public void completeLogCompaction(String compactionInstantTime, HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionWriteMetadata, Option<HoodieTable> tableOpt) {
    writeClient.completeLogCompaction(compactionInstantTime, compactionWriteMetadata, tableOpt);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    return writeClient.compact(compactionInstantTime, shouldComplete);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> logCompact(String logCompactionInstantTime, boolean shouldComplete) {
    return writeClient.logCompact(logCompactionInstantTime, shouldComplete);
  }

  @Override
  public Option<String> scheduleClustering(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return writeClient.scheduleClustering(extraMetadata);
  }

  @Override
  public boolean scheduleClusteringAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return writeClient.scheduleClusteringAtInstant(instantTime, extraMetadata);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> cluster(String clusteringInstant, boolean shouldComplete) {
    return writeClient.cluster(clusteringInstant, shouldComplete);
  }

  @Override
  public boolean purgePendingClustering(String clusteringInstant) {
    return writeClient.purgePendingClustering(clusteringInstant);
  }

  @Override
  public Option<String> scheduleTableService(Option<Map<String, String>> extraMetadata, TableServiceType tableServiceType) {
    return writeClient.scheduleTableService(extraMetadata, tableServiceType);
  }

  @Override
  public Option<String> scheduleTableService(String instantTime, Option<Map<String, String>> extraMetadata, TableServiceType tableServiceType) {
    return writeClient.scheduleTableService(instantTime, extraMetadata, tableServiceType);
  }

  @Override
  public HoodieMetrics getMetrics() {
    return writeClient.getMetrics();
  }

  @Override
  public HoodieIndex<?, ?> getIndex() {
    return writeClient.getIndex();
  }

  @Override
  public void validateAgainstTableProperties(HoodieTableConfig tableConfig, HoodieWriteConfig writeConfig) {
    writeClient.validateAgainstTableProperties(tableConfig, writeConfig);
  }

  @Override
  public void setWriteTimer(String commitType) {
    writeClient.setWriteTimer(commitType);
  }

  @Override
  public boolean lazyRollbackFailedIndexing() {
    return writeClient.lazyRollbackFailedIndexing();
  }

  @Override
  public boolean rollbackFailedWrites(HoodieTableMetaClient metaClient) {
    return writeClient.rollbackFailedWrites(metaClient);
  }

  @Override
  public void addColumn(String colName, Schema schema, String doc, String position, TableChange.ColumnPositionChange.ColumnPositionType positionType) {
    writeClient.addColumn(colName, schema, doc, position, positionType);
  }

  @Override
  public void addColumn(String colName, Schema schema) {
    writeClient.addColumn(colName, schema);
  }

  @Override
  public void deleteColumns(String... colNames) {
    writeClient.deleteColumns(colNames);
  }

  @Override
  public void renameColumn(String colName, String newName) {
    writeClient.renameColumn(colName, newName);
  }

  @Override
  public void updateColumnNullability(String colName, boolean nullable) {
    writeClient.updateColumnNullability(colName, nullable);
  }

  @Override
  public void updateColumnType(String colName, Type newType) {
    writeClient.updateColumnType(colName, newType);
  }

  @Override
  public void updateColumnComment(String colName, String doc) {
    writeClient.updateColumnComment(colName, doc);
  }

  @Override
  public void reOrderColPosition(String colName, String referColName, TableChange.ColumnPositionChange.ColumnPositionType orderType) {
    writeClient.reOrderColPosition(colName, referColName, orderType);
  }

  @Override
  public Pair<InternalSchema, HoodieTableMetaClient> getInternalSchemaAndMetaClient() {
    return writeClient.getInternalSchemaAndMetaClient();
  }

  @Override
  public void commitTableChange(InternalSchema newSchema, HoodieTableMetaClient metaClient) {
    writeClient.commitTableChange(newSchema, metaClient);
  }

  @Override
  public boolean tableServicesEnabled(HoodieWriteConfig config) {
    return writeClient.tableServicesEnabled(config);
  }

  @Override
  public boolean shouldDelegateToTableServiceManager(HoodieWriteConfig config, ActionType actionType) {
    return writeClient.shouldDelegateToTableServiceManager(config, actionType);
  }

  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType,
                        Map<String, List<String>> partitionToReplacedFileIds, Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
    return writeClient.commit(instantTime, writeStatuses, extraMetadata, commitActionType, partitionToReplacedFileIds, extraPreCommitFunc);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords) {
    return writeClient.filterExists(hoodieRecords);
  }

  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    writeClient.bootstrap(extraMetadata);
  }

  @Override
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return writeClient.upsert(records, instantTime);
  }

  @Override
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    return writeClient.upsertPreppedRecords(preppedRecords, instantTime);
  }

  @Override
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<List<Pair<String, String>>> partitionFileIdPairsOpt) {
    return writeClient.upsertPreppedRecords(preppedRecords, instantTime, partitionFileIdPairsOpt);
  }

  @Override
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return writeClient.insert(records, instantTime);
  }

  @Override
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime) {
    return writeClient.insertPreppedRecords(preppedRecords, instantTime);
  }

  @Override
  public HoodieWriteResult insertOverwrite(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return writeClient.insertOverwrite(records, instantTime);
  }

  @Override
  public HoodieWriteResult insertOverwriteTable(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return writeClient.insertOverwriteTable(records, instantTime);
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    return writeClient.bulkInsert(records, instantTime);
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    return writeClient.bulkInsert(records, instantTime, userDefinedBulkInsertPartitioner);
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    return writeClient.bulkInsertPreppedRecords(preppedRecords, instantTime, bulkInsertPartitioner);
  }

  @Override
  public JavaRDD<WriteStatus> delete(JavaRDD<HoodieKey> keys, String instantTime) {
    return writeClient.delete(keys, instantTime);
  }

  @Override
  public JavaRDD<WriteStatus> deletePrepped(JavaRDD<HoodieRecord<T>> preppedRecord, String instantTime) {
    return writeClient.deletePrepped(preppedRecord, instantTime);
  }

  @Override
  public HoodieWriteResult deletePartitions(List<String> partitions, String instantTime) {
    return writeClient.deletePartitions(partitions, instantTime);
  }

  @Override
  public HoodieWriteResult managePartitionTTL(String instantTime) {
    return writeClient.managePartitionTTL(instantTime);
  }

  @Override
  public void releaseResources(String instantTime) {
    writeClient.releaseResources(instantTime);
  }

  @Override
  public void close() {
    writeClient.close();
    super.close();
    // close all metadata writer instances
    if (metadataWriterMap != null) {
      metadataWriterMap.entrySet().forEach(entry -> {
        if (entry.getValue().isPresent()) {
          try {
            entry.getValue().get().close();
          } catch (Exception e) {
            throw new HoodieException("Failing to close metadata writer instance for " + entry.getKey(), e);
          }
        }
      });
      metadataWriterMap.clear();
      metadataWriterMap = null;
    }
  }

  @Override
  public int hashCode() {
    return writeClient.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return writeClient.equals(obj);
  }

  @Override
  public String toString() {
    return writeClient.toString();
  }
}
