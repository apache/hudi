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
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.common.HoodiePendingRollbackInfo;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class SparkRDDTableServiceClientCommitCoordinator<T> extends SparkRDDTableServiceClient<T> {

  private final SparkRDDTableServiceClient tableServiceClient;
  private Map<String, Option<HoodieTableMetadataWriter>> metadataWriterMap = new HashMap<>();

  protected SparkRDDTableServiceClientCommitCoordinator(HoodieEngineContext context, HoodieWriteConfig clientConfig,
                                                        Option<EmbeddedTimelineService> timelineService,
                                                        SparkRDDTableServiceClient tableServicesClient) {
    super(context, clientConfig, timelineService);
    this.tableServiceClient = tableServicesClient;
    this.tableServiceClient.setWriteToMetadataTableHandler(new WriteToMetadataTableHandler() {
      @Override
      public void writeToMetadataTable(HoodieTable table, String instantTime, List<HoodieWriteStat> metadataWriteStatsSoFar, HoodieCommitMetadata metadata) {
        Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
        if (metadataWriterOpt.isPresent()) {
          try {
            metadataWriterOpt.get().wrapUpStreamingWriteToMetadataTableAndCompleteCommit(instantTime, tableServicesClient.getEngineContext(), metadataWriteStatsSoFar, metadata);
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
    // no op
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
    allWriteStatus.persist("MEMORY_AND_DISK_SER", tableServiceClient.getEngineContext(), HoodieData.HoodieDataCacheKey.of(tableServiceClient.getConfig().getBasePath(), instantTime));
    return allWriteStatus;
  }

  @Override
  public HoodieWriteConfig getConfig() {
    return tableServiceClient.getConfig();
  }

  @Override
  public HoodieEngineContext getEngineContext() {
    return tableServiceClient.getEngineContext();
  }

  @Override
  public String createNewInstantTime() {
    return tableServiceClient.createNewInstantTime();
  }

  @Override
  public String createNewInstantTime(boolean shouldLock) {
    return tableServiceClient.createNewInstantTime(shouldLock);
  }

  @Override
  public Option<EmbeddedTimelineService> getTimelineServer() {
    return tableServiceClient.getTimelineServer();
  }

  @Override
  public HoodieHeartbeatClient getHeartbeatClient() {
    return tableServiceClient.getHeartbeatClient();
  }

  @Override
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return tableServiceClient.scheduleCompaction(extraMetadata);
  }

  @Override
  public void commitCompaction(String compactionInstantTime, HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionWriteMetadata, Option<HoodieTable> tableOpt) {
    tableServiceClient.commitCompaction(compactionInstantTime, compactionWriteMetadata, tableOpt);
  }

  @Override
  public void commitLogCompaction(String compactionInstantTime, HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata, Option<HoodieTable> tableOpt) {
    tableServiceClient.commitLogCompaction(compactionInstantTime, writeMetadata, tableOpt);
  }

  @Override
  public Option<String> scheduleLogCompaction(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return tableServiceClient.scheduleLogCompaction(extraMetadata);
  }

  @Override
  public boolean scheduleLogCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return tableServiceClient.scheduleLogCompactionAtInstant(instantTime, extraMetadata);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> logCompact(String logCompactionInstantTime) {
    return tableServiceClient.logCompact(logCompactionInstantTime);
  }

  @Override
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return tableServiceClient.scheduleCompactionAtInstant(instantTime, extraMetadata);
  }

  @Override
  public Option<String> scheduleClustering(Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return tableServiceClient.scheduleClustering(extraMetadata);
  }

  @Override
  public boolean scheduleClusteringAtInstant(String instantTime, Option<Map<String, String>> extraMetadata) throws HoodieIOException {
    return tableServiceClient.scheduleClusteringAtInstant(instantTime, extraMetadata);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> cluster(String clusteringInstant, boolean shouldComplete) {
    return tableServiceClient.cluster(clusteringInstant, shouldComplete);
  }

  @Override
  public boolean purgePendingClustering(String clusteringInstant) {
    return tableServiceClient.purgePendingClustering(clusteringInstant);
  }

  @Override
  public Option<String> scheduleTableService(String instantTime, Option<Map<String, String>> extraMetadata, TableServiceType tableServiceType) {
    return tableServiceClient.scheduleTableService(instantTime, extraMetadata, tableServiceType);
  }

  @Nullable
  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline, boolean skipLocking) throws HoodieIOException {
    return tableServiceClient.clean(cleanInstantTime, scheduleInline, skipLocking);
  }

  @Nullable
  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline) throws HoodieIOException {
    return tableServiceClient.clean(cleanInstantTime, scheduleInline);
  }

  @Override
  public Option<HoodiePendingRollbackInfo> getPendingRollbackInfo(HoodieTableMetaClient metaClient, String commitToRollback, boolean ignoreCompactionAndClusteringInstants) {
    return tableServiceClient.getPendingRollbackInfo(metaClient, commitToRollback, ignoreCompactionAndClusteringInstants);
  }

  @Override
  public boolean rollback(String commitInstantTime, Option<HoodiePendingRollbackInfo> pendingRollbackInfo, boolean skipLocking, boolean skipVersionCheck) throws HoodieRollbackException {
    return tableServiceClient.rollback(commitInstantTime, pendingRollbackInfo, skipLocking, skipVersionCheck);
  }

  @Override
  public boolean rollback(String commitInstantTime, Option<HoodiePendingRollbackInfo> pendingRollbackInfo, String rollbackInstantTime, boolean skipLocking, boolean skipVersionCheck)
      throws HoodieRollbackException {
    return tableServiceClient.rollback(commitInstantTime, pendingRollbackInfo, rollbackInstantTime, skipLocking, skipVersionCheck);
  }

  @Override
  public void rollbackFailedBootstrap() {
    tableServiceClient.rollbackFailedBootstrap();
  }

  @Override
  public boolean tableServicesEnabled(HoodieWriteConfig config) {
    return tableServiceClient.tableServicesEnabled(config);
  }

  @Override
  public boolean shouldDelegateToTableServiceManager(HoodieWriteConfig config, ActionType actionType) {
    return tableServiceClient.shouldDelegateToTableServiceManager(config, actionType);
  }

  @Override
  protected void runTableServicesInline(HoodieTable table, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    this.tableServiceClient.runTableServicesInline(table, metadata, extraMetadata);
  }

  @Override
  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return tableServiceClient.createMetaClient(loadActiveTimelineOnLoad);
  }

  @Override
  protected void resolveWriteConflict(HoodieTable table, HoodieCommitMetadata metadata, Set<String> pendingInflightAndRequestedInstants) {
    tableServiceClient.resolveWriteConflict(table, metadata, pendingInflightAndRequestedInstants);
  }

  @Override
  protected void finalizeWrite(HoodieTable table, String instantTime, List<HoodieWriteStat> stats) {
    tableServiceClient.finalizeWrite(table, instantTime, stats);
  }

  @Override
  protected void startAsyncCleanerService(BaseHoodieWriteClient writeClient) {
    tableServiceClient.startAsyncCleanerService(writeClient);
  }

  @Override
  protected void startAsyncArchiveService(BaseHoodieWriteClient writeClient) {
    tableServiceClient.startAsyncArchiveService(writeClient);
  }

  @Override
  protected void asyncClean() {
    tableServiceClient.asyncClean();
  }

  @Override
  protected void asyncArchive() {
    tableServiceClient.asyncArchive();
  }

  @Override
  protected void setTableServiceTimer(WriteOperationType operationType) {
    tableServiceClient.setTableServiceTimer(operationType);
  }

  @Override
  protected void setPendingInflightAndRequestedInstants(Set<String> pendingInflightAndRequestedInstants) {
    tableServiceClient.setPendingInflightAndRequestedInstants(pendingInflightAndRequestedInstants);
  }

  @Override
  protected void preCommit(HoodieCommitMetadata metadata) {
    tableServiceClient.preCommit(metadata);
  }

  @Override
  protected Option<String> inlineCompaction(Option<Map<String, String>> extraMetadata) {
    return tableServiceClient.inlineCompaction(extraMetadata);
  }

  @Override
  protected HoodieWriteMetadata<JavaRDD<WriteStatus>> logCompact(String logCompactionInstantTime, boolean shouldComplete) {
    return tableServiceClient.logCompact(logCompactionInstantTime, shouldComplete);
  }

  @Override
  protected Option<String> inlineLogCompact(Option<Map<String, String>> extraMetadata) {
    return tableServiceClient.inlineLogCompact(extraMetadata);
  }

  @Override
  protected void runAnyPendingCompactions(HoodieTable table) {
    tableServiceClient.runAnyPendingCompactions(table);
  }

  @Override
  protected void runAnyPendingLogCompactions(HoodieTable table) {
    tableServiceClient.runAnyPendingLogCompactions(table);
  }

  @Override
  protected Option<String> inlineScheduleCompaction(Option<Map<String, String>> extraMetadata) {
    return tableServiceClient.inlineScheduleCompaction(extraMetadata);
  }

  @Override
  protected HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    return tableServiceClient.compact(compactionInstantTime, shouldComplete);
  }

  @Override
  protected HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(HoodieTable<?, HoodieData<HoodieRecord<T>>, ?, HoodieData<WriteStatus>> table, String compactionInstantTime, boolean shouldComplete) {
    return tableServiceClient.compact(table, compactionInstantTime, shouldComplete);
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime, List<HoodieWriteStat> metadataWriteStatsSoFar) {
    tableServiceClient.completeCompaction(metadata, table, compactionCommitTime, metadataWriteStatsSoFar);
  }

  @Override
  protected void completeLogCompaction(HoodieCommitMetadata metadata, HoodieTable table, String logCompactionCommitTime, List<HoodieWriteStat> metadataWriteStatsSoFar) {
    tableServiceClient.completeLogCompaction(metadata, table, logCompactionCommitTime, metadataWriteStatsSoFar);
  }

  @Override
  protected Option<String> scheduleTableServiceInternal(String instantTime, Option<Map<String, String>> extraMetadata, TableServiceType tableServiceType) {
    return tableServiceClient.scheduleTableServiceInternal(instantTime, extraMetadata, tableServiceType);
  }

  @Override
  protected HoodieTable createTableAndValidate(HoodieWriteConfig config, BiFunction<HoodieWriteConfig, HoodieEngineContext, HoodieTable> createTableFn, boolean skipValidation) {
    return tableServiceClient.createTableAndValidate(config, createTableFn, skipValidation);
  }

  @Override
  protected HoodieTable<?, HoodieData<HoodieRecord<T>>, ?, HoodieData<WriteStatus>> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf) {
    return tableServiceClient.createTable(config, storageConf);
  }

  @Override
  protected Option<String> inlineClustering(Option<Map<String, String>> extraMetadata) {
    return tableServiceClient.inlineClustering(extraMetadata);
  }

  @Override
  protected Option<String> inlineScheduleClustering(Option<Map<String, String>> extraMetadata) {
    return tableServiceClient.inlineScheduleClustering(extraMetadata);
  }

  @Override
  protected void runAnyPendingClustering(HoodieTable table) {
    tableServiceClient.runAnyPendingClustering(table);
  }

  @Override
  protected void archive(HoodieTable table) {
    tableServiceClient.archive(table);
  }

  @Override
  protected Option<HoodiePendingRollbackInfo> getPendingRollbackInfo(HoodieTableMetaClient metaClient, String commitToRollback) {
    return tableServiceClient.getPendingRollbackInfo(metaClient, commitToRollback);
  }

  @Override
  protected Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient) {
    return tableServiceClient.getPendingRollbackInfos(metaClient);
  }

  @Override
  protected Map<String, Option<HoodiePendingRollbackInfo>> getPendingRollbackInfos(HoodieTableMetaClient metaClient, boolean ignoreCompactionAndClusteringInstants) {
    return tableServiceClient.getPendingRollbackInfos(metaClient, ignoreCompactionAndClusteringInstants);
  }

  @Override
  protected boolean rollbackFailedIndexingCommits() {
    return tableServiceClient.rollbackFailedIndexingCommits();
  }

  @Override
  protected boolean rollbackFailedWrites(HoodieTableMetaClient metaClient) {
    return tableServiceClient.rollbackFailedWrites(metaClient);
  }

  @Override
  protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback) {
    tableServiceClient.rollbackFailedWrites(instantsToRollback);
  }

  @Override
  protected void rollbackFailedWrites(Map<String, Option<HoodiePendingRollbackInfo>> instantsToRollback, boolean skipLocking, boolean skipVersionCheck) {
    tableServiceClient.rollbackFailedWrites(instantsToRollback, skipLocking, skipVersionCheck);
  }

  @Override
  protected List<String> getInstantsToRollback(HoodieTableMetaClient metaClient, HoodieFailedWritesCleaningPolicy cleaningPolicy, Option<String> curInstantTime) {
    return tableServiceClient.getInstantsToRollback(metaClient, cleaningPolicy, curInstantTime);
  }

  @Override
  protected boolean isPreCommitRequired() {
    return tableServiceClient.isPreCommitRequired();
  }

  @Override
  protected void handleWriteErrors(List<HoodieWriteStat> writeStats, TableServiceType tableServiceType) {
    tableServiceClient.handleWriteErrors(writeStats, tableServiceType);
  }

  @Override
  protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    tableServiceClient.updateColumnsToIndexWithColStats(metaClient, columnsToIndex);
  }

  @Override
  protected HoodieTable<?, HoodieData<HoodieRecord<T>>, ?, HoodieData<WriteStatus>> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
    return tableServiceClient.createTable(config, storageConf, skipValidation);
  }

  @Override
  protected void releaseResources(String instantTime) {
    tableServiceClient.releaseResources(instantTime);
  }

  @Override
  public void close() {
    tableServiceClient.close();
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
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
