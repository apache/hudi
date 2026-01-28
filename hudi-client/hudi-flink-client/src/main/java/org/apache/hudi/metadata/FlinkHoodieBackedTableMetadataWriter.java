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

package org.apache.hudi.metadata;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.BulkInsertPartitioner;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;

/**
 * Flink hoodie backed table metadata writer.
 */
@Slf4j
public class FlinkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<WriteStatus>> {

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return new FlinkHoodieBackedTableMetadataWriter(conf, writeConfig, EAGER, context, Option.empty());
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inFlightInstantTimestamp) {
    return new FlinkHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inFlightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inFlightInstantTimestamp,
                                                 boolean streamingWrites) {
    return new FlinkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inFlightInstantTimestamp, streamingWrites);
  }

  FlinkHoodieBackedTableMetadataWriter(StorageConfiguration<?> storageConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inFlightInstantTimestamp) {
    super(storageConf, writeConfig, failedWritesCleaningPolicy, engineContext, inFlightInstantTimestamp);
  }

  FlinkHoodieBackedTableMetadataWriter(StorageConfiguration<?> storageConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inFlightInstantTimestamp,
                                       boolean streamingWrites) {
    super(storageConf, writeConfig, failedWritesCleaningPolicy, engineContext, inFlightInstantTimestamp, streamingWrites);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      // should support executor metrics
      this.metrics = Option.of(new HoodieMetadataMetrics(metadataWriteConfig.getMetricsConfig(), dataMetaClient.getStorage()));
    } else {
      this.metrics = Option.empty();
    }
  }

  /**
   * Return the write client for metadata table, which will be used for compaction scheduling in flink write coordinator.
   */
  @Override
  public BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> getWriteClient() {
    return super.getWriteClient();
  }

  @Override
  protected void updateColumnsToIndexWithColStats(List<String> columnsToIndex) {
    // no op. HUDI-8801 to fix.
  }

  @Override
  protected void commit(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap) {
    commitInternal(instantTime, partitionRecordsMap, false, Option.empty());
  }

  @Override
  protected List<HoodieRecord> convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records) {
    return records.collectAsList();
  }

  @Override
  protected HoodieData<WriteStatus> convertEngineSpecificDataToHoodieData(List<WriteStatus> records) {
    return HoodieListData.lazy(records);
  }

  @Override
  protected void bulkCommit(String instantTime, String partitionPath, HoodieData<HoodieRecord> records, MetadataTableFileGroupIndexParser fileGroupIndexParser) {
    // TODO: functional and secondary index are not supported with Flink yet, but we should fix the partition name when we support them.
    commitInternal(instantTime, Collections.singletonMap(partitionPath, records), true, Option.empty());
  }

  @Override
  protected void commitInternal(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap, boolean isInitializing,
                                Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    // this method will be invoked during compaction of data table, here table services for mdt is
    // not performed if streaming writes mdt is enabled, since the compaction/clean will be performed
    // asynchronously in the dedicated compaction pipeline.
    if (!dataWriteConfig.getMetadataConfig().isStreamingWriteEnabled()) {
      performTableServices(Option.ofNullable(instantTime), false);
    }
    metadataMetaClient.reloadActiveTimeline();
    super.commitInternal(instantTime, partitionRecordsMap, isInitializing, bulkInsertPartitioner);
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    throw new HoodieNotSupportedException("Dropping metadata index not supported for Flink metadata table yet.");
  }

  @Override
  public BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> initializeWriteClient() {
    return new HoodieFlinkWriteClient(engineContext, metadataWriteConfig);
  }

  @Override
  protected void preWrite(String instantTime) {
    metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);
  }

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism, HoodieSchema tableSchema, HoodieSchema readerSchema,
                                                               StorageConfiguration<?> storageConf, String instantTime) {
    throw new HoodieNotSupportedException("Flink metadata table does not support expression index yet.");
  }

  @Override
  protected List<WriteStatus> streamWriteToMetadataTable(Pair<List<HoodieFileGroupId>, HoodieData<HoodieRecord>> fileGroupIdToTaggedRecords, String instantTime) {
    return getWriteClient().upsertPreppedRecords(fileGroupIdToTaggedRecords.getValue().collectAsList(), instantTime);
  }

  @Override
  public List<WriteStatus> secondaryWriteToMetadataTablePartitions(List<HoodieRecord> preppedRecords, String instantTime) {
    return getWriteClient().upsertPreppedRecords(preppedRecords, instantTime);
  }

  protected void bulkInsertAndCommit(BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> writeClient, String instantTime, List<HoodieRecord> preppedRecordInputs,
                                     Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    List<WriteStatus> writeStatusJavaRDD = writeClient.bulkInsertPreppedRecords(preppedRecordInputs, instantTime, bulkInsertPartitioner);
    writeClient.commit(instantTime, writeStatusJavaRDD);
  }

  @Override
  protected void upsertAndCommit(BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> writeClient, String instantTime, List<HoodieRecord> preppedRecordInputs) {
    List<WriteStatus> writeStatusJavaRDD = writeClient.upsertPreppedRecords(preppedRecordInputs, instantTime);
    writeClient.commit(instantTime, writeStatusJavaRDD);
  }

  @Override
  protected void upsertAndCommit(BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> writeClient, String instantTime, List<HoodieRecord> preppedRecordInputs,
                                 List<HoodieFileGroupId> fileGroupsIdsToUpdate) {
    throw new UnsupportedOperationException("Not implemented for Flink engine yet");
  }

  @Override
  public void startCommit(String instantTime) {
    super.startCommit(instantTime);
    // for streaming writing to metadata table, the commit will first start in coordinator, and writing in writer tasks,
    // so also transition the instant to inflight.
    this.metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieTimeline.DELTA_COMMIT_ACTION, instantTime);
  }

  @Override
  protected EngineType getEngineType() {
    return EngineType.FLINK;
  }
}
