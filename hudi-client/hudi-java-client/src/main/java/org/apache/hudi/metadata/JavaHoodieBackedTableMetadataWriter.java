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
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;

public class JavaHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<List<HoodieRecord>, List<WriteStatus>> {

  /**
   * Hudi backed table metadata writer.
   *
   * @param storageConf                Storage configuration to use for the metadata writer
   * @param writeConfig                Writer config
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   * @param engineContext              Engine context
   * @param inflightInstantTimestamp   Timestamp of any instant in progress
   */
  protected JavaHoodieBackedTableMetadataWriter(StorageConfiguration<?> storageConf, HoodieWriteConfig writeConfig, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                HoodieEngineContext engineContext,
                                                Option<String> inflightInstantTimestamp) {
    super(storageConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp);
  }

  @Override
  MetadataIndexGenerator initializeMetadataIndexGenerator() {
    throw new UnsupportedOperationException("Streaming writes are not supported for Java");
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new JavaHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(StorageConfiguration<?> conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new JavaHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      this.metrics = Option.of(new HoodieMetadataMetrics(metadataWriteConfig.getMetricsConfig(), dataMetaClient.getStorage()));
    } else {
      this.metrics = Option.empty();
    }
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
  protected void bulkCommit(String instantTime, String partitionPath, HoodieData<HoodieRecord> records, MetadataTableFileGroupIndexParser indexParser) {
    commitInternal(instantTime, Collections.singletonMap(partitionPath, records), true, Option.of(new JavaHoodieMetadataBulkInsertPartitioner()));
  }

  @Override
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
    throw new UnsupportedOperationException("Not implemented for Java engine yet");
  }

  @Override
  protected BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> initializeWriteClient() {
    return new HoodieJavaWriteClient(engineContext, metadataWriteConfig);
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    throw new HoodieNotSupportedException("Dropping metadata index not supported for Java metadata table yet.");
  }

  @Override
  protected void preWrite(String instantTime) {
    metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);
  }

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism, Schema tableSchema, Schema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime) {
    throw new HoodieNotSupportedException("Expression index not supported for Java metadata table writer yet.");
  }

  @Override
  protected EngineType getEngineType() {
    return EngineType.JAVA;
  }
}
