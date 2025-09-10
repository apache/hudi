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
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;

/**
 * Flink hoodie backed table metadata writer.
 */
public class FlinkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<List<HoodieRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkHoodieBackedTableMetadataWriter.class);

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
                                                 Option<String> inFlightInstantTimestamp) {
    return new FlinkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inFlightInstantTimestamp);
  }

  FlinkHoodieBackedTableMetadataWriter(StorageConfiguration<?> storageConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inFlightInstantTimestamp) {
    super(storageConf, writeConfig, failedWritesCleaningPolicy, engineContext, inFlightInstantTimestamp);
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
  protected void bulkCommit(String instantTime, String partitionName, HoodieData<HoodieRecord> records, int fileGroupCount) {
    // TODO: functional and secondary index are not supported with Flink yet, but we should fix the partition name when we support them.
    commitInternal(instantTime, Collections.singletonMap(partitionName, records), true, Option.empty());
  }

  @Override
  protected void commitInternal(String instantTime, Map<String, HoodieData<HoodieRecord>> partitionRecordsMap, boolean isInitializing,
                                Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    ValidationUtils.checkState(metadataMetaClient != null, "Metadata table is not fully initialized yet.");
    HoodieData<HoodieRecord> preppedRecords = prepRecords(partitionRecordsMap);
    List<HoodieRecord> preppedRecordList = preppedRecords.collectAsList();

    //  Flink engine does not optimize initialCommit to MDT as bulk insert is not yet supported

    BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> writeClient = (BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>>) getWriteClient();
    // rollback partially failed writes if any.
    if (writeClient.rollbackFailedWrites(metadataMetaClient)) {
      metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient);
    }

    compactIfNecessary(writeClient, Option.empty());

    if (!metadataMetaClient.getActiveTimeline().containsInstant(instantTime)) {
      // if this is a new commit being applied to metadata for the first time
      LOG.info("New commit at " + instantTime + " being applied to MDT.");
    } else {
      // this code path refers to a re-attempted commit that:
      //   1. got committed to metadata table, but failed in datatable.
      //   2. failed while committing to metadata table
      // for e.g., let's say compaction c1 on 1st attempt succeeded in metadata table and failed before committing to datatable.
      // when retried again, data table will first rollback pending compaction. these will be applied to metadata table, but all changes
      // are upserts to metadata table and so only a new delta commit will be created.
      // once rollback is complete in datatable, compaction will be retried again, which will eventually hit this code block where the respective commit is
      // already part of completed commit. So, we have to manually rollback the completed instant and proceed.
      Option<HoodieInstant> alreadyCompletedInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.requestedTime().equals(instantTime))
          .lastInstant();
      LOG.info(String.format("%s completed commit at %s being applied to MDT.",
          alreadyCompletedInstant.isPresent() ? "Already" : "Partially", instantTime));

      // Rollback the previous commit
      if (!writeClient.rollback(instantTime)) {
        throw new HoodieMetadataException("Failed to rollback deltacommit at " + instantTime + " from MDT");
      }
      metadataMetaClient.reloadActiveTimeline();
    }

    writeClient.startCommitWithTime(instantTime);
    preWrite(instantTime);

    List<WriteStatus> statuses = isInitializing
        ? writeClient.bulkInsertPreppedRecords(preppedRecordList, instantTime, bulkInsertPartitioner)
        : writeClient.upsertPreppedRecords(preppedRecordList, instantTime);
    // flink does not support auto-commit yet, also the auto commit logic is not complete as BaseHoodieWriteClient now.
    writeClient.commit(instantTime, statuses, Option.empty(), HoodieActiveTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap());

    // reload timeline
    metadataMetaClient.reloadActiveTimeline();
    cleanIfNecessary(writeClient, "");
    writeClient.archive();

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metadataMetaClient, metadata, dataMetaClient.getTableConfig().getMetadataPartitions()));
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    throw new HoodieNotSupportedException("Dropping metadata index not supported for Flink metadata table yet.");
  }

  @Override
  public BaseHoodieWriteClient<?, List<HoodieRecord>, ?, ?> initializeWriteClient() {
    return new HoodieFlinkWriteClient(engineContext, metadataWriteConfig);
  }

  @Override
  protected void preWrite(String instantTime) {
    metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);
    // Every time `HFile` class is loaded in class loader, it'll create a static `MetricsIO` instance for region server IO and
    // register a metric 'RegionServer,sub=IO' in `DefaultMetricsSystem#newSourceName`. There is no configuration to disable it.
    // When the flink cluster is deployed as session mode and hudi flink bundle jar is summited with SQL or datastream job,
    // multiple userCode context classloaders may register same region server metric which leads to validating exception in
    // `DefaultMetricsSystem#newSourceName`. So here we set the init mode for hadoop metrics as `STANDBY` to disable metric
    // registering for HFile writer.
    System.setProperty("hadoop.metrics.init.mode", "STANDBY");
  }

  @Override
  protected HoodieData<HoodieRecord> getExpressionIndexRecords(List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition,
                                                               HoodieTableMetaClient metaClient, int parallelism, Schema readerSchema, StorageConfiguration<?> storageConf,
                                                               String instantTime) {
    throw new HoodieNotSupportedException("Flink metadata table does not support expression index yet.");
  }

  @Override
  protected HoodieTable getTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) {
    return HoodieFlinkTable.create(writeConfig, engineContext, metaClient);
  }

  @Override
  protected EngineType getEngineType() {
    return EngineType.FLINK;
  }
}
