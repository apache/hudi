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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;

/**
 * Flink hoodie backed table metadata writer.
 */
public class FlinkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkHoodieBackedTableMetadataWriter.class);
  private transient BaseHoodieWriteClient writeClient;

  public static HoodieTableMetadataWriter create(Configuration conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return new FlinkHoodieBackedTableMetadataWriter(conf, writeConfig, EAGER, context, Option.empty());
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inFlightInstantTimestamp) {
    return new FlinkHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inFlightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inFlightInstantTimestamp) {
    return new FlinkHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inFlightInstantTimestamp);
  }

  FlinkHoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                       HoodieWriteConfig writeConfig,
                                       HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                       HoodieEngineContext engineContext,
                                       Option<String> inFlightInstantTimestamp) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inFlightInstantTimestamp);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      // should support executor metrics
      Registry registry = Registry.getRegistry("HoodieMetadata");
      this.metrics = Option.of(new HoodieMetadataMetrics(registry));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap) {
    doCommit(instantTime, partitionRecordsMap, false);
  }

  @Override
  protected void bulkCommit(String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records, int fileGroupCount) {
    Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap = new HashMap<>();
    partitionRecordsMap.put(partitionType, records);
    doCommit(instantTime, partitionRecordsMap, true);
  }

  private void doCommit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap, boolean isInitializing) {
    ValidationUtils.checkState(metadataMetaClient != null, "Metadata table is not fully initialized yet.");
    HoodieData<HoodieRecord> preppedRecords = prepRecords(partitionRecordsMap);
    List<HoodieRecord> preppedRecordList = preppedRecords.collectAsList();

    //  Flink engine does not optimize initialCommit to MDT as bulk insert is not yet supported

    try (HoodieFlinkWriteClient writeClient = (HoodieFlinkWriteClient) getWriteClient()) {
      // rollback partially failed writes if any.
      if (writeClient.rollbackFailedWrites()) {
        metadataMetaClient = HoodieTableMetaClient.reload(metadataMetaClient);
      }
      metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant().ifPresent(instant -> compactIfNecessary(writeClient, instant.getTimestamp()));

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
        Option<HoodieInstant> alreadyCompletedInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.getTimestamp().equals(instantTime))
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
      metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);

      List<WriteStatus> statuses = isInitializing
          ? writeClient.bulkInsertPreppedRecords(preppedRecordList, instantTime, Option.empty())
          : writeClient.upsertPreppedRecords(preppedRecordList, instantTime);
      // flink does not support auto-commit yet, also the auto commit logic is not complete as BaseHoodieWriteClient now.
      writeClient.commit(instantTime, statuses, Option.empty(), HoodieActiveTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap());

      // reload timeline
      metadataMetaClient.reloadActiveTimeline();
      metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant().ifPresent(instant -> cleanIfNecessary(writeClient, instant.getTimestamp()));
      writeClient.archive();
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metadataMetaClient, metadata, dataMetaClient.getTableConfig().getMetadataPartitions()));
  }

  /**
   * Validates the timeline for both main and metadata tables to ensure compaction on MDT can be scheduled.
   */
  @Override
  protected boolean validateTimelineBeforeSchedulingCompaction(Option<String> inFlightInstantTimestamp, String latestDeltaCommitTimeInMetadataTable) {
    // Allows compaction of the metadata table to run regardless of inflight instants
    return true;
  }

  @Override
  protected void validateRollback(String commitToRollbackInstantTime, HoodieInstant compactionInstant, HoodieTimeline deltacommitsSinceCompaction) {
    // ignore, flink has more radical compression strategy, it is very probably that
    // the latest compaction instant has greater timestamp than the instant to roll back.

    // The limitation can be relaxed because the log reader of MDT only accepts valid instants
    // based on the DT timeline, so the base file of MDT does not include un-committed instants.
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    throw new HoodieNotSupportedException("Dropping metadata index not supported for Flink metadata table yet.");
  }

  @Override
  public BaseHoodieWriteClient getWriteClient() {
    if (writeClient == null) {
      writeClient = new HoodieFlinkWriteClient(engineContext, metadataWriteConfig);
    }
    return writeClient;
  }
}
