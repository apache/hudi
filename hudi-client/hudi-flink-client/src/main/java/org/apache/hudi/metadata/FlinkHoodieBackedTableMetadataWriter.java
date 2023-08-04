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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inFlightInstantTimestamp, true);
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
    commitInternal(instantTime, partitionRecordsMap, false, Option.empty());
  }

  @Override
  protected void bulkCommit(String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records, int fileGroupCount) {
    commitInternal(instantTime, Collections.singletonMap(partitionType, records), true, Option.empty());
  }

  @Override
  protected void postInternalCommit(String instantTime, List<WriteStatus> statuses) {
    // flink does not support auto-commit yet, also the auto commit logic is not complete as BaseHoodieWriteClient now.
    writeClient.commit(instantTime, statuses, Option.empty(), HoodieActiveTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap());

    // reload timeline
    metadataMetaClient.reloadActiveTimeline();
    metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant().ifPresent(instant -> cleanIfNecessary(writeClient, instant.getTimestamp()));
    writeClient.archive();
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
