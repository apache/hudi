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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FlinkHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(FlinkHoodieBackedTableMetadataWriter.class);

  public static HoodieTableMetadataWriter create(Configuration conf, HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context) {
    return create(conf, writeConfig, context, Option.empty());
  }

  public static <T extends SpecificRecordBase> HoodieTableMetadataWriter create(Configuration conf,
                                                                                HoodieWriteConfig writeConfig,
                                                                                HoodieEngineContext context,
                                                                                Option<T> actionMetadata) {
    return new FlinkHoodieBackedTableMetadataWriter(conf, writeConfig, context, actionMetadata, Option.empty());
  }

  public static <T extends SpecificRecordBase> HoodieTableMetadataWriter create(Configuration conf,
                                                                                HoodieWriteConfig writeConfig,
                                                                                HoodieEngineContext context,
                                                                                Option<T> actionMetadata,
                                                                                Option<String> inFlightInstantTimestamp) {
    return new FlinkHoodieBackedTableMetadataWriter(conf, writeConfig, context, actionMetadata, inFlightInstantTimestamp);
  }

  <T extends SpecificRecordBase> FlinkHoodieBackedTableMetadataWriter(Configuration hadoopConf,
                                                                      HoodieWriteConfig writeConfig,
                                                                      HoodieEngineContext engineContext,
                                                                      Option<T> actionMetadata,
                                                                      Option<String> inFlightInstantTimestamp) {
    super(hadoopConf, writeConfig, engineContext, actionMetadata, inFlightInstantTimestamp);
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
  protected <T extends SpecificRecordBase> void initialize(HoodieEngineContext engineContext,
                                                           Option<T> actionMetadata,
                                                           Option<String> inflightInstantTimestamp) {
    try {
      if (enabled) {
        initializeIfNeeded(dataMetaClient, actionMetadata, inflightInstantTimestamp);
      }
    } catch (IOException e) {
      LOG.error("Failed to initialize metadata table. Disabling the writer.", e);
      enabled = false;
    }
  }

  @Override
  protected void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap,
                        boolean canTriggerTableService) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be committed to as it is not enabled");
    ValidationUtils.checkState(metadataMetaClient != null, "Metadata table is not fully initialized yet.");
    HoodieData<HoodieRecord> preppedRecords = prepRecords(partitionRecordsMap);
    List<HoodieRecord> preppedRecordList = preppedRecords.collectAsList();

    try (HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(engineContext, metadataWriteConfig)) {
      if (canTriggerTableService) {
        // trigger compaction before doing the delta commit. this is to ensure, if this delta commit succeeds in metadata table, but failed in data table,
        // we would have compacted metadata table and so could have included uncommitted data which will never be ignored while reading from metadata
        // table (since reader will filter out only from delta commits)
        compactIfNecessary(writeClient, instantTime);
      }

      if (!metadataMetaClient.getActiveTimeline().containsInstant(instantTime)) {
        // if this is a new commit being applied to metadata for the first time
        writeClient.startCommitWithTime(instantTime);
        metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);
      } else {
        Option<HoodieInstant> alreadyCompletedInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().filter(entry -> entry.getTimestamp().equals(instantTime)).lastInstant();
        if (alreadyCompletedInstant.isPresent()) {
          // this code path refers to a re-attempted commit that got committed to metadata table, but failed in datatable.
          // for eg, lets say compaction c1 on 1st attempt succeeded in metadata table and failed before committing to datatable.
          // when retried again, data table will first rollback pending compaction. these will be applied to metadata table, but all changes
          // are upserts to metadata table and so only a new delta commit will be created.
          // once rollback is complete, compaction will be retried again, which will eventually hit this code block where the respective commit is
          // already part of completed commit. So, we have to manually remove the completed instant and proceed.
          // and it is for the same reason we enabled withAllowMultiWriteOnSameInstant for metadata table.
          HoodieActiveTimeline.deleteInstantFile(metadataMetaClient.getFs(), metadataMetaClient.getMetaPath(), alreadyCompletedInstant.get());
          metadataMetaClient.reloadActiveTimeline();
        }
        // If the alreadyCompletedInstant is empty, that means there is a requested or inflight
        // instant with the same instant time.  This happens for data table clean action which
        // reuses the same instant time without rollback first.  It is a no-op here as the
        // clean plan is the same, so we don't need to delete the requested and inflight instant
        // files in the active timeline.

        // The metadata writer uses LAZY cleaning strategy without auto commit,
        // write client then checks the heartbeat expiration when committing the instant,
        // sets up the heartbeat explicitly to make the check pass.
        writeClient.getHeartbeatClient().start(instantTime);
      }

      List<WriteStatus> statuses = preppedRecordList.size() > 0
          ? writeClient.upsertPreppedRecords(preppedRecordList, instantTime)
          : Collections.emptyList();
      statuses.forEach(writeStatus -> {
        if (writeStatus.hasErrors()) {
          throw new HoodieMetadataException("Failed to commit metadata table records at instant " + instantTime);
        }
      });
      // flink does not support auto-commit yet, also the auto commit logic is not complete as BaseHoodieWriteClient now.
      writeClient.commit(instantTime, statuses, Option.empty(), HoodieActiveTimeline.DELTA_COMMIT_ACTION, Collections.emptyMap());

      // reload timeline
      metadataMetaClient.reloadActiveTimeline();
      if (canTriggerTableService) {
        cleanIfNecessary(writeClient, instantTime);
        writeClient.archive();
      }
    }

    // Update total size of the metadata and count of base/log files
    metrics.ifPresent(m -> m.updateSizeMetrics(metadataMetaClient, metadata));
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    throw new HoodieNotSupportedException("Dropping metadata index not supported for Flink metadata table yet.");
  }
}