/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.table.HoodieFlinkCopyOnWriteTable;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * A flink implementation of {@link AbstractCompactHelpers}.
 *
 * @param <T>
 */
public class FlinkCompactHelpers<T extends HoodieRecordPayload> extends
    AbstractCompactHelpers<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkCompactHelpers.class);

  private FlinkCompactHelpers() {
  }

  private static class CompactHelperHolder {
    private static final FlinkCompactHelpers FLINK_COMPACT_HELPERS = new FlinkCompactHelpers();
  }

  public static FlinkCompactHelpers newInstance() {
    return CompactHelperHolder.FLINK_COMPACT_HELPERS;
  }

  @Override
  public HoodieCommitMetadata createCompactionMetadata(HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                                       String compactionInstantTime,
                                                       List<WriteStatus> writeStatuses,
                                                       String schema) throws IOException {
    byte[] planBytes = table.getActiveTimeline().readCompactionPlanAsBytes(
        HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get();
    HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(planBytes);
    List<HoodieWriteStat> updateStatusMap = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    org.apache.hudi.common.model.HoodieCommitMetadata metadata = new org.apache.hudi.common.model.HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }
    metadata.addMetadata(org.apache.hudi.common.model.HoodieCommitMetadata.SCHEMA_KEY, schema);
    if (compactionPlan.getExtraMetadata() != null) {
      compactionPlan.getExtraMetadata().forEach(metadata::addMetadata);
    }
    return metadata;
  }

  @SuppressWarnings("unchecked, rawtypes")
  public static List<WriteStatus> compact(
      HoodieFlinkWriteClient writeClient,
      String compactInstantTime,
      CompactionOperation compactionOperation) throws IOException {
    HoodieFlinkMergeOnReadTableCompactor compactor = new HoodieFlinkMergeOnReadTableCompactor();
    return compactor.compact(
        new HoodieFlinkCopyOnWriteTable<>(
            writeClient.getConfig(),
            writeClient.getEngineContext(),
            writeClient.getHoodieTable().getMetaClient()),
        writeClient.getHoodieTable().getMetaClient(),
        writeClient.getConfig(),
        compactionOperation,
        compactInstantTime);
  }

  /**
   * Called by the metadata table compactor code path.
   */
  @SuppressWarnings("unchecked, rawtypes")
  public static List<WriteStatus> compact(String compactionInstantTime, HoodieFlinkWriteClient writeClient) throws IOException {
    HoodieFlinkTable table = writeClient.getHoodieTable();
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      writeClient.rollbackInflightCompaction(inflightInstant, table);
      table.getMetaClient().reloadActiveTimeline();
    }

    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
          table.getMetaClient(), compactionInstantTime);

    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      // do nothing.
      LOG.info("No compaction plan for instant " + compactionInstantTime);
      return Collections.emptyList();
    } else {
      HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
      table.getMetaClient().reloadActiveTimeline();

      List<CompactionOperation> operations = compactionPlan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
      LOG.info("Compacting " + operations + " files");
      List<WriteStatus> writeStatusList = new ArrayList<>();
      for (CompactionOperation operation : operations) {
        List<WriteStatus> statuses = compact(writeClient, compactionInstantTime, operation);
        writeStatusList.addAll(statuses);
      }
      return writeStatusList;
    }
  }
}

