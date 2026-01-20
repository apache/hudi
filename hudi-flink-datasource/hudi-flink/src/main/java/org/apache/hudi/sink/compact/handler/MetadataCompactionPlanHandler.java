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

package org.apache.hudi.sink.compact.handler;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.CompactionUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Handler for scheduling compaction plans on Hudi metadata tables.
 *
 * <p>This handler extends {@link CompactionPlanHandler} to support metadata table specific
 * compaction operations, including:
 * <ul>
 *   <li>Compaction;</li>
 *   <li>Log compaction(if enabled);</li>
 *   <li>Rollback of the compactions;</li>
 * </ul>
 *
 * <p>The handler first attempts to schedule regular compaction. If no regular compaction
 * is pending and log compaction is enabled, it will schedule log compaction instead.
 * This ensures efficient file layout of metadata table storage.
 *
 * @see CompactionPlanHandler
 * @see CompactionPlanEvent
 */
@Slf4j
public class MetadataCompactionPlanHandler extends CompactionPlanHandler {

  public MetadataCompactionPlanHandler(HoodieFlinkWriteClient writeClient) {
    super(writeClient);
  }

  /**
   * Schedules compaction operations for metadata tables.
   *
   * <p>This method is overridden to support both regular compaction(full compaction)
   * and log compaction(minor compaction) for metadata tables. It first attempts to schedule regular compaction.
   * If no regular compaction is pending and log compaction is enabled, it schedules log
   * compaction instead.
   *
   * @param checkpointId      The Flink checkpoint ID triggering this scheduling
   * @param compactionMetrics Metrics collector for tracking compaction progress
   * @param output            Output stream for emitting compaction plan events
   */
  @Override
  public void collectCompactionOperations(long checkpointId, FlinkCompactionMetrics compactionMetrics, Output<StreamRecord<CompactionPlanEvent>> output) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    metaClient.reloadActiveTimeline();
    // retrieve compaction plan
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    Option<Pair<String, HoodieCompactionPlan>> instantAndPlanOpt = getCompactionPlan(
        metaClient, pendingCompactionTimeline, checkpointId, compactionMetrics, CompactionUtils::getCompactionPlan);
    if (instantAndPlanOpt.isPresent()) {
      doCollectCompactionOperations(instantAndPlanOpt.get().getLeft(), instantAndPlanOpt.get().getRight(), output);
      return;
    }
    if (!writeClient.getConfig().isLogCompactionEnabled()) {
      return;
    }
    // schedule log compaction
    HoodieTimeline pendingLogCompactionTimeline = metaClient.getActiveTimeline().filterPendingLogCompactionTimeline();
    instantAndPlanOpt = getCompactionPlan(
        metaClient, pendingLogCompactionTimeline, checkpointId, compactionMetrics, CompactionUtils::getLogCompactionPlan);
    if (instantAndPlanOpt.isPresent()) {
      doCollectLogCompactions(instantAndPlanOpt.get().getLeft(), instantAndPlanOpt.get().getRight(), output);
    }
  }

  /**
   * Rolls back pending compaction operations for metadata tables.
   *
   * <p>This method is overridden to support rolling back both normal compaction and log compaction.
   */
  @Override
  public void rollbackCompaction() {
    super.rollbackCompaction();
    if (writeClient.getConfig().isLogCompactionEnabled()) {
      CompactionUtil.rollbackLogCompaction(table, writeClient);
    }
  }

  /**
   * Creates a compaction plan event for metadata table operations.
   *
   * <p>This method is overridden to create the metadata table compaction events.
   *
   * @param compactionInstantTime The instant time for the compaction
   * @param operation             The compaction operation details
   * @param operationIndex        The index of this operation in the compaction plan
   *
   * @return A compaction plan event configured for metadata table compaction
   */
  @Override
  protected CompactionPlanEvent createPlanEvent(String compactionInstantTime, CompactionOperation operation, int operationIndex) {
    return new CompactionPlanEvent(compactionInstantTime, operation, operationIndex, true, false);
  }

  /**
   * Collects and emits log compaction plan events for metadata tables.
   *
   * <p>This method transitions the log compaction instant from requested to inflight,
   * extracts compaction operations from the plan, deletes marker directories, and
   * emits compaction plan events for each operation. Operations with the same file ID
   * are assigned the same operation index to ensure they are processed together.
   *
   * @param compactionInstantTime The instant time for the log compaction
   * @param compactionPlan        The log compaction plan containing operations to execute
   * @param output                Output stream for emitting compaction plan events
   */
  public void doCollectLogCompactions(
      String compactionInstantTime,
      HoodieCompactionPlan compactionPlan,
      Output<StreamRecord<CompactionPlanEvent>> output) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    // Mark log compaction as inflight
    HoodieInstant instant = metaClient.getInstantGenerator().getLogCompactionRequestedInstant(compactionInstantTime);
    metaClient.getActiveTimeline().transitionLogCompactionRequestedToInflight(instant);
    metaClient.reloadActiveTimeline();

    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    log.info("Execute log compaction plan for instant {} as {} file groups", compactionInstantTime, operations.size());

    WriteMarkersFactory
        .get(table.getConfig().getMarkersType(), table, compactionInstantTime)
        .deleteMarkerDir(table.getContext(), table.getConfig().getMarkersDeleteParallelism());

    Map<String, Integer> fileIdIndexMap = new HashMap<>();
    int index = 0;
    for (CompactionOperation operation : operations) {
      int operationIndex;
      if (fileIdIndexMap.containsKey(operation.getFileId())) {
        operationIndex = fileIdIndexMap.get(operation.getFileId());
      } else {
        operationIndex = index;
        fileIdIndexMap.put(operation.getFileId(), operationIndex);
        index++;
      }
      output.collect(new StreamRecord<>(
          new CompactionPlanEvent(compactionInstantTime, operation, operationIndex, true, true)));
    }
  }
}
