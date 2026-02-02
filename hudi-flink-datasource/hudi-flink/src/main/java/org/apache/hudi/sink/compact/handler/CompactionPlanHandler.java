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
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.CompactionUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;

/**
 * Handler for scheduling and managing compaction plans in compaction sub-pipeline.
 *
 * <p>The responsibilities:
 * <ul>
 *   <li>Retrieves pending compaction plans from the timeline;</li>
 *   <li>Transitions compaction instants from REQUESTED to INFLIGHT state;</li>
 *   <li>Distributes compaction operations to downstream tasks;</li>
 *   <li>Rolls back failed compactions.</li>
 * </ul>
 *
 * <p>The handler reads the Hudi timeline to detect the target pending compaction
 * instant and generates compaction plan events for each compaction plan operation.
 *
 * @see CompactionPlanEvent
 * @see HoodieFlinkWriteClient
 */
@Slf4j
public class CompactionPlanHandler implements Closeable {
  protected final HoodieFlinkTable table;
  protected final HoodieFlinkWriteClient writeClient;

  /**
   * Constructs a new CompactionPlanHandler.
   *
   * @param writeClient the Hudi Flink write client for table operations
   */
  public CompactionPlanHandler(HoodieFlinkWriteClient writeClient) {
    this.table = writeClient.getHoodieTable();
    this.writeClient = writeClient;
  }

  /**
   * Retrieve the first pending compaction plan and distribute compaction operations to downstream tasks.
   *
   * @param checkpointId      The Flink checkpoint ID
   * @param compactionMetrics Metrics collector for compaction operations
   * @param output            The output collector for emitting compaction plan events
   */
  public void collectCompactionOperations(
      long checkpointId,
      FlinkCompactionMetrics compactionMetrics,
      Output<StreamRecord<CompactionPlanEvent>> output) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    metaClient.reloadActiveTimeline();

    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    Option<Pair<String, HoodieCompactionPlan>> instantAndPlanOpt =
        getCompactionPlan(metaClient, pendingCompactionTimeline, checkpointId, compactionMetrics, CompactionUtils::getCompactionPlan);
    if (instantAndPlanOpt.isEmpty()) {
      return;
    }
    doCollectCompactionOperations(instantAndPlanOpt.get().getLeft(), instantAndPlanOpt.get().getRight(), output);
  }

  /**
   * Rolls back any pending compaction operations.
   *
   * <p>This method is typically called during failure recovery to clean up
   * incomplete compaction attempts.
   */
  public void rollbackCompaction() {
    CompactionUtil.rollbackCompaction(table, this.writeClient);
  }

  /**
   * Retrieves the first pending compaction plan from the timeline.
   *
   * @param metaClient                The table meta client
   * @param pendingCompactionTimeline The timeline containing pending compaction instants
   * @param checkpointId              The Flink checkpoint ID
   * @param compactionMetrics         Metrics collector for compaction operations
   * @param planGenerator             Function to generate the compaction plan from meta client and instant
   * @return an optional pair of instant time and compaction plan, empty if no valid plan exists
   */
  protected Option<Pair<String, HoodieCompactionPlan>> getCompactionPlan(
      HoodieTableMetaClient metaClient,
      HoodieTimeline pendingCompactionTimeline,
      long checkpointId,
      FlinkCompactionMetrics compactionMetrics,
      BiFunction<HoodieTableMetaClient, String, HoodieCompactionPlan> planGenerator) {
    // the first instant takes the highest priority.
    Option<HoodieInstant> firstRequested = pendingCompactionTimeline
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED).firstInstant();
    // record metrics
    compactionMetrics.setFirstPendingCompactionInstant(firstRequested);
    compactionMetrics.setPendingCompactionCount(pendingCompactionTimeline.countInstants());

    if (firstRequested.isEmpty()) {
      log.info("No compaction plan for checkpoint {}", checkpointId);
      return Option.empty();
    }

    String compactionInstantTime = firstRequested.get().requestedTime();
    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = planGenerator.apply(metaClient, compactionInstantTime);
    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      log.info("Empty compaction plan for instant {}", compactionInstantTime);
      return Option.empty();
    }
    return Option.of(Pair.of(compactionInstantTime, compactionPlan));
  }

  /**
   * Collects and distributes compaction operations for a Hudi table.
   *
   * <p>This method:
   * <ul>
   *   <li>Transitions the compaction instant from REQUESTED to INFLIGHT;</li>
   *   <li>Deletes any existing marker directories;</li>
   *   <li>Creates a compaction plan event for each operation;</li>
   *   <li>Assigns operation indices to ensure proper task distribution;</li>
   * </ul>
   *
   * @param compactionInstantTime The instant time for this compaction
   * @param compactionPlan        The compaction plan containing operations to execute
   * @param output                The output collector for emitting compaction plan events
   */
  protected void doCollectCompactionOperations(
      String compactionInstantTime,
      HoodieCompactionPlan compactionPlan,
      Output<StreamRecord<CompactionPlanEvent>> output) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    // Mark instant as compaction inflight
    HoodieInstant instant = metaClient.getInstantGenerator().getCompactionRequestedInstant(compactionInstantTime);
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
    metaClient.reloadActiveTimeline();

    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    log.info("Execute compaction plan for instant {} as {} file groups", compactionInstantTime, operations.size());

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
      output.collect(new StreamRecord<>(createPlanEvent(compactionInstantTime, operation, operationIndex)));
    }
  }

  /**
   * Creates a compaction plan event for a single compaction operation.
   *
   * @param compactionInstantTime The instant time for this compaction
   * @param operation             The compaction operation to execute
   * @param operationIndex        The index of this operation for task distribution
   * @return a new compaction plan event
   */
  protected CompactionPlanEvent createPlanEvent(String compactionInstantTime, CompactionOperation operation, int operationIndex) {
    return new CompactionPlanEvent(compactionInstantTime, operation, operationIndex);
  }

  /**
   * Closes the handler and releases resources.
   */
  @Override
  public void close() {
    this.writeClient.close();
  }
}
