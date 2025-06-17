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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metrics.FlinkClusteringMetrics;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Operator that generates the clustering plan with pluggable strategies on finished checkpoints.
 *
 * <p>It should be singleton to avoid conflicts.
 */
public class ClusteringPlanOperator extends AbstractStreamOperator<ClusteringPlanEvent>
    implements OneInputStreamOperator<RowData, ClusteringPlanEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(ClusteringPlanOperator.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Meta Client.
   */
  @SuppressWarnings("rawtypes")
  private transient HoodieFlinkTable table;

  private transient FlinkClusteringMetrics clusteringMetrics;

  public ClusteringPlanOperator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.table = FlinkTables.createTable(conf, getRuntimeContext());
    registerMetrics();
    // when starting up, rolls back all the inflight clustering instants if there exists,
    // these instants are in priority for scheduling task because the clustering instants are
    // scheduled from earliest(FIFO sequence).
    ClusteringUtil.rollbackClustering(table, FlinkWriteClients.createWriteClient(conf, getRuntimeContext()));
  }

  @Override
  public void processElement(StreamRecord<RowData> streamRecord) {
    // no operation
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    try {
      table.getMetaClient().reloadActiveTimeline();
      scheduleClustering(table, checkpointId);
    } catch (Throwable throwable) {
      // make it fail-safe
      LOG.error("Error while scheduling clustering plan for checkpoint: " + checkpointId, throwable);
    }
  }

  private void scheduleClustering(HoodieFlinkTable<?> table, long checkpointId) {
    List<HoodieInstant> pendingClusteringInstantTimes =
        ClusteringUtils.getPendingClusteringInstantTimes(table.getMetaClient());
    // the first instant takes the highest priority.
    Option<HoodieInstant> firstRequested = Option.fromJavaOptional(
        pendingClusteringInstantTimes.stream()
            .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED).findFirst());

    // record metrics
    clusteringMetrics.setFirstPendingClusteringInstant(firstRequested);
    clusteringMetrics.setPendingClusteringCount(pendingClusteringInstantTimes.size());

    if (!firstRequested.isPresent()) {
      // do nothing.
      LOG.info("No clustering plan for checkpoint " + checkpointId);
      return;
    }

    String clusteringInstantTime = firstRequested.get().requestedTime();

    // generate clustering plan
    // should support configurable commit metadata
    HoodieInstant clusteringInstant = firstRequested.get();
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
        table.getMetaClient(), clusteringInstant);

    if (!clusteringPlanOption.isPresent()) {
      // do nothing.
      LOG.info("No clustering plan scheduled");
      return;
    }

    HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();

    if (clusteringPlan == null || (clusteringPlan.getInputGroups() == null)
        || (clusteringPlan.getInputGroups().isEmpty())) {
      // do nothing.
      LOG.info("Empty clustering plan for instant " + clusteringInstantTime);
    } else {
      // Mark instant as clustering inflight
      ClusteringUtils.transitionClusteringOrReplaceRequestedToInflight(clusteringInstant, Option.empty(), table.getActiveTimeline());
      table.getMetaClient().reloadActiveTimeline();

      Map<String, Integer> groupIndexMap = new HashMap<>();
      int index = 0;
      for (HoodieClusteringGroup clusteringGroup : clusteringPlan.getInputGroups()) {
        ClusteringGroupInfo groupInfo = ClusteringGroupInfo.create(clusteringGroup);
        String groupFileIds = groupInfo.getOperations().stream().map(ClusteringOperation::getFileId).collect(Collectors.joining());
        int groupIndex;
        if (groupIndexMap.containsKey(groupFileIds)) {
          groupIndex = groupIndexMap.get(groupFileIds);
        } else {
          groupIndex = index;
          groupIndexMap.put(groupFileIds, groupIndex);
          index++;
        }
        LOG.info("Execute clustering plan for instant {} as {} file slices", clusteringInstantTime, clusteringGroup.getSlices().size());
        output.collect(new StreamRecord<>(
            new ClusteringPlanEvent(clusteringInstantTime, groupInfo, clusteringPlan.getStrategy().getStrategyParams(), groupIndex)
        ));
      }
    }
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<ClusteringPlanEvent>> output) {
    this.output = output;
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    clusteringMetrics = new FlinkClusteringMetrics(metrics);
    clusteringMetrics.registerMetrics();
  }
}
