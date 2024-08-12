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

package org.apache.hudi.sink.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkTables;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Operator that generates the compaction plan with pluggable strategies on finished checkpoints.
 *
 * <p>It should be singleton to avoid conflicts.
 */
public class CompactionPlanOperator extends AbstractStreamOperator<CompactionPlanEvent>
    implements OneInputStreamOperator<Object, CompactionPlanEvent>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionPlanOperator.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Meta Client.
   */
  @SuppressWarnings("rawtypes")
  private transient HoodieFlinkTable table;

  private transient FlinkCompactionMetrics compactionMetrics;

  public CompactionPlanOperator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open() throws Exception {
    super.open();
    registerMetrics();
    this.table = FlinkTables.createTable(conf, getRuntimeContext());
    // when starting up, rolls back all the inflight compaction instants if there exists,
    // these instants are in priority for scheduling task because the compaction instants are
    // scheduled from earliest(FIFO sequence).
    CompactionUtil.rollbackCompaction(table);
  }

  @Override
  public void processElement(StreamRecord<Object> streamRecord) {
    // no operation
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    try {
      table.getMetaClient().reloadActiveTimeline();
      // There is no good way to infer when the compaction task for an instant crushed
      // or is still undergoing. So we use a configured timeout threshold to control the rollback:
      // {@code FlinkOptions.COMPACTION_TIMEOUT_SECONDS},
      // when the earliest inflight instant has timed out, assumes it has failed
      // already and just rolls it back.

      // comment out: do we really need the timeout rollback ?
      // CompactionUtil.rollbackEarliestCompaction(table, conf);
      scheduleCompaction(table, checkpointId);
    } catch (Throwable throwable) {
      // make it fail-safe
      LOG.error("Error while scheduling compaction plan for checkpoint: " + checkpointId, throwable);
    }
  }

  private void scheduleCompaction(HoodieFlinkTable<?> table, long checkpointId) throws IOException {
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();

    // the first instant takes the highest priority.
    Option<HoodieInstant> firstRequested = pendingCompactionTimeline
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED).firstInstant();
    // record metrics
    compactionMetrics.setFirstPendingCompactionInstant(firstRequested);
    compactionMetrics.setPendingCompactionCount(pendingCompactionTimeline.countInstants());

    if (!firstRequested.isPresent()) {
      // do nothing.
      LOG.info("No compaction plan for checkpoint " + checkpointId);
      return;
    }

    String compactionInstantTime = firstRequested.get().getTimestamp();

    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        table.getMetaClient(), compactionInstantTime);

    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      // do nothing.
      LOG.info("Empty compaction plan for instant " + compactionInstantTime);
    } else {
      HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
      table.getMetaClient().reloadActiveTimeline();

      List<CompactionOperation> operations = compactionPlan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
      LOG.info("Execute compaction plan for instant {} as {} file groups", compactionInstantTime, operations.size());
      WriteMarkersFactory
          .get(table.getConfig().getMarkersType(), table, compactionInstantTime)
          .deleteMarkerDir(table.getContext(), table.getConfig().getMarkersDeleteParallelism());
      for (CompactionOperation operation : operations) {
        output.collect(new StreamRecord<>(new CompactionPlanEvent(compactionInstantTime, operation)));
      }
    }
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<CompactionPlanEvent>> output) {
    this.output = output;
  }

  @Override
  public void endInput() throws Exception {
    // Called when the input data ends, only used in batch mode.
    notifyCheckpointComplete(-1);
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    compactionMetrics = new FlinkCompactionMetrics(metrics);
    compactionMetrics.registerMetrics();
  }
}
