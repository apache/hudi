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
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * Operator that generates the compaction plan with pluggable strategies on finished checkpoints.
 *
 * <p>It should be singleton to avoid conflicts.
 */
public class CompactionPlanOperator extends AbstractStreamOperator<CompactionPlanEvent>
    implements OneInputStreamOperator<Object, CompactionPlanEvent> {

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Compaction instant time.
   */
  private String compactionInstantTime;

  public CompactionPlanOperator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void open() throws Exception {
    super.open();
    initWriteClient();
  }

  @Override
  public void processElement(StreamRecord<Object> streamRecord) {
    // no operation
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws IOException {
    HoodieFlinkTable<?> table = writeClient.getHoodieTable();
    // the last instant takes the highest priority.
    Option<HoodieInstant> compactionInstant = table.getActiveTimeline().filterPendingCompactionTimeline().lastInstant();
    String compactionInstantTime = compactionInstant.isPresent() ? compactionInstant.get().getTimestamp() : null;
    if (compactionInstantTime == null) {
      // do nothing.
      LOG.info("No compaction plan for checkpoint " + checkpointId);
      return;
    }
    if (this.compactionInstantTime != null
        && Objects.equals(this.compactionInstantTime, compactionInstantTime)) {
      // do nothing
      LOG.info("Duplicate scheduling for compaction instant: " + compactionInstantTime + ", ignore");
      return;
    }
    // generate compaction plan
    // should support configurable commit metadata
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        table.getMetaClient(), compactionInstantTime);

    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      // do nothing.
      LOG.info("No compaction plan for checkpoint " + checkpointId + " and instant " + compactionInstantTime);
    } else {
      this.compactionInstantTime = compactionInstantTime;
      HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
      HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
      if (!pendingCompactionTimeline.containsInstant(instant)) {
        throw new IllegalStateException(
            "No Compaction request available at " + compactionInstantTime + " to run compaction");
      }

      // Mark instant as compaction inflight
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
      table.getMetaClient().reloadActiveTimeline();

      List<CompactionOperation> operations = compactionPlan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
      LOG.info("CompactionPlanFunction compacting " + operations + " files");
      for (CompactionOperation operation : operations) {
        output.collect(new StreamRecord<>(new CompactionPlanEvent(compactionInstantTime, operation)));
      }
    }
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<CompactionPlanEvent>> output) {
    this.output = output;
  }

  private void initWriteClient() {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(StreamerUtil.getHadoopConf()),
            new FlinkTaskContextSupplier(getRuntimeContext()));

    writeClient = new HoodieFlinkWriteClient<>(context, StreamerUtil.getHoodieClientConfig(this.conf));
  }
}
