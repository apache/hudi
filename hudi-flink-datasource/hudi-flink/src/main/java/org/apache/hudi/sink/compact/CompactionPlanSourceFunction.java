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

package org.apache.hudi.sink.compact;

import org.apache.hudi.adapter.AbstractRichFunctionAdapter;
import org.apache.hudi.adapter.SourceFunctionAdapter;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink hudi compaction source function.
 *
 * <P>This function read the compaction plan as {@link CompactionOperation}s then assign the compaction task
 * event {@link CompactionPlanEvent} to downstream operators.
 *
 * <p>The compaction instant time is specified explicitly with strategies:
 *
 * <ul>
 *   <li>If the timeline has no inflight instants,
 *   use {@link org.apache.hudi.common.table.timeline.HoodieActiveTimeline#createNewInstantTime() as the instant time;</li>
 *   <li>If the timeline has inflight instants,
 *   use the median instant time between [last complete instant time, earliest inflight instant time]
 *   as the instant time.</li>
 * </ul>
 */
public class CompactionPlanSourceFunction extends AbstractRichFunctionAdapter implements SourceFunctionAdapter<CompactionPlanEvent> {

  protected static final Logger LOG = LoggerFactory.getLogger(CompactionPlanSourceFunction.class);

  /**
   * compaction plan instant -> compaction plan
   */
  private final List<Pair<String, HoodieCompactionPlan>> compactionPlans;
  private final Configuration conf;

  public CompactionPlanSourceFunction(List<Pair<String, HoodieCompactionPlan>> compactionPlans, Configuration conf) {
    this.compactionPlans = compactionPlans;
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    // no operation
  }

  @Override
  public void run(SourceContext sourceContext) throws Exception {
    HoodieTimeline pendingCompactionTimeline = StreamerUtil.createMetaClient(conf).getActiveTimeline().filterPendingCompactionTimeline();
    for (Pair<String, HoodieCompactionPlan> pair : compactionPlans) {
      if (!pendingCompactionTimeline.containsInstant(pair.getLeft())) {
        LOG.warn(pair.getLeft() + " not found in pending compaction instants.");
        continue;
      }
      HoodieCompactionPlan compactionPlan = pair.getRight();
      List<CompactionOperation> operations = compactionPlan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(Collectors.toList());
      LOG.info("CompactionPlanFunction compacting " + operations + " files");
      for (CompactionOperation operation : operations) {
        sourceContext.collect(new CompactionPlanEvent(pair.getLeft(), operation));
      }
    }
  }

  @Override
  public void close() throws Exception {
    // no operation
  }

  @Override
  public void cancel() {
    // no operation
  }
}
