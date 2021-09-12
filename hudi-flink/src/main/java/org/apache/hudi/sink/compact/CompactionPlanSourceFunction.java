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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.CompactionOperation;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.util.stream.Collectors.toList;

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
 *   use {@link org.apache.hudi.common.table.timeline.HoodieActiveTimeline#createNewInstantTime()}
 *   as the instant time;</li>
 *   <li>If the timeline has inflight instants,
 *   use the median instant time between [last complete instant time, earliest inflight instant time]
 *   as the instant time.</li>
 * </ul>
 */
public class CompactionPlanSourceFunction extends AbstractRichFunction implements SourceFunction<CompactionPlanEvent> {

  protected static final Logger LOG = LoggerFactory.getLogger(CompactionPlanSourceFunction.class);

  /**
   * Compaction instant time.
   */
  private final String compactionInstantTime;

  /**
   * The compaction plan.
   */
  private final HoodieCompactionPlan compactionPlan;

  public CompactionPlanSourceFunction(HoodieCompactionPlan compactionPlan, String compactionInstantTime) {
    this.compactionPlan = compactionPlan;
    this.compactionInstantTime = compactionInstantTime;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    // no operation
  }

  @Override
  public void run(SourceContext sourceContext) throws Exception {
    List<CompactionOperation> operations = this.compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    LOG.info("CompactionPlanFunction compacting " + operations + " files");
    for (CompactionOperation operation : operations) {
      sourceContext.collect(new CompactionPlanEvent(compactionInstantTime, operation));
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
