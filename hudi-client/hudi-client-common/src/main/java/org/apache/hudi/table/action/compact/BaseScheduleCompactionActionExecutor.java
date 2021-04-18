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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseScheduleCompactionActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieCompactionPlan>> {

  private final Option<Map<String, String>> extraMetadata;

  public BaseScheduleCompactionActionExecutor(HoodieEngineContext context,
                                              HoodieWriteConfig config,
                                              HoodieTable<T, I, K, O> table,
                                              String instantTime,
                                              Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime);
    this.extraMetadata = extraMetadata;
  }

  protected abstract HoodieCompactionPlan scheduleCompaction();

  @Override
  public Option<HoodieCompactionPlan> execute() {
    if (!config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()
        && !config.getFailedWritesCleanPolicy().isLazy()) {
      // if there are inflight writes, their instantTime must not be less than that of compaction instant time
      table.getActiveTimeline().getCommitsTimeline().filterPendingExcludingCompaction().firstInstant()
          .ifPresent(earliestInflight -> ValidationUtils.checkArgument(
              HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), HoodieTimeline.GREATER_THAN, instantTime),
              "Earliest write inflight instant time must be later than compaction time. Earliest :" + earliestInflight
                  + ", Compaction scheduled at " + instantTime));
      // Committed and pending compaction instants should have strictly lower timestamps
      List<HoodieInstant> conflictingInstants = table.getActiveTimeline()
          .getWriteTimeline().getInstants()
          .filter(instant -> HoodieTimeline.compareTimestamps(
              instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS, instantTime))
          .collect(Collectors.toList());
      ValidationUtils.checkArgument(conflictingInstants.isEmpty(),
          "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
              + conflictingInstants);
    }

    HoodieCompactionPlan plan = scheduleCompaction();
    if (plan != null && (plan.getOperations() != null) && (!plan.getOperations().isEmpty())) {
      extraMetadata.ifPresent(plan::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      try {
        table.getActiveTimeline().saveToCompactionRequested(compactionInstant,
            TimelineMetadataUtils.serializeCompactionPlan(plan));
      } catch (IOException ioe) {
        throw new HoodieIOException("Exception scheduling compaction", ioe);
      }
      return Option.of(plan);
    }
    return Option.empty();
  }
}
