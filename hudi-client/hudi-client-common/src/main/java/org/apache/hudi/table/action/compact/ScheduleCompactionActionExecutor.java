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
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ScheduleCompactionActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieCompactionPlan>> {

  private static final Logger LOG = LogManager.getLogger(ScheduleCompactionActionExecutor.class);

  private final Option<Map<String, String>> extraMetadata;
  private final HoodieCompactor compactor;

  public ScheduleCompactionActionExecutor(HoodieEngineContext context,
                                          HoodieWriteConfig config,
                                          HoodieTable<T, I, K, O> table,
                                          String instantTime,
                                          Option<Map<String, String>> extraMetadata,
                                          HoodieCompactor compactor) {
    super(context, config, table, instantTime);
    this.extraMetadata = extraMetadata;
    this.compactor = compactor;
  }

  @Override
  public Option<HoodieCompactionPlan> execute() {
    if (!config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()
        && !config.getFailedWritesCleanPolicy().isLazy()) {
      // TODO(yihua): this validation is removed for Java client used by kafka-connect.  Need to revisit this.
      if (config.getEngineType() != EngineType.JAVA) {
        // if there are inflight writes, their instantTime must not be less than that of compaction instant time
        table.getActiveTimeline().getCommitsTimeline().filterPendingExcludingCompaction().firstInstant()
            .ifPresent(earliestInflight -> ValidationUtils.checkArgument(
                HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), HoodieTimeline.GREATER_THAN, instantTime),
                "Earliest write inflight instant time must be later than compaction time. Earliest :" + earliestInflight
                    + ", Compaction scheduled at " + instantTime));
      }
      // Committed and pending compaction instants should have strictly lower timestamps
      List<HoodieInstant> conflictingInstants = table.getActiveTimeline()
          .getWriteTimeline().filterCompletedAndCompactionInstants().getInstants()
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

  private HoodieCompactionPlan scheduleCompaction() {
    LOG.info("Checking if compaction needs to be run on " + config.getBasePath());
    // judge if we need to compact according to num delta commits and time elapsed
    boolean compactable = needCompact(config.getInlineCompactTriggerStrategy());
    if (compactable) {
      LOG.info("Generating compaction plan for merge on read table " + config.getBasePath());
      try {
        SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();
        Set<HoodieFileGroupId> fgInPendingCompactionAndClustering = fileSystemView.getPendingCompactionOperations()
            .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
            .collect(Collectors.toSet());
        // exclude files in pending clustering from compaction.
        fgInPendingCompactionAndClustering.addAll(fileSystemView.getFileGroupsInPendingClustering().map(Pair::getLeft).collect(Collectors.toSet()));
        return compactor.generateCompactionPlan(context, table, config, instantTime, fgInPendingCompactionAndClustering);
      } catch (IOException e) {
        throw new HoodieCompactionException("Could not schedule compaction " + config.getBasePath(), e);
      }
    }

    return new HoodieCompactionPlan();
  }

  private Pair<Integer, String> getLatestDeltaCommitInfo() {
    Option<HoodieInstant> lastCompaction = table.getActiveTimeline().getCommitTimeline()
        .filterCompletedInstants().lastInstant();
    HoodieTimeline deltaCommits = table.getActiveTimeline().getDeltaCommitTimeline();

    String latestInstantTs;
    final int deltaCommitsSinceLastCompaction;
    if (lastCompaction.isPresent()) {
      latestInstantTs = lastCompaction.get().getTimestamp();
      deltaCommitsSinceLastCompaction = deltaCommits.findInstantsAfter(latestInstantTs, Integer.MAX_VALUE).countInstants();
    } else {
      latestInstantTs = deltaCommits.firstInstant().get().getTimestamp();
      deltaCommitsSinceLastCompaction = deltaCommits.findInstantsAfterOrEquals(latestInstantTs, Integer.MAX_VALUE).countInstants();
    }
    return Pair.of(deltaCommitsSinceLastCompaction, latestInstantTs);
  }

  private boolean needCompact(CompactionTriggerStrategy compactionTriggerStrategy) {
    boolean compactable;
    // get deltaCommitsSinceLastCompaction and lastCompactionTs
    Pair<Integer, String> latestDeltaCommitInfo = getLatestDeltaCommitInfo();
    int inlineCompactDeltaCommitMax = config.getInlineCompactDeltaCommitMax();
    int inlineCompactDeltaSecondsMax = config.getInlineCompactDeltaSecondsMax();
    switch (compactionTriggerStrategy) {
      case NUM_COMMITS:
        compactable = inlineCompactDeltaCommitMax <= latestDeltaCommitInfo.getLeft();
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s, trigger compaction scheduler.", inlineCompactDeltaCommitMax));
        }
        break;
      case TIME_ELAPSED:
        compactable = inlineCompactDeltaSecondsMax <= parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
        if (compactable) {
          LOG.info(String.format("The elapsed time >=%ss, trigger compaction scheduler.", inlineCompactDeltaSecondsMax));
        }
        break;
      case NUM_OR_TIME:
        compactable = inlineCompactDeltaCommitMax <= latestDeltaCommitInfo.getLeft()
            || inlineCompactDeltaSecondsMax <= parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s or elapsed_time >=%ss, trigger compaction scheduler.", inlineCompactDeltaCommitMax,
              inlineCompactDeltaSecondsMax));
        }
        break;
      case NUM_AND_TIME:
        compactable = inlineCompactDeltaCommitMax <= latestDeltaCommitInfo.getLeft()
            && inlineCompactDeltaSecondsMax <= parsedToSeconds(instantTime) - parsedToSeconds(latestDeltaCommitInfo.getRight());
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s and elapsed_time >=%ss, trigger compaction scheduler.", inlineCompactDeltaCommitMax,
              inlineCompactDeltaSecondsMax));
        }
        break;
      default:
        throw new HoodieCompactionException("Unsupported compaction trigger strategy: " + config.getInlineCompactTriggerStrategy());
    }
    return compactable;
  }

  private Long parsedToSeconds(String time) {
    long timestamp;
    try {
      timestamp = HoodieActiveTimeline.parseDateFromInstantTime(time).getTime() / 1000;
    } catch (ParseException e) {
      throw new HoodieCompactionException(e.getMessage(), e);
    }
    return timestamp;
  }
}
