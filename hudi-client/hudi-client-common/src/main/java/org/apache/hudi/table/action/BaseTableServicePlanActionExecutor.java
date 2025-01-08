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

package org.apache.hudi.table.action;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;

public abstract class BaseTableServicePlanActionExecutor<T, I, K, O, R> extends BaseActionExecutor<T, I, K, O, R> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseTableServicePlanActionExecutor.class);

  public BaseTableServicePlanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config,
                                            HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime);
  }

  /**
   * Get partitions, if strategy implement `IncrementalPartitionAwareStrategy` then return incremental partitions,
   * otherwise return all partitions of the table
   * @param strategy
   * @return
   */
  public List<String> getPartitions(Object strategy, TableServiceType type) {
    if (strategy instanceof IncrementalPartitionAwareStrategy) {
      try {
        // get incremental partitions.
        Set<String> incrementalPartitions = getIncrementalPartitions(type);
        if (!incrementalPartitions.isEmpty()) {
          return new ArrayList<>(incrementalPartitions);
        }
      } catch (Exception ex) {
        LOG.warn("Failed to get incremental partitions", ex);
      }
    }

    // fall back to get all partitions
    return FSUtils.getAllPartitionPaths(context, table.getMetaClient().getStorage(),
        config.getMetadataConfig(), table.getMetaClient().getBasePath());
  }

  private Set<String> getIncrementalPartitions(TableServiceType type) {
    Pair<Option<HoodieInstant>, List<String>> missingPair = fetchMissingPartitions(type);
    if (!missingPair.getLeft().isPresent()) {
      // Last complete table service commit maybe archived.
      return Collections.emptySet();
    }

    List<String> missingPartitions = missingPair.getRight();

    String leftBoundary = missingPair.getLeft().get().requestedTime();
    String rightBoundary = instantTime;

    // compute [leftBoundary, rightBoundary) as time window
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    Set<String> partitionsInCommitMeta = table.getActiveTimeline().filterCompletedInstants().getCommitsTimeline().getInstants().stream().flatMap(instant -> {
      if (!instant.getAction().equals(CLUSTERING_ACTION)) {
        String completionTime = instant.getCompletionTime();
        if (completionTime.compareTo(leftBoundary) <= 0 && completionTime.compareTo(rightBoundary) > 0) {
          try {
            HoodieCommitMetadata metadata = TimelineUtils.getCommitMetadata(instant, activeTimeline);
            return metadata.getWriteStats().stream().map(HoodieWriteStat::getPartitionPath);
          } catch (IOException e) {
            throw new HoodieIOException("Failed to get commit meta " + instant, e);
          }
        }
      }
      return Stream.empty();
    }).collect(Collectors.toSet());

    partitionsInCommitMeta.addAll(missingPartitions);
    return partitionsInCommitMeta;
  }

  private Pair<Option<HoodieInstant>, List<String>> fetchMissingPartitions(TableServiceType tableServiceType) {
    Option<HoodieInstant> instant = Option.empty();
    List<String> missingPartitions = new ArrayList<>();

    switch (tableServiceType) {
      case COMPACT:
      case LOG_COMPACT: {
        Option<HoodieInstant> lastCompactionCommitInstant = table.getActiveTimeline()
            .filterCompletedInstants().getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION)).lastInstant();
        if (lastCompactionCommitInstant.isPresent()) {
          instant = lastCompactionCommitInstant;
          String action = tableServiceType.equals(TableServiceType.COMPACT) ? HoodieTimeline.COMPACTION_ACTION : HoodieTimeline.LOG_COMPACTION_ACTION;
          HoodieInstant compactionPlanInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, action,
              instant.get().requestedTime(), InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
          Option<byte[]> details = table.getMetaClient().getActiveTimeline().readCompactionPlanAsBytes(compactionPlanInstant);
          HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(table.getMetaClient(), details);
          if (compactionPlan.getMissingSchedulePartitions() != null) {
            missingPartitions = compactionPlan.getMissingSchedulePartitions();
          }
        }
        break;
      }
      case CLUSTER: {
        Option<HoodieInstant> lastClusteringInstant = table.getActiveTimeline().filterCompletedInstants().getLastClusteringInstant();
        if (lastClusteringInstant.isPresent()) {
          instant = lastClusteringInstant;
          Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlan = ClusteringUtils.getClusteringPlan(table.getMetaClient(), lastClusteringInstant.get());
          if (clusteringPlan.isPresent() && clusteringPlan.get().getRight().getMissingSchedulePartitions() != null) {
            missingPartitions = clusteringPlan.get().getRight().getMissingSchedulePartitions();
          }
        }
        break;
      }
      default:
        throw new HoodieException("Un-supported incremental table service " + tableServiceType);
    }

    return Pair.of(instant, missingPartitions);
  }

}
