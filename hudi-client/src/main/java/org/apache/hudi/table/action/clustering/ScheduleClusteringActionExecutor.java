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

package org.apache.hudi.table.action.clustering;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieCommitMetadata.CLUSTERING_KEY;

public class ScheduleClusteringActionExecutor extends BaseActionExecutor<Option<HoodieClusteringPlan>> {

  private static final Logger LOG = LogManager.getLogger(ScheduleClusteringActionExecutor.class);

  private final Option<Map<String, String>> extraMetadata;

  public ScheduleClusteringActionExecutor(JavaSparkContext jsc,
                                          HoodieWriteConfig config,
                                          HoodieTable<?> table,
                                          String instantTime,
                                          Option<Map<String, String>> extraMetadata) {
    super(jsc, config, table, instantTime);
    this.extraMetadata = extraMetadata;
  }

  private Option<HoodieInstant> getLastClusteringInstant() {
    List<HoodieInstant> instants = table.getActiveTimeline().getCommitsTimeline().getReverseOrderedInstants().collect(Collectors.toList());
    for (HoodieInstant instant : instants) {
      try {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                table.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getMetadata(CLUSTERING_KEY) != null
                && Boolean.parseBoolean(commitMetadata.getMetadata(CLUSTERING_KEY))) {
          return Option.of(instant);
        }
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
    return Option.empty();

  }

  private HoodieClusteringPlan scheduleClustering() {
    LOG.info("Checking if clustering needs to be run on " + config.getBasePath());
    Option<HoodieInstant> last = getLastClusteringInstant();
    String deltaCommitsSinceTs = "0";
    if (last.isPresent()) {
      deltaCommitsSinceTs = last.get().getTimestamp();
    }

    int deltaCommitsSinceLastClustering = table.getActiveTimeline().getCommitsTimeline()
        .findInstantsAfter(deltaCommitsSinceTs, Integer.MAX_VALUE).countInstants();
    if (config.getInlineClusteringDeltaCommitMax() > deltaCommitsSinceLastClustering) {
      LOG.info("Not running clustering as only " + deltaCommitsSinceLastClustering
          + " delta commits was found since last clustering " + deltaCommitsSinceTs + ". Waiting for "
          + config.getInlineClusteringDeltaCommitMax());
      return new HoodieClusteringPlan();
    }

    HoodieCopyOnWriteTableCluster cluster = new HoodieCopyOnWriteTableCluster();
    try {
      return cluster.generateClusteringPlan(jsc, table, config, instantTime,
          ((SyncableFileSystemView) table.getSliceView()).getPendingClusteringOperations()
              .flatMap(instantTimeOpPair -> instantTimeOpPair.getValue().getBaseFilePaths().stream().map(s -> new HoodieFileGroupId(instantTimeOpPair.getValue().getPartitionPath(), s)))
              .collect(Collectors.toSet()));

    } catch (IOException e) {
      throw new HoodieCompactionException("Could not schedule clustering " + config.getBasePath(), e);
    }
  }

  @Override
  public Option<HoodieClusteringPlan> execute() {
    // if there are inflight writes, their instantTime must not be less than that of clustering instant time
    table.getActiveTimeline().getCommitsTimeline().filterPendingClusteringTimeline().firstInstant()
        .ifPresent(earliestInflight -> ValidationUtils.checkArgument(
            HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), HoodieTimeline.GREATER_THAN, instantTime),
            "Earliest write inflight instant time must be later than clustering time. Earliest :" + earliestInflight
                + ", Clustering scheduled at " + instantTime));

    // Committed and pending clustering instants should have strictly lower timestamps
    List<HoodieInstant> conflictingInstants = table.getActiveTimeline()
        .getCommitsAndCompactionTimeline().getInstants()
        .filter(instant -> HoodieTimeline.compareTimestamps(
            instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS, instantTime))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(conflictingInstants.isEmpty(),
        "Following instants have timestamps >= clusteringInstant (" + instantTime + ") Instants :"
            + conflictingInstants);

    HoodieClusteringPlan plan = scheduleClustering();
    if (plan != null && (plan.getOperations() != null) && (!plan.getOperations().isEmpty())) {
      extraMetadata.ifPresent(plan::setExtraMetadata);
      HoodieInstant clusteringInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, instantTime);
      try {
        table.getActiveTimeline().saveToClusteringRequested(clusteringInstant,
            TimelineMetadataUtils.serializeClusteringPlan(plan));
      } catch (IOException ioe) {
        throw new HoodieIOException("Exception scheduling clustering", ioe);
      }
      return Option.of(plan);
    }
    return Option.empty();
  }
}
