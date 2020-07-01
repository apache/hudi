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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClusteringHelpers {

  public static void completeInflightClustering(HoodieTable<?> table, String clusteringCommitTime, HoodieCommitMetadata commitMetadata) {
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    try {
      activeTimeline.transitionClusteringInflightToComplete(
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, clusteringCommitTime),
          Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + clusteringCommitTime, e);
    }
  }
}
