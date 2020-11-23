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

package org.apache.hudi.table.action.clustering.update;

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieClusteringUpdateException;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RejectUpdateStrategy implements UpdateStrategy {
  private static final Logger LOG = LogManager.getLogger(RejectUpdateStrategy.class);

  @Override
  public void apply(List<Pair<HoodieFileGroupId, HoodieInstant>> fileGroupsInPendingClustering, WorkloadProfile workloadProfile) {
    Set<Pair<String, String>> partitionPathAndFileIds = fileGroupsInPendingClustering.stream()
        .map(entry -> Pair.of(entry.getLeft().getPartitionPath(), entry.getLeft().getFileId())).collect(Collectors.toSet());
    if (partitionPathAndFileIds.size() == 0) {
      return;
    }

    Set<Map.Entry<String, WorkloadStat>> partitionStatEntries = workloadProfile.getPartitionPathStatMap().entrySet();
    for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
              partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        String partitionPath = partitionStat.getKey();
        String fileId = updateLocEntry.getKey();
        if (partitionPathAndFileIds.contains(Pair.of(partitionPath, fileId))) {
          String msg = String.format("Not allowed to update the clustering files partition: %s fileID: %s. "
              + "For pending clustering operations, we are not going to support update for now.", partitionPath, fileId);
          LOG.error(msg);
          throw new HoodieClusteringUpdateException(msg);
        }
      }
    }
  }
}
