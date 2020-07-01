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

package org.apache.hudi.table.action.clustering.updates;

import org.apache.hudi.avro.model.HoodieClusteringOperation;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieUpdateRejectException;
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
  public void apply(HoodieTableMetaClient client, WorkloadProfile workloadProfile) {
    List<Pair<HoodieInstant, HoodieClusteringPlan>> plans = ClusteringUtils.getAllPendingClusteringPlans(client);
    if (plans == null || plans.size() == 0) {
      return;
    }
    List<Pair<String, String>> partitionFileIdPairs = plans.stream().map(entry -> {
      HoodieClusteringPlan plan = entry.getValue();
      List<HoodieClusteringOperation> operations = plan.getOperations();
      List<Pair<String, String>> partitionFileIdPair =
              operations.stream()
                      .flatMap(operation -> operation.getBaseFilePaths().stream().map(filePath -> Pair.of(operation.getPartitionPath(), FSUtils.getFileId(filePath))))
                      .collect(Collectors.toList());
      return partitionFileIdPair;
    }).collect(Collectors.toList()).stream().flatMap(list -> list.stream()).collect(Collectors.toList());

    if (partitionFileIdPairs.size() == 0) {
      return;
    }

    Set<Map.Entry<String, WorkloadStat>> partitionStatEntries = workloadProfile.getPartitionPathStatMap().entrySet();
    for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
              partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        String partitionPath = partitionStat.getKey();
        String fileId = updateLocEntry.getKey();
        if (partitionFileIdPairs.contains(Pair.of(partitionPath, fileId))) {
          LOG.error("Not allowed to update the clustering files, partition: " + partitionPath + ", fileID " + fileId + ", please use other strategy.");
          throw new HoodieUpdateRejectException(
                  "Not allowed to update the clustering files during clustering, partition: "
                          + partitionPath + ", fileID " + fileId + ", please use other strategy.");
        }
      }
    }
  }
}
