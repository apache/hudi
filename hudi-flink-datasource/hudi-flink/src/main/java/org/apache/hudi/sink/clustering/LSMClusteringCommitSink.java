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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.flink.configuration.Configuration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LSMClusteringCommitSink extends ClusteringCommitSink {

  public LSMClusteringCommitSink(Configuration conf) {
    super(conf);
  }

  /**
   *   new HoodieFileGroupId(s.getPartitionPath(), s.getPath()))
   *   use file path as replaced file ID in clustering commit
   */
  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(
      HoodieClusteringPlan clusteringPlan,
      HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    Set<HoodieFileGroupId> newFilesWritten = writeMetadata.getWriteStats().get().stream()
        .map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getPath())).collect(Collectors.toSet());
    return ClusteringUtils.getFileGroupsFromClusteringPlan(clusteringPlan)
        .filter(fg -> !newFilesWritten.contains(fg))
        .collect(Collectors.groupingBy(HoodieFileGroupId::getPartitionPath, Collectors.mapping(HoodieFileGroupId::getFileId, Collectors.toList())));
  }
}
