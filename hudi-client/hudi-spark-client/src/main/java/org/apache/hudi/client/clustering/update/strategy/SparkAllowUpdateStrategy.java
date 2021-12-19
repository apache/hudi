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

package org.apache.hudi.client.clustering.update.strategy;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;

import org.apache.spark.api.java.JavaRDD;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Allow ingestion commits during clustering job.
 */
public class SparkAllowUpdateStrategy<T extends HoodieRecordPayload<T>> extends UpdateStrategy<T, JavaRDD<HoodieRecord<T>>> {

  public SparkAllowUpdateStrategy(
      HoodieSparkEngineContext engineContext, HashSet<HoodieFileGroupId> fileGroupsInPendingClustering) {
    super(engineContext, fileGroupsInPendingClustering);
  }

  private List<HoodieFileGroupId> getGroupIdsWithUpdate(JavaRDD<HoodieRecord<T>> inputRecords) {
    List<HoodieFileGroupId> fileGroupIdsWithUpdates = inputRecords
        .filter(record -> record.getCurrentLocation() != null)
        .map(record -> new HoodieFileGroupId(record.getPartitionPath(), record.getCurrentLocation().getFileId())).distinct().collect();
    return fileGroupIdsWithUpdates;
  }

  @Override
  public Pair<JavaRDD<HoodieRecord<T>>, Set<HoodieFileGroupId>> handleUpdate(JavaRDD<HoodieRecord<T>> taggedRecordsRDD) {
    List<HoodieFileGroupId> fileGroupIdsWithRecordUpdate = getGroupIdsWithUpdate(taggedRecordsRDD);
    Set<HoodieFileGroupId> fileGroupIdsWithUpdatesAndPendingClustering = fileGroupIdsWithRecordUpdate.stream()
        .filter(f -> fileGroupsInPendingClustering.contains(f))
        .collect(Collectors.toSet());
    return Pair.of(taggedRecordsRDD, fileGroupIdsWithUpdatesAndPendingClustering);
  }
}
