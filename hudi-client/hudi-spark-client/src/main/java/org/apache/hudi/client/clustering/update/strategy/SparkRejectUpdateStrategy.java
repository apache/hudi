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
import org.apache.hudi.exception.HoodieClusteringUpdateException;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Update strategy based on following.
 * if some file group have update record, throw exception
 */
public class SparkRejectUpdateStrategy<T extends HoodieRecordPayload<T>> extends UpdateStrategy<T, JavaRDD<HoodieRecord<T>>> {
  private static final Logger LOG = LogManager.getLogger(SparkRejectUpdateStrategy.class);

  public SparkRejectUpdateStrategy(HoodieSparkEngineContext engineContext, HashSet<HoodieFileGroupId> fileGroupsInPendingClustering) {
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
    fileGroupIdsWithRecordUpdate.forEach(fileGroupIdWithRecordUpdate -> {
      if (fileGroupsInPendingClustering.contains(fileGroupIdWithRecordUpdate)) {
        String msg = String.format("Not allowed to update the clustering file group %s. "
                + "For pending clustering operations, we are not going to support update for now.",
            fileGroupIdWithRecordUpdate.toString());
        LOG.error(msg);
        throw new HoodieClusteringUpdateException(msg);
      }
    });
    return Pair.of(taggedRecordsRDD, Collections.emptySet());
  }

}
