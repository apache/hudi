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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieClusteringUpdateException;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Set;

/**
 * Update strategy based on following.
 * if some file groups have update record, throw exception
 */
@Slf4j
public class SparkRejectUpdateStrategy<T> extends BaseSparkUpdateStrategy<T> {

  public SparkRejectUpdateStrategy(HoodieEngineContext engineContext, HoodieTable table,
                                   Set<HoodieFileGroupId> fileGroupsInPendingClustering,
                                   Set<HoodieFileGroupId> fileGroupsToBeReplaced) {
    super(engineContext, table, fileGroupsInPendingClustering, fileGroupsToBeReplaced);
  }

  @Override
  public Pair<HoodieData<HoodieRecord<T>>, Set<HoodieFileGroupId>> handleUpdate(HoodieData<HoodieRecord<T>> taggedRecordsRDD) {
    Set<HoodieFileGroupId> allAffectedFileGroups = getGroupIdsWithUpdate(taggedRecordsRDD);
    // also treat replaced file groups as potential conflict targets
    allAffectedFileGroups.addAll(fileGroupsToBeReplaced);
    allAffectedFileGroups.forEach(affectedFileGroup -> {
      if (fileGroupsInPendingClustering.contains(affectedFileGroup)) {
        String msg = String.format("Not allowed to update the clustering file group %s. "
                + "For pending clustering operations, we are not going to support update for now.",
            affectedFileGroup.toString());
        log.error(msg);
        throw new HoodieClusteringUpdateException(msg);
      }
    });
    return Pair.of(taggedRecordsRDD, Collections.emptySet());
  }

}
