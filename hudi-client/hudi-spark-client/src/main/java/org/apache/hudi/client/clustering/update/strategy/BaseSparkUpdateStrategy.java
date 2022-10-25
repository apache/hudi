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
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.update.strategy.UpdateStrategy;

import java.util.List;
import java.util.Set;

/**
 * Spark base update strategy, write records to the file groups which are in clustering
 * need to check. Spark relate implementations should extend this base class.
 */
public abstract class BaseSparkUpdateStrategy<T extends HoodieRecordPayload<T>> extends UpdateStrategy<T, HoodieData<HoodieRecord<T>>> {

  public BaseSparkUpdateStrategy(HoodieEngineContext engineContext, HoodieTable table,
                                 Set<HoodieFileGroupId> fileGroupsInPendingClustering) {
    super(engineContext, table, fileGroupsInPendingClustering);
  }

  /**
   * Get records matched file group ids.
   * @param inputRecords the records to write, tagged with target file id
   * @return the records matched file group ids
   */
  protected List<HoodieFileGroupId> getGroupIdsWithUpdate(HoodieData<HoodieRecord<T>> inputRecords) {
    return inputRecords
            .filter(record -> record.getCurrentLocation() != null)
            .map(record -> new HoodieFileGroupId(record.getPartitionPath(), record.getCurrentLocation().getFileId())).distinct().collectAsList();
  }
}
