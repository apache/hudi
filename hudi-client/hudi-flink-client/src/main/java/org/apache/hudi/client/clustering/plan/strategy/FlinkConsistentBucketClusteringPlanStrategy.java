/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.BaseConsistentHashingBucketClusteringPlanStrategy;

import java.util.List;

/**
 * Consistent hashing bucket index clustering plan for Flink engine.
 */
public class FlinkConsistentBucketClusteringPlanStrategy<T extends HoodieRecordPayload<T>>
    extends BaseConsistentHashingBucketClusteringPlanStrategy<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  public FlinkConsistentBucketClusteringPlanStrategy(
      HoodieTable table, HoodieEngineContext engineContext,
      HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected boolean isBucketClusteringMergeEnabled() {
    // Flink would not generate merge plan because it would cause multiple write sub tasks write to the same file group.
    return false;
  }

  @Override
  protected boolean isBucketClusteringSortEnabled() {
    // Flink would not generate sort clustering plans for buckets that are not involved in merge or split to avoid unnecessary clustering costs.
    return false;
  }
}
