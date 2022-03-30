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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;

public class SparkExecuteClusteringCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseSparkCommitActionExecutor<T> {

  private final HoodieClusteringPlan clusteringPlan;

  public SparkExecuteClusteringCommitActionExecutor(HoodieEngineContext context,
                                                    HoodieWriteConfig config, HoodieTable table,
                                                    String instantTime) {
    super(context, config, table, instantTime, WriteOperationType.CLUSTER);
    this.clusteringPlan = ClusteringUtils.getClusteringPlan(
        table.getMetaClient(), HoodieTimeline.getReplaceCommitRequestedInstant(instantTime))
        .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException(
            "Unable to read clustering plan for instant: " + instantTime));
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    return executeClustering(clusteringPlan);
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.REPLACE_COMMIT_ACTION;
  }
}
