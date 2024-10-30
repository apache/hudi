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

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;

public class HoodieJavaTableServiceClient<T> extends BaseHoodieTableServiceClient<List<HoodieRecord<T>>, List<WriteStatus>, List<WriteStatus>> {

  protected HoodieJavaTableServiceClient(HoodieEngineContext context,
                                         HoodieWriteConfig clientConfig,
                                         Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  @Override
  protected void validateClusteringCommit(HoodieWriteMetadata<List<WriteStatus>> clusteringMetadata, String clusteringCommitTime, HoodieTable table) {
    if (clusteringMetadata.getWriteStatuses().isEmpty()) {
      HoodieClusteringPlan clusteringPlan = ClusteringUtils.getClusteringPlan(
              table.getMetaClient(), ClusteringUtils.getInflightClusteringInstant(clusteringCommitTime, table.getActiveTimeline(), table.getInstantFactory()).get())
          .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException(
              "Unable to read clustering plan for instant: " + clusteringCommitTime));
      throw new HoodieClusteringException("Clustering plan produced 0 WriteStatus for " + clusteringCommitTime
          + " #groups: " + clusteringPlan.getInputGroups().size() + " expected at least "
          + clusteringPlan.getInputGroups().stream().mapToInt(HoodieClusteringGroup::getNumOutputFileGroups).sum()
          + " write statuses");
    }
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> convertToOutputMetadata(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return writeMetadata;
  }

  @Override
  protected HoodieData<WriteStatus> convertToWriteStatus(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return HoodieListData.eager(writeMetadata.getWriteStatuses());
  }

  @Override
  protected HoodieTable<?, List<HoodieRecord<T>>, ?, List<WriteStatus>> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
    return createTableAndValidate(config, HoodieJavaTable::create, skipValidation);
  }
}
