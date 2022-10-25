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

package org.apache.hudi.sink.cluster;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestSQL;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ITTestFlinkConsistentHashingClustering extends TestConsistentHashingClusteringBase {

  @Test
  public void testScheduleMergePlan() throws Exception {
    TableEnvironment tableEnv = setupTableEnv();
    prepareData(tableEnv);

    // Schedule clustering
    Configuration conf = getDefaultConfiguration();
    conf.setString(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key(), "1");
    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);
    Option<String> clusteringInstantOption = writeClient.scheduleClustering(Option.empty());
    Assertions.assertTrue(clusteringInstantOption.isPresent());

    // Validate clustering plan
    HoodieClusteringPlan clusteringPlan = getLatestClusteringPlan(writeClient);
    Assertions.assertEquals(1, clusteringPlan.getInputGroups().size());
    Assertions.assertEquals(4, clusteringPlan.getInputGroups().get(0).getSlices().size());

    // Upsert data. The new data should be immediately visible because of duplicate update strategy
    tableEnv.executeSql(TestSQL.UPDATE_INSERT_T1).await();
    TestData.checkWrittenData(tempFile, EXPECTED_AFTER_UPSERT, 0);
  }

  @Test
  public void testScheduleSplitPlan() throws Exception {
    TableEnvironment tableEnv = setupTableEnv();
    prepareData(tableEnv);

    // Schedule clustering
    Configuration conf = getDefaultConfiguration();
    conf.setString(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key(), "4");
    conf.setString(HoodieIndexConfig.BUCKET_INDEX_MAX_NUM_BUCKETS.key(), "8");
    // Manually set the split threshold to trigger split in the clustering
    conf.set(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE, 1);
    conf.setString(HoodieIndexConfig.BUCKET_SPLIT_THRESHOLD.key(), String.valueOf(1 / 1024.0 / 1024.0));
    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);
    Option<String> clusteringInstantOption = writeClient.scheduleClustering(Option.empty());
    Assertions.assertTrue(clusteringInstantOption.isPresent());

    // Validate clustering plan
    HoodieClusteringPlan clusteringPlan = getLatestClusteringPlan(writeClient);
    Assertions.assertEquals(4, clusteringPlan.getInputGroups().size());
    Assertions.assertEquals(1, clusteringPlan.getInputGroups().get(0).getSlices().size());
    Assertions.assertEquals(1, clusteringPlan.getInputGroups().get(1).getSlices().size());
    Assertions.assertEquals(1, clusteringPlan.getInputGroups().get(2).getSlices().size());
    Assertions.assertEquals(1, clusteringPlan.getInputGroups().get(3).getSlices().size());

    // Upsert data. The data should be immediately visible because of duplicate update strategy
    tableEnv.executeSql(TestSQL.UPDATE_INSERT_T1).await();
    TestData.checkWrittenData(tempFile, EXPECTED_AFTER_UPSERT, 0);
  }

  private HoodieClusteringPlan getLatestClusteringPlan(HoodieFlinkWriteClient writeClient) {
    HoodieFlinkTable<?> table = writeClient.getHoodieTable();
    table.getMetaClient().reloadActiveTimeline();
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption = ClusteringUtils.getClusteringPlan(
        table.getMetaClient(), table.getMetaClient().getActiveTimeline().filterPendingReplaceTimeline().lastInstant().get());
    return clusteringPlanOption.get().getRight();
  }
}
