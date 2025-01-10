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
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClusteringPlanActionExecutor extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPartitionsForIncrClusteringWithMaxGroupLimit(boolean enableIncrTableService) throws Exception {
    int maxClusteringGroup = 1;
    HoodieWriteConfig writeConfig = buildWriteConfig(enableIncrTableService, new Properties(), maxClusteringGroup);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    String[] partitions = {"2025/01/10", "2025/01/11"};
    // write twice, make sure there are multi-slices in each partitions
    prepareBasicData(writeConfig, partitions);
    prepareBasicData(writeConfig, partitions);
    String clusteringInstantTime = doClustering(writeConfig);

    HoodieClusteringPlan clusteringPlan = ClusteringTestUtils.getClusteringPlan(metaClient, clusteringInstantTime);
    if (enableIncrTableService) {
      // Since setting maxClusteringGroup 1, and there are 2 slices under each partition,
      // all files in each partition have not been completely processed, that is, they are all missing partitions.
      assertEquals(2, clusteringPlan.getMissingSchedulePartitions().size());
      assertTrue(clusteringPlan.getMissingSchedulePartitions().contains("2025/01/10"));
      assertTrue(clusteringPlan.getMissingSchedulePartitions().contains("2025/01/11"));

      String[] partitions2 = {"2025/01/12"};
      prepareBasicData(buildWriteConfig(true, new Properties(), maxClusteringGroup), partitions2);
      HoodieSparkTable table = HoodieSparkTable.create(writeConfig, context());
      ClusteringPlanActionExecutor executor =
          new ClusteringPlanActionExecutor<>(context(), writeConfig, table, "9999999", Option.empty());

      List<String> incrementalPartitions = executor.getPartitions(ReflectionUtils.loadClass(
          ClusteringPlanStrategy.checkAndGetClusteringPlanStrategy(writeConfig),
          new Class<?>[] {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class}, table, context(), writeConfig), TableServiceType.CLUSTER);
      // fetch 2 missing partitions from last completed clustering plan as incremental partitions
      // get 1 partitions from new commit as incremental partitions
      assertEquals(3, incrementalPartitions.size());
    } else {
      assertEquals(0, clusteringPlan.getMissingSchedulePartitions().size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPartitionsForIncrClusteringWithRegexFilter(boolean enableIncrTableService) throws Exception {
    Properties props = new Properties();
    props.setProperty("hoodie.clustering.plan.strategy.partition.regex.pattern", "20250111.*");
    HoodieWriteConfig writeConfig = buildWriteConfig(enableIncrTableService, props, 100);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    String[] partitions = {"20250110", "20250111"};
    // write twice, make sure there are multi-slices in each partitions
    prepareBasicData(writeConfig, partitions);
    String clusteringInstantTime = doClustering(writeConfig);
    HoodieClusteringPlan clusteringPlan = ClusteringTestUtils.getClusteringPlan(metaClient, clusteringInstantTime);

    // For partitions filtered out by the regular expression, they will not be recorded in the missingPartitions
    assertEquals(0, clusteringPlan.getMissingSchedulePartitions().size());
  }

  private HoodieWriteConfig buildWriteConfig(boolean enableIncrTableService, Properties properties, int maxClusteringGroup) {
    properties.put("hoodie.datasource.write.row.writer.enable", String.valueOf(false));
    properties.put("hoodie.parquet.small.file.limit", String.valueOf(-1));
    return getConfigBuilder(true)
        .withIncrementalTableServiceEnable(enableIncrTableService)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringMaxNumGroups(maxClusteringGroup)
            .withClusteringMaxBytesInGroup(maxClusteringGroup)
            .build())
        .withProperties(properties)
        .build();
  }

  private void prepareBasicData(HoodieWriteConfig writeConfig, String[] partitions) throws IOException {
    SparkRDDWriteClient client = getHoodieWriteClient(writeConfig);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(partitions);
    for (int i = 0; i < partitions.length; i++) {
      String instantTime = client.createNewInstantTime();
      client.startCommitWithTime(instantTime);
      client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime, 10, partitions[i]), 1), instantTime);
    }
    client.close();
  }

  private String doClustering(HoodieWriteConfig writeConfig) throws IOException {
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      return ClusteringTestUtils.runClustering(client, false, true);
    }
  }
}
