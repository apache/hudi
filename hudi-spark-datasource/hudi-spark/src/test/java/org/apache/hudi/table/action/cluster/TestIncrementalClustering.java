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
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.config.HoodieClusteringConfig.DAYBASED_LOOKBACK_PARTITIONS;
import static org.apache.hudi.config.HoodieClusteringConfig.PARTITION_FILTER_BEGIN_PARTITION;
import static org.apache.hudi.config.HoodieClusteringConfig.PARTITION_FILTER_END_PARTITION;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_PARTITION_FILTER_MODE;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestIncrementalClustering extends SparkClientFunctionalTestHarness {

  private static final String TODAY;
  private static final String YESTERDAY;
  private static final String TOMORROW;

  static {
    LocalDate today = LocalDate.now();
    LocalDate yesterday = today.minusDays(1);
    LocalDate tomorrow = today.plusDays(1);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    TODAY = today.format(formatter);
    YESTERDAY = yesterday.format(formatter);
    TOMORROW = tomorrow.format(formatter);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPartitionsForIncrClusteringWithMaxGroupLimit(boolean enableIncrTableService) throws Exception {
    int maxClusteringGroup = 1;
    HoodieWriteConfig writeConfig = buildWriteConfig(enableIncrTableService, new Properties(), maxClusteringGroup);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    String[] partitions = {YESTERDAY, TODAY};
    // write twice, make sure there are multi-slices in each partitions
    prepareBasicData(writeConfig, partitions);
    prepareBasicData(writeConfig, partitions);
    String clusteringInstantTime = doClustering(writeConfig);

    HoodieClusteringPlan clusteringPlan = ClusteringTestUtils.getClusteringPlan(metaClient, clusteringInstantTime);
    if (enableIncrTableService) {
      // Since setting maxClusteringGroup 1, and there are 2 slices under each partition,
      // all files in each partition have not been completely processed, that is, they are all missing partitions.
      assertEquals(2, clusteringPlan.getMissingSchedulePartitions().size());
      assertTrue(clusteringPlan.getMissingSchedulePartitions().contains(YESTERDAY));
      assertTrue(clusteringPlan.getMissingSchedulePartitions().contains(TODAY));

      String[] partitions2 = {TOMORROW};
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
      assertNull(clusteringPlan.getMissingSchedulePartitions());
    }
  }

  @ParameterizedTest
  @MethodSource("testIncrClusteringWithFilter")
  public void testPartitionsForIncrClusteringWithFilter(ClusteringPlanPartitionFilterMode mode,
                                                        Properties props) throws Exception {
    HoodieWriteConfig writeConfig = buildWriteConfig(true, props, 100);
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.COPY_ON_WRITE);
    String[] partitions = {YESTERDAY, TODAY};
    // write twice, make sure there are multi-slices in each partitions
    prepareBasicData(writeConfig, partitions);
    String clusteringInstantTime = doClustering(writeConfig);
    HoodieClusteringPlan clusteringPlan = ClusteringTestUtils.getClusteringPlan(metaClient, clusteringInstantTime);

    switch (mode) {
      case NONE: {
        // For partitions filtered out by the regex expression, they will not be recorded in the missingPartitions
        assertEquals(0, clusteringPlan.getMissingSchedulePartitions().size());
        break;
      }
      case SELECTED_PARTITIONS: {
        Set<String> affectedPartitions = getAffectedPartition(clusteringPlan);
        assertEquals(1, affectedPartitions.size());
        assertTrue(affectedPartitions.contains(YESTERDAY));
        assertEquals(0, clusteringPlan.getMissingSchedulePartitions().size());
        break;
      }
      case RECENT_DAYS: {
        assertEquals(1, clusteringPlan.getMissingSchedulePartitions().size());
        assertTrue(clusteringPlan.getMissingSchedulePartitions().contains(TODAY));
        // do another TOMORROW ingestion and clustering
        String[] partitions2 = {TOMORROW};
        HoodieWriteConfig hoodieWriteConfig2 = buildWriteConfig(true, props, 100);
        prepareBasicData(hoodieWriteConfig2, partitions2);
        String clusteringInstantTime2 = doClustering(hoodieWriteConfig2);
        HoodieClusteringPlan clusteringPlan2 = ClusteringTestUtils.getClusteringPlan(metaClient, clusteringInstantTime2);
        assertEquals(1, clusteringPlan2.getMissingSchedulePartitions().size());
        assertTrue(clusteringPlan2.getMissingSchedulePartitions().contains(TOMORROW));
        break;
      }
      default: {
        throw new HoodieException("Un-support mode" + mode);
      }
    }
  }

  private Set<String> getAffectedPartition(HoodieClusteringPlan clusteringPlan) {
    return clusteringPlan.getInputGroups().stream().flatMap(hoodieClusteringGroup -> {
      return hoodieClusteringGroup.getSlices().stream();
    }).map(HoodieSliceInfo::getPartitionPath).collect(Collectors.toSet());
  }

  public static Stream<Object> testIncrClusteringWithFilter() {
    Properties none = new Properties();
    none.put(PLAN_PARTITION_FILTER_MODE, ClusteringPlanPartitionFilterMode.NONE);
    none.put("hoodie.clustering.plan.strategy.partition.regex.pattern", TODAY + ".*");

    Properties selectedPartitions = new Properties();
    selectedPartitions.put(PLAN_PARTITION_FILTER_MODE, ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS.name());
    selectedPartitions.put(PARTITION_FILTER_BEGIN_PARTITION.key(), YESTERDAY);
    selectedPartitions.put(PARTITION_FILTER_END_PARTITION.key(), YESTERDAY);

    Properties recentDay = new Properties();
    recentDay.put(PLAN_PARTITION_FILTER_MODE, ClusteringPlanPartitionFilterMode.RECENT_DAYS);
    recentDay.put(PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST.key(), 1);
    recentDay.put(DAYBASED_LOOKBACK_PARTITIONS.key(), 1);

    return Stream.of(
        Arguments.of(ClusteringPlanPartitionFilterMode.NONE, none),
        Arguments.of(ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS, selectedPartitions),
        Arguments.of(ClusteringPlanPartitionFilterMode.RECENT_DAYS, recentDay)
    );
  }

  private HoodieWriteConfig buildWriteConfig(boolean enableIncrTableService, Properties properties, int maxClusteringGroup) {
    properties.put("hoodie.datasource.write.row.writer.enable", String.valueOf(false));
    properties.put("hoodie.parquet.small.file.limit", String.valueOf(-1));
    return getConfigBuilder()
        .withIncrementalTableServiceEnabled(enableIncrTableService)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringMaxNumGroups(maxClusteringGroup)
            .withClusteringMaxBytesInGroup(maxClusteringGroup)
            .fromProperties(properties)
            .build())
        .withProperties(properties)
        .build();
  }

  private void prepareBasicData(HoodieWriteConfig writeConfig, String[] partitions) throws IOException {
    SparkRDDWriteClient client = getHoodieWriteClient(writeConfig);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(partitions);
    for (int i = 0; i < partitions.length; i++) {
      String instantTime = client.startCommit();
      JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(jsc().parallelize(dataGen.generateInsertsForPartition(instantTime, 10, partitions[i]), 1), instantTime);
      client.commit(instantTime, writeStatusJavaRDD, Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
    }
    client.close();
  }

  private String doClustering(HoodieWriteConfig writeConfig) throws IOException {
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      return ClusteringTestUtils.runClustering(client, false, true);
    }
  }
}
