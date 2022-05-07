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

package org.apache.hudi.table.cluster.strategy;

import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class TestClusteringPlanStrategyConfigCompatibility {

  private static Stream<Arguments> configParams() {
    /**
     * (user specified class, converted class, filter mode)
     */
    Object[][] data = new Object[][] {
        {"org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy",
            "org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy",
            ClusteringPlanPartitionFilterMode.RECENT_DAYS},
        {"org.apache.hudi.client.clustering.plan.strategy.SparkSelectedPartitionsClusteringPlanStrategy",
            "org.apache.hudi.client.clustering.plan.strategy.SparkSizeBasedClusteringPlanStrategy",
            ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS},
        {"org.apache.hudi.client.clustering.plan.strategy.JavaRecentDaysClusteringPlanStrategy",
            "org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy",
            ClusteringPlanPartitionFilterMode.RECENT_DAYS}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest()
  @MethodSource("configParams")
  public void testCheckAndGetClusteringPlanStrategy(String oldClass, String newClass, ClusteringPlanPartitionFilterMode mode) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(oldClass)
            .build())
        .build();

    Assertions.assertEquals(newClass, ClusteringPlanStrategy.checkAndGetClusteringPlanStrategy(config));
    Assertions.assertEquals(mode, config.getClusteringPlanPartitionFilterMode());
  }
}
