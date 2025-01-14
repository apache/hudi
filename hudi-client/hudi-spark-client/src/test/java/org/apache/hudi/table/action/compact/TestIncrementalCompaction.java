///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.hudi.table.action.compact;
//
//import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
//import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
//import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
//
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.Arguments;
//import org.junit.jupiter.params.provider.MethodSource;
//
//import java.util.Properties;
//import java.util.stream.Stream;
//
//public class TestIncrementalCompaction extends HoodieSparkClientTestHarness {
//
//  @ParameterizedTest
//  @MethodSource("testIncrCompactionWithFilter")
//  public void testPartitionsForIncrCompaction(CompactionStrategy strategy, Properties props) throws Exception {
//
//
//
//
//  }
//
//  public static Stream<Object> testIncrClusteringWithFilter() {
//    Properties none = new Properties();
//
//    return Stream.of(
//        Arguments.of(ClusteringPlanPartitionFilterMode.NONE, none),
//        Arguments.of(ClusteringPlanPartitionFilterMode.SELECTED_PARTITIONS, selectedPartitions),
//        Arguments.of(ClusteringPlanPartitionFilterMode.RECENT_DAYS, recentDay)
//    );
//  }
//
//
//}
