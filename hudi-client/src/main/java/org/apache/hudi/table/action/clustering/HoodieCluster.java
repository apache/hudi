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

package org.apache.hudi.table.action.clustering;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

public interface HoodieCluster extends Serializable {

  HoodieClusteringPlan generateClusteringPlan(JavaSparkContext jsc, HoodieTable hoodieTable, HoodieWriteConfig config,
                                                String clusteringCommitTime, Set<HoodieFileGroupId> fgIdsInPendingClustering) throws IOException;

  JavaRDD<WriteStatus> clustering(JavaSparkContext jsc, HoodieClusteringPlan clusteringPlan, HoodieTable hoodieTable,
                                 HoodieWriteConfig config, String clusteringInstantTime) throws IOException;
}
