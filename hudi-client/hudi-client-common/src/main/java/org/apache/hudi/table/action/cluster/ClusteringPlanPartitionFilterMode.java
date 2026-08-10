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

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Clustering partition filter mode
 */
@EnumDescription("Partition filter mode used in the creation of clustering plan.")
public enum ClusteringPlanPartitionFilterMode {

  @EnumFieldDescription("Do not filter partitions. The clustering plan will include all partitions that have clustering candidates.")
  NONE,

  @EnumFieldDescription("This filter assumes that your data is partitioned by date. The clustering plan will only include partitions "
      + "from K days ago to N days ago, where K >= N. K is determined by `hoodie.clustering.plan.strategy.daybased.lookback.partitions` "
      + "and N is determined by `hoodie.clustering.plan.strategy.daybased.skipfromlatest.partitions`.")
  RECENT_DAYS,

  @EnumFieldDescription("The clustering plan will include only partition paths with names that sort within the inclusive range "
      + "[`hoodie.clustering.plan.strategy.cluster.begin.partition`, `hoodie.clustering.plan.strategy.cluster.end.partition`].")
  SELECTED_PARTITIONS,

  @EnumFieldDescription("To determine the partitions in the clustering plan, the eligible partitions will be sorted in ascending order. "
      + "Each partition will have an index i in that list. The clustering plan will only contain partitions such that i mod 24 = H, "
      + "where H is the current hour of the day (from 0 to 23).")
  DAY_ROLLING
}
