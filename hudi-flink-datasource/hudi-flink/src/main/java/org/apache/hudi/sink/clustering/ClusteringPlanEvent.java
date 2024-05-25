/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.clustering;

import org.apache.hudi.common.model.ClusteringGroupInfo;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a cluster command from the clustering plan task {@link ClusteringPlanSourceFunction}.
 */
public class ClusteringPlanEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private String clusteringInstantTime;

  private ClusteringGroupInfo clusteringGroupInfo;

  private Map<String, String> strategyParams;

  public ClusteringPlanEvent() {
  }

  public ClusteringPlanEvent(
      String instantTime,
      ClusteringGroupInfo clusteringGroupInfo,
      Map<String, String> strategyParams) {
    this.clusteringInstantTime = instantTime;
    this.clusteringGroupInfo = clusteringGroupInfo;
    this.strategyParams = strategyParams;
  }

  public void setClusteringInstantTime(String clusteringInstantTime) {
    this.clusteringInstantTime = clusteringInstantTime;
  }

  public void setClusteringGroupInfo(ClusteringGroupInfo clusteringGroupInfo) {
    this.clusteringGroupInfo = clusteringGroupInfo;
  }

  public void setStrategyParams(Map<String, String> strategyParams) {
    this.strategyParams = strategyParams;
  }

  public String getClusteringInstantTime() {
    return clusteringInstantTime;
  }

  public ClusteringGroupInfo getClusteringGroupInfo() {
    return clusteringGroupInfo;
  }

  public Map<String, String> getStrategyParams() {
    return strategyParams;
  }
}
