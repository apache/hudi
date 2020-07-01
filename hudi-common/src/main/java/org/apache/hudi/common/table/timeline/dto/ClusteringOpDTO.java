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

package org.apache.hudi.common.table.timeline.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The data transfer object of clustering.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusteringOpDTO {
  @JsonProperty("clusteringInstant")
  String clusteringInstantTime;

  @JsonProperty("dataFiles")
  private List<String> dataFilePaths;

  @JsonProperty("partition")
  private String partitionPath;

  @JsonProperty("metrics")
  private Map<String, Double> metrics;

  public static ClusteringOpDTO fromClusteringOperation(String clusteringInstantTime, ClusteringOperation op) {
    ClusteringOpDTO dto = new ClusteringOpDTO();
    dto.clusteringInstantTime = clusteringInstantTime;
    dto.dataFilePaths = new ArrayList<>(op.getBaseFilePaths());
    dto.partitionPath = op.getPartitionPath();
    dto.metrics = op.getMetrics() == null ? new HashMap<>() : new HashMap<>(op.getMetrics());
    return dto;
  }

  public static Pair<String, ClusteringOperation> toClusteringOperation(ClusteringOpDTO dto) {
    return Pair.of(dto.clusteringInstantTime,
        new ClusteringOperation(dto.dataFilePaths, dto.partitionPath, dto.metrics));
  }
}
