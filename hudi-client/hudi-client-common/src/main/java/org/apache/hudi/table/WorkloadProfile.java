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

package org.apache.hudi.table;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Information about incoming records for upsert/insert obtained either via sampling or introspecting the data fully.
 * <p>
 * TODO(vc): Think about obtaining this directly from index.tagLocation
 */
public class WorkloadProfile implements Serializable {

  /**
   * Computed workload stats.
   */
  protected final Map<String, WorkloadStat> inputPartitionPathStatMap;

  /**
   * Execution/Output workload stats
   */
  protected final Map<String, WorkloadStat> outputPartitionPathStatMap;

  /**
   * Global workloadStat.
   */
  protected final WorkloadStat globalStat;

  /**
   * Write operation type.
   */
  private WriteOperationType operationType;

  private final boolean hasOutputWorkLoadStats;

  public WorkloadProfile(Pair<Map<String, WorkloadStat>, WorkloadStat> profile) {
    this(profile, false);
  }

  public WorkloadProfile(Pair<Map<String, WorkloadStat>, WorkloadStat> profile, boolean hasOutputWorkLoadStats) {
    this.inputPartitionPathStatMap = profile.getLeft();
    this.globalStat = profile.getRight();
    this.outputPartitionPathStatMap = new HashMap<>();
    this.hasOutputWorkLoadStats = hasOutputWorkLoadStats;
  }

  public WorkloadProfile(Pair<Map<String, WorkloadStat>, WorkloadStat> profile, WriteOperationType operationType, boolean hasOutputWorkLoadStats) {
    this(profile, hasOutputWorkLoadStats);
    this.operationType = operationType;
  }

  public WorkloadStat getGlobalStat() {
    return globalStat;
  }

  public Set<String> getPartitionPaths() {
    return inputPartitionPathStatMap.keySet();
  }

  public Set<String> getOutputPartitionPaths() {
    return hasOutputWorkLoadStats ? outputPartitionPathStatMap.keySet() : inputPartitionPathStatMap.keySet();
  }

  public Map<String, WorkloadStat> getInputPartitionPathStatMap() {
    return inputPartitionPathStatMap;
  }

  public Map<String, WorkloadStat> getOutputPartitionPathStatMap() {
    return outputPartitionPathStatMap;
  }

  public boolean hasOutputWorkLoadStats() {
    return hasOutputWorkLoadStats;
  }

  public void updateOutputPartitionPathStatMap(String partitionPath, WorkloadStat workloadStat) {
    if (hasOutputWorkLoadStats) {
      outputPartitionPathStatMap.put(partitionPath, workloadStat);
    }
  }

  public WorkloadStat getWorkloadStat(String partitionPath) {
    return inputPartitionPathStatMap.get(partitionPath);
  }

  public WorkloadStat getOutputWorkloadStat(String partitionPath) {
    return hasOutputWorkLoadStats ? outputPartitionPathStatMap.get(partitionPath) : inputPartitionPathStatMap.get(partitionPath);
  }

  public WriteOperationType getOperationType() {
    return operationType;
  }

  @Override
  public String toString() {
    return "WorkloadProfile {" + "globalStat=" + globalStat + ", "
        + "InputPartitionStat=" + inputPartitionPathStatMap + ", "
        + "OutputPartitionStat=" + outputPartitionPathStatMap + ", "
        + "operationType=" + operationType
        + '}';
  }
}
