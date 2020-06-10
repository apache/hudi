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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * Information about incoming records for upsert/insert obtained either via sampling or introspecting the data fully.
 * <p>
 * TODO(vc): Think about obtaining this directly from index.tagLocation
 */
public abstract class BaseWorkloadProfile<I> implements Serializable {

  /**
   * Input workload.
   */
  public final I taggedRecords;

  /**
   * Computed workload profile.
   */
  public final HashMap<String, WorkloadStat> partitionPathStatMap;

  /**
   * Global workloadStat.
   */
  public final WorkloadStat globalStat;

  public BaseWorkloadProfile(I taggedRecords) {
    this.taggedRecords = taggedRecords;
    this.partitionPathStatMap = new HashMap<>();
    this.globalStat = new WorkloadStat();
    buildProfile();
  }

  /**
   * Method help to build WorkloadProfile.
   */
  public abstract void buildProfile();

  public WorkloadStat getGlobalStat() {
    return globalStat;
  }

  public Set<String> getPartitionPaths() {
    return partitionPathStatMap.keySet();
  }

  public HashMap<String, WorkloadStat> getPartitionPathStatMap() {
    return partitionPathStatMap;
  }

  public WorkloadStat getWorkloadStat(String partitionPath) {
    return partitionPathStatMap.get(partitionPath);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkloadProfile {");
    sb.append("globalStat=").append(globalStat).append(", ");
    sb.append("partitionStat=").append(partitionPathStatMap);
    sb.append('}');
    return sb.toString();
  }
}
