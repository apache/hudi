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

import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Set;

public class ClusteringPartitionEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private String partitionPath;

  private Pair<Set<String>, Set<String>> missingAndCompletedInstants;

  public ClusteringPartitionEvent() {
  }

  public ClusteringPartitionEvent(String partitionPath, Pair<Set<String>, Set<String>> missingAndCompletedInstants) {
    this.partitionPath = partitionPath;
    this.missingAndCompletedInstants = missingAndCompletedInstants;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public Pair<Set<String>, Set<String>> getMissingAndCompletedInstants() {
    return missingAndCompletedInstants;
  }

  public void setMissingAndCompletedInstants(Pair<Set<String>, Set<String>> missingAndCompletedInstants) {
    this.missingAndCompletedInstants = missingAndCompletedInstants;
  }
}
