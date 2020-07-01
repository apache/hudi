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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.model.HoodieClusteringOperation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates all the needed information about a clustering and make a decision whether this clustering is effective
 * or not.
 */
public class ClusteringOperation implements Serializable {

  private List<String> baseFilePaths;
  private String partitionPath;
  private Map<String, Double> metrics;

  // Only for serialization/de-serialization
  @Deprecated
  public ClusteringOperation() {}

  public ClusteringOperation(List<String> dataFileNames, String partitionPath,
                             Map<String, Double> metrics) {
    this.baseFilePaths = new ArrayList<>(dataFileNames);
    this.partitionPath = partitionPath;
    this.metrics = metrics;
  }

  public List<String> getBaseFilePaths() {
    return baseFilePaths;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public Map<String, Double> getMetrics() {
    return metrics;
  }

  /**
   * Convert Avro generated Clustering operation to POJO for Spark RDD operation.
   * 
   * @param operation Hoodie Clustering Operation
   * @return
   */
  public static ClusteringOperation convertFromAvroRecordInstance(HoodieClusteringOperation operation) {
    ClusteringOperation op = new ClusteringOperation();
    op.baseFilePaths = new ArrayList<>(operation.getBaseFilePaths());
    op.partitionPath = operation.getPartitionPath();
    op.metrics = operation.getMetrics() == null ? new HashMap<>() : new HashMap<>(operation.getMetrics());
    return op;
  }

  @Override
  public String toString() {
    return "ClusteringOperation{baseFilePaths='" + baseFilePaths + '\'' + ", partitionPath="
        + partitionPath + '\'' + ", metrics=" + metrics + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusteringOperation operation = (ClusteringOperation) o;
    return Objects.equals(baseFilePaths, operation.baseFilePaths)
        && Objects.equals(partitionPath, operation.partitionPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseFilePaths, partitionPath);
  }
}
