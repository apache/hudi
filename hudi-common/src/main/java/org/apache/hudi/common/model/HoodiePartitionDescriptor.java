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

import java.util.List;
import java.util.Objects;

public class HoodiePartitionDescriptor {
  // Relative partition path within the dataset
  private final String partitionPath;

  // List of values for corresponding partition columns
  private final List<Object> partitionColumnValues;

  public HoodiePartitionDescriptor(String partitionPath, List<Object> partitionColumnValues) {
    this.partitionPath = partitionPath;
    this.partitionColumnValues = partitionColumnValues;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public List<Object> getPartitionColumnValues() {
    return partitionColumnValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodiePartitionDescriptor that = (HoodiePartitionDescriptor) o;
    return Objects.equals(partitionPath, that.partitionPath) && Objects.equals(partitionColumnValues, that.partitionColumnValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionPath, partitionColumnValues);
  }

  @Override
  public String toString() {
    return "HoodiePartitionDescriptor{" +
        "partitionPath='" + partitionPath + '\'' +
        ", partitionColumnValues=" + partitionColumnValues +
        '}';
  }
}
