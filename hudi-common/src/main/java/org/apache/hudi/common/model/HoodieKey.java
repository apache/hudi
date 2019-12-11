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

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * HoodieKey consists of
 * <p>
 * - recordKey : a recordKey that acts as primary key for a record - partitionPath : path to the partition that contains
 * the record.
 * - partitionPath : the partition path of a record.
 */
public class HoodieKey implements Serializable {


  private final String recordKey;

  private final String partitionPath;

  public HoodieKey(String recordKey, String partitionPath) {
    this.recordKey = recordKey;
    this.partitionPath = partitionPath;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieKey otherKey = (HoodieKey) o;
    return Objects.equal(recordKey, otherKey.recordKey) && Objects.equal(partitionPath, otherKey.partitionPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(recordKey, partitionPath);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieKey {");
    sb.append(" recordKey=").append(recordKey);
    sb.append(" partitionPath=").append(partitionPath);
    sb.append('}');
    return sb.toString();
  }
}
