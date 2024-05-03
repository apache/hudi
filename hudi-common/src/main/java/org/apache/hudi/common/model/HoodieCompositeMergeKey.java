/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * A composite key for merge operation.
 * Implements the {@link HoodieMergeKey} interface but allows for complex types as keys,
 * enhancing flexibility for scenarios where a simple string key is not sufficient.
 */
public class HoodieCompositeMergeKey<K extends Serializable> implements HoodieMergeKey {

  private final K compositeKey;
  private final String partitionPath;

  public HoodieCompositeMergeKey(K compositeKey, String partitionPath) {
    this.compositeKey = compositeKey;
    this.partitionPath = partitionPath;
  }

  @Override
  public String getPartitionPath() {
    return partitionPath;
  }

  @Override
  public Serializable getRecordKey() {
    return compositeKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieCompositeMergeKey<?> that = (HoodieCompositeMergeKey<?>) o;
    return Objects.equals(compositeKey, that.compositeKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compositeKey);
  }
}
