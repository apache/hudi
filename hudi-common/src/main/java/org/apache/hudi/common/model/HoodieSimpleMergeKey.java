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
 * Wraps {@link HoodieKey} and implements the {@link HoodieMergeKey} interface for simple scenarios where the key is a string.
 */
public class HoodieSimpleMergeKey implements HoodieMergeKey {

  private final HoodieKey simpleKey;

  public HoodieSimpleMergeKey(HoodieKey simpleKey) {
    this.simpleKey = simpleKey;
  }

  @Override
  public String getPartitionPath() {
    return simpleKey.getPartitionPath();
  }

  @Override
  public Serializable getRecordKey() {
    return simpleKey.getRecordKey();
  }

  public HoodieKey getHoodieKey() {
    return simpleKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieSimpleMergeKey that = (HoodieSimpleMergeKey) o;
    return Objects.equals(simpleKey.getRecordKey(), that.simpleKey.getRecordKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(simpleKey.getRecordKey());
  }
}
