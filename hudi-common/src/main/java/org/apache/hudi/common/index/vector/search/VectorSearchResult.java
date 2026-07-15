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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import java.io.Serializable;

/**
 * One engine-neutral final result row from a vector search (RFC-104 v3 §1): the logical record key,
 * the exact metric distance (squared L2 kept internally, surfaced per the requested metric), and
 * the live record location the value was read from.
 */
public final class VectorSearchResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String recordKey;
  private final double distance;
  private final HoodieRecordGlobalLocation location;

  public VectorSearchResult(String recordKey, double distance, HoodieRecordGlobalLocation location) {
    this.recordKey = recordKey;
    this.distance = distance;
    this.location = location;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public double getDistance() {
    return distance;
  }

  public HoodieRecordGlobalLocation getLocation() {
    return location;
  }
}
