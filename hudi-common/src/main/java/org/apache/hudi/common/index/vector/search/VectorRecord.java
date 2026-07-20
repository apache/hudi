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
 * A decoded record read back from the base table by a {@link org.apache.hudi.common.index.vector.search}
 * read handle (RFC-109 v3 §9): the logical record key, its full-precision vector, and the live
 * location it was read from. Only the record-key and vector columns are decoded — no full-row
 * materialization.
 */
public final class VectorRecord implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String recordKey;
  private final float[] vector;
  private final HoodieRecordGlobalLocation location;

  public VectorRecord(String recordKey, float[] vector, HoodieRecordGlobalLocation location) {
    this.recordKey = recordKey;
    this.vector = vector;
    this.location = location;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public float[] getVector() {
    return vector;
  }

  public HoodieRecordGlobalLocation getLocation() {
    return location;
  }
}
