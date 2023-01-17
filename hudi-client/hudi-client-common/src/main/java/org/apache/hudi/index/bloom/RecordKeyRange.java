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

package org.apache.hudi.index.bloom;

import java.io.Serializable;

/**
 * Represents the min and max record key in the {@link KeyRangeNode}.
 */
class RecordKeyRange implements Comparable<RecordKeyRange>, Serializable {

  private final String minRecordKey;
  private final String maxRecordKey;

  /**
   * Instantiates a new {@link RecordKeyRange}.
   * @param minRecordKey min record key
   * @param maxRecordKey max record key
   */
  RecordKeyRange(String minRecordKey, String maxRecordKey) {
    this.minRecordKey = minRecordKey;
    this.maxRecordKey = maxRecordKey;
  }

  @Override
  public String toString() {
    return "RecordKeyRange{minRecordKey='" + minRecordKey + '\'' + ", maxRecordKey='" + maxRecordKey + '}';
  }

  /**
   * Compares the min record key, followed by max record key.
   *
   * @param that the {@link RecordKeyRange} to be compared with
   * @return the result of comparison. 0 if both min and max are equal in both. 1 if this {@link RecordKeyRange} is
   * greater than the {@code that} keyRangeNode. -1 if {@code that} keyRangeNode is greater than this {@link
   * RecordKeyRange}
   */
  @Override
  public int compareTo(RecordKeyRange that) {
    int compareValue = minRecordKey.compareTo(that.minRecordKey);
    if (compareValue == 0) {
      return maxRecordKey.compareTo(that.maxRecordKey);
    } else {
      return compareValue;
    }
  }

  public String getMinRecordKey() {
    return minRecordKey;
  }

  public String getMaxRecordKey() {
    return maxRecordKey;
  }
}
