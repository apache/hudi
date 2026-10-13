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

package org.apache.hudi.source.stats;

import lombok.Value;

import javax.annotation.Nullable;

/**
 * Column statistics.
 */
@Value
public class ColumnStats {

  @Nullable
  Object minVal;
  @Nullable
  Object maxVal;
  /**
   * Number of null values in the column for this file.
   * A negative value (-1) is used as a sentinel to indicate that statistics
   * are unavailable (e.g. after CSI column-list expansion). Missing stats
   * must never be treated as "all values are null".
   */
  long nullCnt;

  /** Returns true when column statistics are not available for this file. */
  public boolean isUnknown() {
    return nullCnt < 0;
  }

  /** Returns true when the column is known to contain only null values. */
  public boolean isAllNull() {
    return !isUnknown() && minVal == null && maxVal == null && nullCnt > 0;
  }
}