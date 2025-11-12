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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Column statistics.
 */
public class ColumnStats {

  @Nullable private final Object minVal;
  @Nullable private final Object maxVal;
  private final long nullCnt;

  public ColumnStats(@Nullable Object minVal, @Nullable Object maxVal, long nullCnt) {
    this.minVal = minVal;
    this.maxVal = maxVal;
    this.nullCnt = nullCnt;
  }

  @Nullable
  public Object getMinVal() {
    return minVal;
  }

  @Nullable
  public Object getMaxVal() {
    return maxVal;
  }

  public long getNullCnt() {
    return nullCnt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnStats that = (ColumnStats) o;
    return nullCnt == that.nullCnt
        && Objects.equals(minVal, that.minVal)
        && Objects.equals(maxVal, that.maxVal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(minVal, maxVal, nullCnt);
  }
}
