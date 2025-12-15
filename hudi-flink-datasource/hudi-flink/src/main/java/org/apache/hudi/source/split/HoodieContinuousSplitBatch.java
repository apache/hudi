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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.ValidationUtils;

import java.util.Collection;
import java.util.Collections;

/**
 * Result from continuous enumerator.
 */
public class HoodieContinuousSplitBatch {
  public static final HoodieContinuousSplitBatch EMPTY = new HoodieContinuousSplitBatch(Collections.emptyList(), "", "");
  private final Collection<HoodieSourceSplit> splits;
  private final String fromInstant;
  private final String toInstant;

  /**
   * @param splits should never be null. But it can be an empty collection
   * @param fromInstant could be null
   * @param toInstant should never be null. But it can be empty string
   */
  public HoodieContinuousSplitBatch(
      Collection<HoodieSourceSplit> splits,
      String fromInstant,
      String toInstant) {

    ValidationUtils.checkArgument(splits != null, "Invalid to splits collection: null");
    ValidationUtils.checkArgument(toInstant != null, "Invalid end instant: null");
    this.splits = splits;
    this.fromInstant = fromInstant;
    this.toInstant = toInstant;
  }

  public Collection<HoodieSourceSplit> getSplits() {
    return splits;
  }

  public String getFromInstant() {
    return fromInstant;
  }

  public String getToInstant() {
    return toInstant;
  }
}
