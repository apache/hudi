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
import org.apache.hudi.source.IncrementalInputSplits;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result from continuous enumerator. It has the same semantic to the {@link org.apache.hudi.source.IncrementalInputSplits.Result}.
 */
public class HoodieContinuousSplitBatch {
  public static final HoodieContinuousSplitBatch EMPTY = new HoodieContinuousSplitBatch(Collections.emptyList(), "", "");
  private final Collection<HoodieSourceSplit> splits;
  private final String endInstant; // end instant to consume to
  private final String offset;     // monotonic increasing consumption offset

  /**
   * @param splits should never be null. But it can be an empty collection
   * @param endInstant should never be null, end instant to consume to
   * @param offset could be null. monotonic increasing consumption offset
   */
  public HoodieContinuousSplitBatch(
      Collection<HoodieSourceSplit> splits,
      String endInstant,
      String offset) {

    ValidationUtils.checkArgument(splits != null, "Invalid to splits collection: null");
    ValidationUtils.checkArgument(endInstant != null, "Invalid end instant: null");
    this.splits = splits;
    this.endInstant = endInstant;
    this.offset = offset;
  }

  public static HoodieContinuousSplitBatch fromResult(IncrementalInputSplits.Result result) {
    List<HoodieSourceSplit> splits = result.getInputSplits().stream().map(split ->
        new HoodieSourceSplit(
            HoodieSourceSplit.SPLIT_COUNTER.incrementAndGet(),
            split.getBasePath().orElse(null),
            split.getLogPaths(), split.getTablePath(),
            split.getMergeType(), split.getFileId()
        )
    ).collect(Collectors.toList());

    return new HoodieContinuousSplitBatch(splits, result.getEndInstant(), result.getOffset());
  }

  public Collection<HoodieSourceSplit> getSplits() {
    return splits;
  }

  public String getEndInstant() {
    return endInstant;
  }

  public String getOffset() {
    return offset;
  }
}
