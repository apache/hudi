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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.spark.Partitioner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Routes phase-2 records to Spark partitions by <b>writing instant time</b>, so that each partition
 * owns a contiguous range of instants.
 *
 * <p>{@code bounds} holds the <b>last instant belonging to each partition but the last</b>, sorted
 * ascending and free of duplicates. Partition {@code i} therefore owns
 * {@code (bounds[i - 1], bounds[i]]}, partition {@code 0} owns {@code (-inf, bounds[0]]}, and the
 * final partition owns {@code (bounds[last], +inf)}.
 *
 * <p><b>Total over the key domain.</b> Every string routes somewhere; there is no membership check
 * and no way for an unexpected key to fail the job. That is not a convenience — it is the whole
 * reason this class exists. Its predecessor answered a key outside its sampled space with
 * {@code checkArgument}, which killed the entire validation, so both phase-2 dimensions had to
 * bound their shuffled population to a sampled subset and report the excluded records without a
 * verdict. Nothing needs to do that any more.
 *
 * <p><b>An instant is never split across partitions.</b> A key equal to {@code bounds[i]} binary
 * searches to exactly {@code i}, and a key strictly between two bounds resolves to a single
 * insertion point. Every record of one instant is therefore co-located, which is what lets a task
 * scan the archived timeline at most once per instant it owns.
 *
 * <p>String ordering is the correct ordering for instant times: {@code InstantComparison}'s
 * predicates are all {@code commit1.compareTo(commit2)}.
 *
 * <p><b>A null key is a caller contract violation, not a race.</b> Both callers already drop
 * records that name no instant, because what to do with such a record differs between them. So this
 * class rejects null rather than inventing a bucket for it.
 */
public final class InstantRangePartitioner extends Partitioner {

  private static final long serialVersionUID = 1L;

  private final List<String> bounds;

  /**
   * @param bounds the last instant of each partition but the last, sorted ascending and distinct.
   *               An empty list means a single partition holding everything.
   */
  public InstantRangePartitioner(List<String> bounds) {
    ValidationUtils.checkArgument(bounds != null, "InstantRangePartitioner requires bounds");
    this.bounds = Collections.unmodifiableList(new ArrayList<>(bounds));
  }

  @Override
  public int numPartitions() {
    return bounds.size() + 1;
  }

  @Override
  public int getPartition(Object key) {
    ValidationUtils.checkArgument(key instanceof String,
        "InstantRangePartitioner expects a non-null String instant time key, got "
            + (key == null ? "null" : key.getClass().getName()));
    int index = Collections.binarySearch(bounds, (String) key);
    return index >= 0 ? index : -index - 1;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof InstantRangePartitioner)) {
      return false;
    }
    return bounds.equals(((InstantRangePartitioner) other).bounds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bounds);
  }

  @Override
  public String toString() {
    return "InstantRangePartitioner{partitions=" + numPartitions() + ", bounds=" + bounds + "}";
  }
}
