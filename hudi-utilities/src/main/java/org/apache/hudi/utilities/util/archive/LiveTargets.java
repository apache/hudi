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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The instants a search is looking for, and which of them have been located so far.
 *
 * <p>Targets are held sorted so that "does this file's range hold anything still worth reading?" is a
 * binary search rather than a scan of the whole set. That is strictly better than tracking a single
 * live {@code [min, max]} bound: a file whose range falls in a <em>gap</em> between live targets is
 * skipped too.
 *
 * <p>Two different questions are asked of this class, and they deliberately consult different
 * things:
 *
 * <ul>
 *   <li>{@link #hasLiveIn} drives <b>file pruning</b> and consults only targets not yet found.
 *   <li>{@link #isTarget} drives <b>record delivery</b> and consults the full target set, found or
 *       not. This matters: the v6 archive persists a requested, an inflight and a completed entry
 *       per instant, and only one of them carries write stats. Stopping delivery at the first record
 *       would make what a consumer sees depend on read order.
 * </ul>
 *
 * <p>All methods are safe to call from many threads.
 */
public final class LiveTargets {

  private final String[] sorted;
  private final AtomicReferenceArray<StoragePath> locations;
  private final AtomicInteger remaining;

  private LiveTargets(String[] sorted) {
    this.sorted = sorted;
    this.locations = new AtomicReferenceArray<>(sorted.length);
    this.remaining = new AtomicInteger(sorted.length);
  }

  /**
   * @param targets the instants to look for; duplicates are collapsed and nulls rejected
   */
  public static LiveTargets of(Collection<String> targets) {
    Objects.requireNonNull(targets, "targets");
    TreeSet<String> unique = new TreeSet<>();
    for (String target : targets) {
      unique.add(Objects.requireNonNull(target, "a target instant was null"));
    }
    return new LiveTargets(unique.toArray(new String[0]));
  }

  /** The distinct target instants, ascending. */
  public List<String> instants() {
    return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(sorted)));
  }

  public int size() {
    return sorted.length;
  }

  /** True once every target has been located, which is the only sound reason to stop searching. */
  public boolean allFound() {
    return remaining.get() == 0;
  }

  /** Whether this instant is one of the instants being searched for, found or not. */
  public boolean isTarget(String instant) {
    return Arrays.binarySearch(sorted, instant) >= 0;
  }

  public boolean isFound(String instant) {
    int index = Arrays.binarySearch(sorted, instant);
    return index >= 0 && locations.get(index) != null;
  }

  public Option<StoragePath> locationOf(String instant) {
    int index = Arrays.binarySearch(sorted, instant);
    return index < 0 ? Option.empty() : Option.ofNullable(locations.get(index));
  }

  /**
   * Records where an instant was found. First writer wins, so the recorded file is the newest one
   * that held it.
   *
   * @return true when this call was the one that marked it, false when it was already found or is
   *         not a target at all
   */
  public boolean markFound(String instant, StoragePath archiveFile) {
    Objects.requireNonNull(archiveFile, "archiveFile");
    int index = Arrays.binarySearch(sorted, instant);
    if (index < 0) {
      return false;
    }
    if (!locations.compareAndSet(index, null, archiveFile)) {
      return false;
    }
    remaining.decrementAndGet();
    return true;
  }

  /**
   * Whether any target still to be found lies inside this range.
   *
   * <p>Races against concurrent {@link #markFound} calls are safe in both directions: reading a
   * stale <em>live</em> flag costs an unnecessary scan, and reading a fresh <em>found</em> flag is
   * simply correct. A file can never be skipped while it holds a target still needing to be found,
   * because a target only ever moves live to found, never back.
   */
  public boolean hasLiveIn(InstantRange range) {
    if (range.isAll()) {
      return !allFound();
    }
    int from = lowerBound(range.min());
    for (int i = from; i < sorted.length && sorted[i].compareTo(range.max()) <= 0; i++) {
      if (locations.get(i) == null) {
        return true;
      }
    }
    return false;
  }

  /** Index of the first target not sorting before {@code key}. */
  private int lowerBound(String key) {
    int low = 0;
    int high = sorted.length;
    while (low < high) {
      int mid = (low + high) >>> 1;
      if (sorted[mid].compareTo(key) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }
}
