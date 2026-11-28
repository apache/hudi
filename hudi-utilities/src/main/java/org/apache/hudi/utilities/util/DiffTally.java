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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What one phase-1 unit of work found, and how much of the table it looked at.
 *
 * <p><b>It lives on the executor and never crosses a shuffle.</b> {@link FileSliceMismatchAnalyzer}
 * fills one per partition, the task that made it merges its partitions' tallies into one, and the
 * two counters go back to the driver on accumulators while the records go back through the RDD. The
 * unit of the phase-2 shuffle is a {@link FileDiffRecord}, because a record is what an adjudicator
 * decides.
 *
 * <p><b>It is not keyed by partition, and the counters are not per partition either.</b> Nothing
 * downstream reads either figure by partition: {@code fs_mdt_diff/} carries one {@code filesSeen}
 * and one merged {@code dropCounts} for the whole run, and the records carry their own partition.
 *
 * <p>The two counters exist because neither can be derived from the records. A file excluded from
 * adjudication is counted and <em>not</em> carried forward as a record, so a run whose every file
 * was dropped has no records and is still not clean; and {@code filesSeen} is the denominator a
 * consumer sizes a delete list against, which a partition with nothing wrong contributes to just as
 * much as one that did.
 */
public class DiffTally implements Serializable {

  private static final long serialVersionUID = 1L;

  private final List<FileDiffRecord> records = new ArrayList<>();
  private final Map<String, Long> dropCounts = new LinkedHashMap<>();
  private long totalCount;
  private long filesSeen;

  public void add(FileDiffRecord record) {
    totalCount++;
    records.add(record);
  }

  /** Records one file excluded from adjudication, by class. See the drop rules in the design. */
  public void countDrop(String dropClass) {
    addDrops(dropClass, 1L);
  }

  /**
   * Adds {@code howMany} drops of one class at once.
   *
   * <p>The bulk form is what {@link #merge} folds with, and what a caller restoring a tally from
   * counts already taken uses. Replaying {@link #countDrop} once per dropped file would make that
   * linear in the number of files dropped, a population that is unbounded on a table whose log
   * files cannot be read.
   */
  public void addDrops(String dropClass, long howMany) {
    Long seen = dropCounts.get(dropClass);
    dropCounts.put(dropClass, seen == null ? howMany : seen + howMany);
  }

  public void addFilesSeen(long count) {
    filesSeen += count;
  }

  /**
   * Folds another tally into this one.
   *
   * <p>Called on the task thread as each partition's future is drained, never from the partition
   * pool: this class is not synchronized, and it does not need to be as long as the merge happens
   * where the futures are collected.
   */
  public void merge(DiffTally other) {
    for (FileDiffRecord record : other.records) {
      add(record);
    }
    for (Map.Entry<String, Long> drop : other.dropCounts.entrySet()) {
      addDrops(drop.getKey(), drop.getValue());
    }
    addFilesSeen(other.filesSeen);
  }

  public List<FileDiffRecord> getRecords() {
    return Collections.unmodifiableList(records);
  }

  public Map<String, Long> getDropCounts() {
    return Collections.unmodifiableMap(dropCounts);
  }

  public long getTotalCount() {
    return totalCount;
  }

  public long getFilesSeen() {
    return filesSeen;
  }

  public boolean isEmpty() {
    return totalCount == 0;
  }
}
