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

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Answers, for one suspect file, whether anything on the timeline still references it.
 *
 * <p>A {@code DANGLING_*} verdict is strong enough for an operator or a cleanup tool to act on
 * without a second opinion, so anything short of proof is {@link Outcome#NOT_SETTLED} and nothing
 * throws. It reads no storage: the driver's completed timeline, its cutoff and this task's
 * {@link ArchivedInstantWorkingSet} are its whole evidence, so a verdict cannot depend on when an
 * executor's task started.
 *
 * <p>The rule is membership, not exactly-one ({@code HoodieFileGroup.getFileAddedForInstant}): write
 * stats name log files too, so a MOR file group routinely has several recorded paths per commit.
 */
public final class DanglingAdjudicator {

  private static final Logger LOG = LoggerFactory.getLogger(DanglingAdjudicator.class);

  /** What the timeline says about one suspect file. */
  public enum Outcome {
    /** No completed commit for the file's instant could be established, and the absence is proven. */
    DANGLING_NO_COMPLETED_COMMIT,
    /** The commit completed and recorded paths for the file group, and this file is not among them. */
    DANGLING_COMMIT_DOES_NOT_NAME_IT,
    /** The commit records this file's name for the file group. */
    REFERENCED,
    /** The question could not be answered. Never a licence to delete. */
    NOT_SETTLED
  }

  /** The driver's {@code T_prevD} narrowed to completed commits, or {@code null} if it was unreadable. */
  private final HoodieTimeline completedActive;

  /** The archival cutoff suspects are bucketed against; {@code null} sends every instant to the archive. */
  private final String earliestActiveWriteCommit;

  /**
   * This task's one archived pass, or {@code null} when none ran (a refusal, never an absence). Keyed
   * by file group so a question about one group is never answered from another group's names.
   */
  private final ArchivedInstantWorkingSet<ArchivedCommitPaths> archived;

  private final Map<String, HoodieInstant> completedActiveByTime;

  /**
   * @param completedActive           the driver's completed {@code T_prevD}, or {@code null} if unreadable
   * @param earliestActiveWriteCommit that read's earliest write commit, computed once on the driver
   * @param archived                  this task's archived working set, or {@code null} if none ran
   */
  public DanglingAdjudicator(HoodieTimeline completedActive, String earliestActiveWriteCommit,
                             ArchivedInstantWorkingSet<ArchivedCommitPaths> archived) {
    this.completedActive = completedActive;
    this.earliestActiveWriteCommit = earliestActiveWriteCommit;
    this.archived = archived;
    Map<String, HoodieInstant> byTime = new HashMap<>();
    if (completedActive != null) {
      for (HoodieInstant instant : completedActive.getInstants()) {
        String requestedTime = instant.requestedTime();
        if (requestedTime == null) {
          continue;
        }
        byTime.putIfAbsent(requestedTime, instant);
      }
    }
    this.completedActiveByTime = byTime;
  }

  /**
   * Whether anything the timeline records still references {@code fileName}; never throws, and an
   * unanswerable question is {@link Outcome#NOT_SETTLED}. Below table version 8 a log file's name
   * carries its slice's base commit, so {@code instantTime} must never be parsed from one.
   *
   * @param partitionPath relative partition path the file was listed in
   * @param fileId        the file group id the file belongs to
   * @param fileName      the bare file name, never a path
   * @param instantTime   the instant that wrote the file
   */
  public Outcome adjudicate(String partitionPath, String fileId, String fileName,
                            String instantTime) {
    try {
      if (partitionPath == null || fileId == null || fileName == null || instantTime == null) {
        BranchBeacon.hit(Branch.DAJ_INPUT_INCOMPLETE);
        return Outcome.NOT_SETTLED;
      }
      if (completedActive == null) {
        BranchBeacon.hit(Branch.DAJ_TIMELINE_UNREADABLE);
        return Outcome.NOT_SETTLED;
      }
      // Same expression the archived pass bucketed with, so the working set is only asked what it covers.
      if (InstantBuckets.pokesArchive(instantTime, earliestActiveWriteCommit)) {
        return adjudicateAgainstArchive(partitionPath, fileId, fileName, instantTime);
      }
      // Completion before membership: a name set resolved for another instant cannot rescue this one.
      HoodieInstant active = completedActiveByTime.get(instantTime);
      if (active != null) {
        return adjudicateAgainstActive(partitionPath, fileId, fileName, active);
      }
      // Inside the window the active timeline is authoritative and contiguous back to the archival
      // cutoff, so an instant that is not on it never completed.
      BranchBeacon.hit(Branch.DAJ_NEVER_COMMITTED);
      return Outcome.DANGLING_NO_COMPLETED_COMMIT;
    } catch (Exception ex) {
      BranchBeacon.hit(Branch.DAJ_THREW);
      LOG.warn("Could not adjudicate {} of file group {} in partition {} at instant {}; reporting "
          + "it unsettled", fileName, fileId, partitionPath, instantTime, ex);
      return Outcome.NOT_SETTLED;
    }
  }

  /** The instant is completed on the active timeline, so only membership remains. */
  private Outcome adjudicateAgainstActive(String partitionPath, String fileId, String fileName,
                                          HoodieInstant instant) {
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
    List<String> recordedPaths;
    try {
      recordedPaths = CommitWriteStats.getFilePathsAddedForInstant(completedActive, fileGroupId, instant);
    } catch (Exception ex) {
      // "I could not check" is not "nothing references this".
      BranchBeacon.hit(Branch.DAJ_ACTIVE_METADATA_UNREADABLE);
      LOG.warn("Could not read the commit metadata of {} for file group {} in partition {}",
          instant, fileId, partitionPath, ex);
      return Outcome.NOT_SETTLED;
    }
    if (recordedPaths.isEmpty()) {
      // Reads as "not referenced", refused anyway: getWriteStats is keyed on the partition-path
      // string, and one spelling mismatch returns zero for every file in the partition.
      BranchBeacon.hit(Branch.DAJ_ACTIVE_NO_RECORDED_PATHS);
      return Outcome.NOT_SETTLED;
    }
    // A base file is always a write stat's path, so getLogFiles() and getCdcStats() need not be read.
    return beacon(namesOf(recordedPaths).contains(fileName),
        Branch.DAJ_ACTIVE_REFERENCED, Outcome.REFERENCED,
        Branch.DAJ_ACTIVE_NOT_NAMED, Outcome.DANGLING_COMMIT_DOES_NOT_NAME_IT);
  }

  /** Answers from this file group's entry of the working set, never the whole batch; no I/O here. */
  private Outcome adjudicateAgainstArchive(String partitionPath, String fileId, String fileName,
                                           String instantTime) {
    if (archived == null) {
      // No resolver ran (a v2-layout table, or no archive bucket), which proves nothing.
      BranchBeacon.hit(Branch.DAJ_ARCHIVE_UNPROVEN);
      return Outcome.NOT_SETTLED;
    }
    switch (archived.stateOf(instantTime)) {
      case FOUND:
        Set<String> named = archived.contentOf(instantTime).orElse(ArchivedCommitPaths.empty())
            .namesOf(new HoodieFileGroupId(partitionPath, fileId));
        if (named.isEmpty()) {
          // Nothing recorded for this group is indistinguishable from a no-match projection: refused.
          BranchBeacon.hit(Branch.DAJ_ARCHIVE_NO_RECORDED_PATHS);
          return Outcome.NOT_SETTLED;
        }
        return beacon(named.contains(fileName),
            Branch.DAJ_ARCHIVE_REFERENCED, Outcome.REFERENCED,
            Branch.DAJ_ARCHIVE_NOT_NAMED, Outcome.DANGLING_COMMIT_DOES_NOT_NAME_IT);
      case NOT_FOUND:
        // Every byte that could have held the instant was read cleanly and it was not there.
        BranchBeacon.hit(Branch.DAJ_ARCHIVE_ABSENT);
        return Outcome.DANGLING_NO_COMPLETED_COMMIT;
      default:
        BranchBeacon.hit(Branch.DAJ_ARCHIVE_UNPROVEN);
        return Outcome.NOT_SETTLED;
    }
  }

  /** Records the branch taken; both outcomes are reachable from the active and the archived path. */
  private static Outcome beacon(boolean condition, Branch whenTrue, Outcome ifTrue,
                                Branch whenFalse, Outcome ifFalse) {
    BranchBeacon.hit(condition ? whenTrue : whenFalse);
    return condition ? ifTrue : ifFalse;
  }

  /** Bare names, never paths: suspects come from a listing, and recorded paths are table-relative. */
  private static Set<String> namesOf(List<String> paths) {
    Set<String> names = new LinkedHashSet<>();
    for (String path : paths) {
      addName(names, path);
    }
    return names;
  }

  private static void addName(Set<String> names, String path) {
    if (path == null || path.isEmpty()) {
      return;
    }
    names.add(new StoragePath(path).getName());
  }
}
