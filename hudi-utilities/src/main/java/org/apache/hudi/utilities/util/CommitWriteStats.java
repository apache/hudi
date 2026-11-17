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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * Reads the write stats a completed commit recorded for one file group.
 *
 * <p>The lookup keeps "the commit named nothing", "the commit named several" and "the commit could
 * not be read" three distinct answers: it <b>throws</b> when the commit metadata cannot be read and
 * returns a (possibly empty, possibly multi-element) list when it can. Distinguishing them decides
 * whether a file is a proven orphan or merely unadjudicated.
 */
public final class CommitWriteStats {

  private CommitWriteStats() {
  }

  /**
   * Every path {@code instant} recorded for {@code fileGroupId}, read from its commit metadata.
   *
   * @param timeline    the timeline to read the commit metadata from
   * @param fileGroupId the file group whose write stats are wanted
   * @param instant     a completed instant
   * @return the recorded paths, empty for an action that records none
   * @throws IOException if the commit metadata cannot be read
   */
  public static List<String> getFilePathsAddedForInstant(HoodieTimeline timeline,
                                                         HoodieFileGroupId fileGroupId,
                                                         HoodieInstant instant) throws IOException {
    return getWriteStatsAddedForInstant(timeline, fileGroupId, instant).stream()
        .map(HoodieWriteStat::getPath)
        .collect(Collectors.toList());
  }

  /**
   * Every write stat {@code instant} recorded for {@code fileGroupId}, before anything is projected
   * out of them.
   *
   * <p>{@link HoodieDeltaWriteStat#getLogFiles()} and {@code path} do not describe the same set:
   *
   * <ul>
   *   <li>{@code path} names the one log file <em>this</em> stat wrote. It is complete for the
   *       commit: on a log rollover the append handle starts a fresh stat from
   *       {@code prevStat.copy()}, so every rolled-over log file arrives with a {@code path} of its
   *       own.
   *   <li>{@code getLogFiles()} is a <b>superset</b>: it is seeded from the pre-existing file
   *       slice's log files and {@code copy()} carries that forward, so it also names log files
   *       written by earlier commits.
   * </ul>
   *
   * <p>A caller deciding which single path a commit recorded must therefore use {@code path} alone.
   * A caller deciding whether a log file is referenced <em>at all</em> may additionally consult
   * {@code getLogFiles()}: it over-names rather than under-names, so it can only ever turn a
   * would-be deletion into a refusal.
   *
   * @param timeline    the timeline to read the commit metadata from
   * @param fileGroupId the file group whose write stats are wanted
   * @param instant     a completed instant
   * @return the recorded write stats, empty for an action that records none and for a partition the
   *         commit never touched
   * @throws IOException if the commit metadata cannot be read
   */
  public static List<HoodieWriteStat> getWriteStatsAddedForInstant(HoodieTimeline timeline,
                                                                   HoodieFileGroupId fileGroupId,
                                                                   HoodieInstant instant) throws IOException {
    List<HoodieWriteStat> writeStats;
    switch (instant.getAction()) {
      case COMMIT_ACTION:
      case DELTA_COMMIT_ACTION:
        HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(instant);
        writeStats = commitMetadata.getWriteStats(fileGroupId.getPartitionPath());
        break;
      case REPLACE_COMMIT_ACTION:
        HoodieReplaceCommitMetadata replaceCommitMetadata = timeline.readReplaceCommitMetadata(instant);
        writeStats = replaceCommitMetadata.getWriteStats(fileGroupId.getPartitionPath());
        break;
      default:
        return Collections.emptyList();
    }
    if (writeStats == null) {
      // HoodieCommitMetadata.getWriteStats(String) is a bare map lookup and returns null when the
      // commit never touched this partition. Returning empty keeps that an ordinary "recorded
      // nothing" answer rather than an NPE surfacing as "the commit could not be read".
      return Collections.emptyList();
    }
    return writeStats.stream()
        .filter(stat -> stat.getFileId().equals(fileGroupId.getFileId()))
        .collect(Collectors.toList());
  }
}
