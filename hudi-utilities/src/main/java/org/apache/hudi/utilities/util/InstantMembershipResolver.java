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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;
import org.apache.hudi.utilities.util.archive.InstantSearchState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Answers, for a batch of (file, writing-instant) pairs, whether the commit metadata for the
 * writing instant names the file: {@link Membership#COMMITTED}, {@link Membership#DANGLING}, or
 * {@link Membership#INSTANT_NOT_FOUND} when the question could not be settled either way.
 *
 * <p><b>{@code DANGLING} requires positive proof</b>: a commit was found and read, and its recorded
 * names do not include the file. That includes a commit that records partitions but nothing for this
 * file group or partition, once partition keys are normalised on both sides. Everything else (an
 * unplaceable instant, a failed read, an action with no write stats, an archived answer too narrow to
 * trust) is {@code INSTANT_NOT_FOUND}. Never throws: a caller handed an exception has no safe guess.
 *
 * <p>A file is named if its bare name is in {@code stat.getPath()} &cup;
 * {@code HoodieDeltaWriteStat.getLogFiles()} &cup; {@code stat.getCdcStats().keySet()}. The log-file
 * term is a superset seeded from the prior slice, so it can only turn {@code DANGLING} into
 * {@code COMMITTED}; the CDC term is required because CDC files are recorded nowhere else
 * ({@code HoodieTableMetadataUtil:501-503}).
 *
 * <p>Records archived before Hudi 1.0.0-beta1 carry neither {@code logFiles} nor {@code cdcStats}, so
 * their projection is {@code path} alone. That is safe for an ordinary log file, which has had its
 * own write stat {@code path} since Hudi 0.7.0, so it is condemned like a base file. A CDC file appears
 * only in {@code cdcStats}, so an archived CDC file is never {@code DANGLING} ({@link #isCdcFile});
 * lifting that needs proof a record's schema declared {@code cdcStats}, which a decoded v6 record
 * cannot give.
 *
 * <p>{@link #lookup} routes by {@link InstantBuckets#pokesArchive}, the same cutoff expression the
 * archived pass was bucketed with, never by active-timeline absence, so the working set is only asked
 * about instants it targeted. The caller materializes the working set in one batched pass
 * ({@code null}: no archive read); only {@link InstantSearchState#FOUND} is proof: a name set for
 * this file group, or a completed commit that recorded write stats and names nothing for this group.
 */
public final class InstantMembershipResolver {

  private static final Logger LOG = LoggerFactory.getLogger(InstantMembershipResolver.class);

  /** What a batched lookup could establish about one (file, writing-instant) pair. */
  public enum Membership {
    /** The writing instant's commit metadata names this file. */
    COMMITTED,
    /** The writing instant's commit metadata was found, read, and does not name this file. */
    DANGLING,
    /** The question could not be settled either way. Never a licence to treat the file as orphaned. */
    INSTANT_NOT_FOUND
  }

  private final HoodieTableMetaClient metaClient;

  /** The completed instants on the active timeline this resolver was built to answer from. */
  private final HoodieTimeline completedActive;

  /**
   * The routing cutoff, taken from the same value the archived pass was bucketed with and never
   * re-derived from {@link #completedActive}; {@code null} routes every instant to the archive.
   */
  private final String earliestActiveWriteCommit;

  /** The archived working set keyed by file group, or {@code null} when no archive was read. */
  private final ArchivedInstantWorkingSet<ArchivedCommitPaths> archived;

  private final Map<String, HoodieInstant> completedActiveByTime;

  public InstantMembershipResolver(HoodieTableMetaClient metaClient, HoodieTimeline completedActive,
                                   String earliestActiveWriteCommit,
                                   ArchivedInstantWorkingSet<ArchivedCommitPaths> archived) {
    this.metaClient = metaClient;
    this.completedActive = completedActive;
    this.earliestActiveWriteCommit = earliestActiveWriteCommit;
    this.archived = archived;
    Map<String, HoodieInstant> byTime = new HashMap<>();
    if (completedActive != null) {
      try {
        for (HoodieInstant instant : completedActive.getInstants()) {
          String requestedTime = instant.requestedTime();
          if (requestedTime != null) {
            byTime.putIfAbsent(requestedTime, instant);
          }
        }
      } catch (Exception ex) {
        // A half-built index could claim an instant it never resolved; empty is the safe direction.
        LOG.warn("Could not read the completed active timeline of {}; every instant will be looked "
            + "up as if it were not on it", safeBasePath(metaClient), ex);
        byTime = new HashMap<>();
      }
    }
    this.completedActiveByTime = byTime;
  }

  /**
   * Whether the commit metadata for {@code instantTime} names {@code fileName} in file group
   * {@code fileGroupId} of {@code partition}. Never throws; an unanswerable question is
   * {@link Membership#INSTANT_NOT_FOUND}.
   *
   * @param partition   relative partition path the file was listed in
   * @param fileGroupId the file group id the file belongs to
   * @param fileName    the bare file name, never a path
   * @param instantTime the instant that wrote the file
   */
  public Membership lookup(String partition, String fileGroupId, String fileName,
                           String instantTime) {
    try {
      if (partition == null || fileGroupId == null || fileName == null || instantTime == null) {
        BranchBeacon.hit(Branch.IMR_INPUT_INCOMPLETE);
        return Membership.INSTANT_NOT_FOUND;
      }
      // Route on the archived pass's own cutoff expression, never on active-timeline absence.
      if (InstantBuckets.pokesArchive(instantTime, earliestActiveWriteCommit)) {
        return lookupAgainstArchive(partition, fileGroupId, fileName, instantTime);
      }
      HoodieInstant active = completedActiveByTime.get(instantTime);
      if (active != null) {
        return lookupAgainstActive(partition, fileGroupId, fileName, active);
      }
      // Inside the window but not on the active timeline: this class never condemns on absence.
      BranchBeacon.hit(Branch.IMR_INSIDE_WINDOW_BUT_NOT_COMPLETED);
      return Membership.INSTANT_NOT_FOUND;
    } catch (Exception ex) {
      BranchBeacon.hit(Branch.IMR_THREW);
      LOG.warn("Could not look up {} in partition {} of {}; reporting the instant not found",
          fileName, partition, safeBasePath(metaClient), ex);
      return Membership.INSTANT_NOT_FOUND;
    }
  }

  /**
   * Answers from the write stats of an instant on the completed active timeline; an empty raw lookup
   * is retried against normalised partition keys ({@link #statsUnderTheSamePartition}).
   */
  private Membership lookupAgainstActive(String partition, String fileGroupId, String fileName,
                                         HoodieInstant instant) {
    HoodieFileGroupId groupId = new HoodieFileGroupId(partition, fileGroupId);
    List<HoodieWriteStat> stats;
    try {
      stats = CommitWriteStats.getWriteStatsAddedForInstant(completedActive, groupId, instant);
    } catch (Exception ex) {
      // "I could not check" is not "nothing references this".
      BranchBeacon.hit(Branch.IMR_ACTIVE_STATS_UNREADABLE);
      LOG.warn("Could not read the commit metadata of {} for file group {} in partition {}",
          instant, fileGroupId, partition, ex);
      return Membership.INSTANT_NOT_FOUND;
    }
    if (!stats.isEmpty()) {
      return beacon(namesOfStats(stats).contains(fileName),
          Branch.IMR_ACTIVE_NAMED, Membership.COMMITTED,
          Branch.IMR_ACTIVE_NOT_NAMED, Membership.DANGLING);
    }

    // Zero stats is ambiguous: (a) the commit spells this partition differently (absolute path,
    // slash, scheme), (b) it does not record the partition, or (c) it records it but not this file
    // group. (a) would condemn every file in the partition, so both sides are normalised through
    // relativePartitionPath; with (a) removed, (b) and (c) are positive proof.
    Option<Map<String, List<HoodieWriteStat>>> byPartition = partitionToWriteStats(instant);
    if (!byPartition.isPresent() || byPartition.get().isEmpty()) {
      // Unreadable, no write stats, or no partitions: the instant never claimed anything, so refuse.
      BranchBeacon.hit(Branch.IMR_ACTIVE_NO_WRITE_STATS_AT_ALL);
      return Membership.INSTANT_NOT_FOUND;
    }
    List<HoodieWriteStat> underThisPartition =
        statsUnderTheSamePartition(byPartition.get(), partition);
    if (underThisPartition == null) {
      BranchBeacon.hit(Branch.IMR_ACTIVE_PARTITION_UNCLAIMED);
      return Membership.DANGLING;
    }
    List<HoodieWriteStat> forThisGroup = new ArrayList<>();
    for (HoodieWriteStat stat : underThisPartition) {
      if (fileGroupId.equals(stat.getFileId())) {
        forThisGroup.add(stat);
      }
    }
    if (forThisGroup.isEmpty()) {
      BranchBeacon.hit(Branch.IMR_ACTIVE_FILE_GROUP_UNCLAIMED);
      return Membership.DANGLING;
    }
    return beacon(namesOfStats(forThisGroup).contains(fileName),
        Branch.IMR_ACTIVE_RESPELLED_NAMED, Membership.COMMITTED,
        Branch.IMR_ACTIVE_RESPELLED_NOT_NAMED, Membership.DANGLING);
  }

  /** Records the branch taken; each answer is reached from three places. See {@link BranchBeacon}. */
  private static Membership beacon(boolean condition, Branch whenTrue, Membership ifTrue,
                                   Branch whenFalse, Membership ifFalse) {
    BranchBeacon.hit(condition ? whenTrue : whenFalse);
    return condition ? ifTrue : ifFalse;
  }

  /**
   * The commit's {@code partition -> write stats} map, or empty (a refusal) when it cannot be read or
   * the action records no write stats. Read only on the path that has already found nothing.
   */
  private Option<Map<String, List<HoodieWriteStat>>> partitionToWriteStats(HoodieInstant instant) {
    try {
      switch (instant.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          return Option.ofNullable(
              completedActive.readCommitMetadata(instant).getPartitionToWriteStats());
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
          return Option.ofNullable(
              completedActive.readReplaceCommitMetadata(instant).getPartitionToWriteStats());
        default:
          return Option.empty();
      }
    } catch (Exception ex) {
      LOG.warn("Could not read the commit metadata of {} to establish which partitions it records; "
          + "reporting the instant not found", instant, ex);
      return Option.empty();
    }
  }

  /**
   * Every write stat the commit recorded for {@code partition}, matched on normalised keys (merging
   * keys that normalise alike), or {@code null} when no key matches. {@code null} (no entry) and an
   * empty list (an empty entry) are kept distinct on purpose.
   */
  private List<HoodieWriteStat> statsUnderTheSamePartition(
      Map<String, List<HoodieWriteStat>> byPartition, String partition) {
    String wanted = relativePartitionPath(partition);
    List<HoodieWriteStat> recorded = null;
    for (Map.Entry<String, List<HoodieWriteStat>> entry : byPartition.entrySet()) {
      if (!wanted.equals(relativePartitionPath(entry.getKey()))) {
        continue;
      }
      if (recorded == null) {
        recorded = new ArrayList<>();
      }
      if (entry.getValue() != null) {
        recorded.addAll(entry.getValue());
      }
    }
    return recorded;
  }

  /**
   * A partition path relative to the table root, with no scheme, authority, or leading or trailing
   * slash, normalised by {@link FSUtils#getRelativePartitionPath}. Never throws: a string it cannot
   * place is returned trimmed, which is no worse than a raw-string comparison.
   */
  @VisibleForTesting
  String relativePartitionPath(String partition) {
    return PartitionPaths.relative(metaClient.getBasePath(), partition);
  }

  /** Answers an archive-bucket instant from the materialized working set; performs no read. */
  private Membership lookupAgainstArchive(String partition, String fileGroupId, String fileName,
                                          String instantTime) {
    if (archived == null) {
      // Fail-fast, or a batch with no archive bucket: no archive was read.
      BranchBeacon.hit(Branch.IMR_ARCHIVE_NOT_ALLOWED);
      return Membership.INSTANT_NOT_FOUND;
    }
    if (!archived.targets().contains(instantTime)) {
      // A routing bug, not an archive miss. stateOf throws for a non-target, so this check must come
      // first; its own branch keeps it distinguishable from IMR_ARCHIVE_INSTANT_UNRESOLVED.
      BranchBeacon.hit(Branch.IMR_ARCHIVE_INSTANT_NOT_A_TARGET);
      return Membership.INSTANT_NOT_FOUND;
    }
    if (archived.stateOf(instantTime) != InstantSearchState.FOUND) {
      // NOT_FOUND or INDETERMINATE: only a FOUND record backs a per-group name set.
      BranchBeacon.hit(Branch.IMR_ARCHIVE_INSTANT_UNRESOLVED);
      return Membership.INSTANT_NOT_FOUND;
    }
    ArchivedCommitPaths recorded =
        archived.contentOf(instantTime).orElse(ArchivedCommitPaths.empty());
    HoodieFileGroupId groupId = new HoodieFileGroupId(partition, fileGroupId);
    Set<String> named = recorded.namesOf(groupId);
    if (named.isEmpty()) {
      return absentFromArchivedCommit(recorded);
    }
    if (named.contains(fileName)) {
      BranchBeacon.hit(Branch.IMR_ARCHIVE_NAMED);
      return Membership.COMMITTED;
    }
    if (isCdcFile(fileName)) {
      // Pre-1.0 archived records lack cdcStats, the only place a CDC file is named (see class javadoc).
      BranchBeacon.hit(Branch.IMR_ARCHIVE_CDC_REFUSED);
      return Membership.INSTANT_NOT_FOUND;
    }
    BranchBeacon.hit(Branch.IMR_ARCHIVE_NOT_NAMED);
    return Membership.DANGLING;
  }

  /**
   * Answers a file group the archived instant names nothing for, from the completed commit's own
   * record: no write stats at all refuses, while write stats that name nothing for this group are
   * proof the commit did not write it.
   */
  private static Membership absentFromArchivedCommit(ArchivedCommitPaths recorded) {
    if (!recorded.isCompletedWithWriteStats()) {
      BranchBeacon.hit(Branch.IMR_ARCHIVE_RECORDED_NOTHING);
      return Membership.INSTANT_NOT_FOUND;
    }
    BranchBeacon.hit(Branch.IMR_ARCHIVE_GROUP_UNCLAIMED);
    return Membership.DANGLING;
  }

  /**
   * Whether {@code fileName} is a CDC log file, delegating to {@link FSUtils#isCDCLogFile} so this
   * cannot drift from Hudi's own definition.
   */
  private static boolean isCdcFile(String fileName) {
    return FSUtils.isCDCLogFile(fileName);
  }

  /** The bare names the stats recorded: each {@code path}, its delta log files, and its CDC files. */
  private static Set<String> namesOfStats(List<HoodieWriteStat> stats) {
    Set<String> names = new LinkedHashSet<>();
    for (HoodieWriteStat stat : stats) {
      addName(names, stat.getPath());
      Map<String, Long> cdcStats = stat.getCdcStats();
      if (cdcStats != null) {
        for (String cdcPath : cdcStats.keySet()) {
          addName(names, cdcPath);
        }
      }
      if (stat instanceof HoodieDeltaWriteStat) {
        List<String> logFiles = ((HoodieDeltaWriteStat) stat).getLogFiles();
        if (logFiles != null) {
          for (String logFile : logFiles) {
            addName(names, logFile);
          }
        }
      }
    }
    return names;
  }

  /** Bare names, never paths: suspects come from a listing, and commit metadata mixes path forms. */
  private static void addName(Set<String> names, String path) {
    if (path == null || path.isEmpty()) {
      return;
    }
    names.add(new StoragePath(path).getName());
  }

  private static String safeBasePath(HoodieTableMetaClient metaClient) {
    try {
      return String.valueOf(metaClient.getBasePath());
    } catch (Exception ex) {
      return "<unknown>";
    }
  }
}
