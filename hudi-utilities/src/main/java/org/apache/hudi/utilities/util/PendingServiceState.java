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

import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a clean or rollback already <b>requested or inflight</b> at {@code T_prev} had already
 * planned to delete, pre-reduced on the driver to per-partition bare file names so this is small
 * enough to broadcast to executors.
 *
 * <p><b>Why this exists alongside {@link ServiceActivityDelta}.</b> A clean that was already inflight at
 * {@code T_prev} and is still inflight at {@code T_aft} contributes nothing to a state diff -- its
 * state never changed -- while having already deleted files the metadata table still names. {@link
 * ServiceActivityDelta} only ever reports what *changed* between the two timelines; it cannot describe a
 * service that was already running before the window {@code between()} looks at. This class covers
 * that gap by reducing every clean and rollback plan on the active timeline at {@code T_prev}
 * <em>once</em>, so the reconciler never has to re-read a plan that has not changed just because the
 * service it belongs to is still pending.
 *
 * <p><b>No timeline or metaclient handle survives construction.</b> {@link #from} is the only place
 * this class ever touches storage.
 *
 * <p><b>Bare names, always.</b> As with {@link ServiceActivityDelta}, clean and rollback plans mix absolute
 * paths and bare names depending on which code wrote the entry. Every file name stored here has been
 * passed through {@code new StoragePath(path).getName()}.
 *
 * <p><b>A read failure is never a clean bill of health.</b> When a plan cannot be read, that fact is
 * recorded per partition via {@link #readFailedFor(String)} rather than silently treated as "nothing
 * is pending here". Every read is wrapped individually so that one bad plan does not abort the rest
 * of the walk.
 *
 * <p><b>Except a {@code .requested}-only read failure, which is weaker evidence on purpose.</b> A
 * clean or rollback whose plan has not yet reached {@code .inflight} may simply be mid-write, so a
 * failure to read it at that point is recorded separately via
 * {@link #hasRequestedOnlyPlanReadFailure()} and never escalated to {@link #readFailedFor(String)}.
 * {@link PendingServiceReconciler#reconcile} treats the two differently: a hard failure refuses
 * ({@code UNRESOLVED}), a requested-only one sanctions the mismatch it might explain.
 */
public class PendingServiceState implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PendingServiceState.class);

  private final Map<String, Set<String>> cleanPlannedFiles;
  private final Map<String, Set<String>> rollbackPlannedFiles;
  private final Set<String> plannedRollbackInstants;
  private final Set<String> readFailurePartitions;
  private final boolean unattributedReadFailure;
  private final boolean requestedOnlyUnattributedReadFailure;

  private PendingServiceState(Map<String, Set<String>> cleanPlannedFiles,
                              Map<String, Set<String>> rollbackPlannedFiles,
                              Set<String> plannedRollbackInstants,
                              Set<String> readFailurePartitions,
                              boolean unattributedReadFailure,
                              boolean requestedOnlyUnattributedReadFailure) {
    this.cleanPlannedFiles = cleanPlannedFiles;
    this.rollbackPlannedFiles = rollbackPlannedFiles;
    this.plannedRollbackInstants = plannedRollbackInstants;
    this.readFailurePartitions = readFailurePartitions;
    this.unattributedReadFailure = unattributedReadFailure;
    this.requestedOnlyUnattributedReadFailure = requestedOnlyUnattributedReadFailure;
  }

  /** No requested or inflight clean/rollback plan anywhere -- for tables with no pending service. */
  public static PendingServiceState empty() {
    return new PendingServiceState(Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptySet(), Collections.emptySet(), false, false);
  }

  /**
   * The pending clean and rollback plans across <b>both</b> of the run's timeline reads.
   *
   * <p><b>Both reads, because either one alone misses a live service.</b> A clean or rollback
   * pending at {@code T_prev} may have deleted files before the metadata snapshot was even taken,
   * and if it is still pending at {@code T_aft} it contributes nothing to any state diff, so
   * {@link ServiceActivityDelta} cannot see it — that is the case this class was written for. But the
   * mirror is just as real and was not covered: a service <em>scheduled during the run</em> is
   * absent from {@code T_prev} entirely and is still pending at {@code T_aft}, so it is in neither
   * the prev walk nor {@code dlt}'s completed loops. Its plan names files it is deleting right now.
   * Walking both reads is what closes that, and it is why this takes two timelines rather than one.
   *
   * <p><b>Nothing historical is read.</b> Both walks are {@code filterInflightsAndRequested()}: a
   * service that had already completed before this run began is not pending at either read and its
   * plan is never opened. Reducing every clean and rollback the table has ever performed would be
   * an unbounded read against storage for evidence that cannot bear on a file this run just
   * observed. Between this and {@link ServiceActivityDelta#changedDuringTheRun}, which bounds the
   * completed half the same way, the reconciliation's whole population is the run's own delta.
   *
   * <p><b>A service pending at both reads is read once.</b> The aft walk skips every requested time
   * the prev walk already covered, so the common case — a long-running clean, pending throughout —
   * costs one plan read, not two. Each plan is read through the timeline it was enumerated from,
   * never through the other one.
   *
   * @param prev the timeline the metadata snapshot was pinned to
   * @param aft  the timeline re-read after phase 1 was forced to completion
   */
  public static PendingServiceState across(HoodieTimeline prev, HoodieTimeline aft,
                                           HoodieTableMetaClient metaClient) {
    PendingServiceState atPrev = from(prev, metaClient, Collections.emptySet());
    return atPrev.mergedWith(from(aft, metaClient, pendingRequestedTimes(prev)));
  }

  /**
   * The pending clean and rollback plans on one timeline.
   *
   * <p>Retained for callers that genuinely have one read to work with (and for tests). A run has
   * two and must use {@link #across}.
   */
  public static PendingServiceState from(HoodieTimeline activeTimeline, HoodieTableMetaClient metaClient) {
    return from(activeTimeline, metaClient, Collections.emptySet());
  }

  /** Requested times of every pending clean and rollback on {@code timeline}. */
  private static Set<String> pendingRequestedTimes(HoodieTimeline timeline) {
    Set<String> times = new HashSet<>();
    for (HoodieInstant instant : timeline.getCleanerTimeline().filterInflightsAndRequested().getInstants()) {
      times.add(instant.requestedTime());
    }
    for (HoodieInstant instant : timeline.getRollbackTimeline().filterInflightsAndRequested().getInstants()) {
      times.add(instant.requestedTime());
    }
    return times;
  }

  /**
   * Everything both walks found. Union throughout, and the read-failure flags are OR-ed: a plan one
   * walk could not read stays unreadable however well the other walk fared, because the two walks
   * cover different services rather than the same service twice.
   */
  private PendingServiceState mergedWith(PendingServiceState other) {
    Map<String, Set<String>> cleans = new HashMap<>(cleanPlannedFiles);
    other.cleanPlannedFiles.forEach(
        (partition, files) -> cleans.computeIfAbsent(partition, p -> new HashSet<>()).addAll(files));
    Map<String, Set<String>> rollbacks = new HashMap<>(rollbackPlannedFiles);
    other.rollbackPlannedFiles.forEach(
        (partition, files) -> rollbacks.computeIfAbsent(partition, p -> new HashSet<>()).addAll(files));
    Set<String> instants = new HashSet<>(plannedRollbackInstants);
    instants.addAll(other.plannedRollbackInstants);
    Set<String> failures = new HashSet<>(readFailurePartitions);
    failures.addAll(other.readFailurePartitions);
    return new PendingServiceState(cleans, rollbacks, instants, failures,
        unattributedReadFailure || other.unattributedReadFailure,
        requestedOnlyUnattributedReadFailure || other.requestedOnlyUnattributedReadFailure);
  }

  /**
   * Walks the requested and inflight clean and rollback plans on {@code activeTimeline} once,
   * reducing each to per-partition bare file names (clean) or bare file names plus the rolled-back
   * instant (rollback). Every read is wrapped individually -- one that fails marks the affected
   * partition (or, when no partition can be attributed, only the object-wide read-failure fallback
   * used by {@link #readFailedFor(String)}) and the walk continues.
   *
   * @param alreadyRead requested times whose plan a previous walk has already reduced; skipped
   *                    before the plan is opened, so a service pending at both of the run's reads
   *                    costs one read rather than two
   */
  private static PendingServiceState from(HoodieTimeline activeTimeline,
                                          HoodieTableMetaClient metaClient,
                                          Set<String> alreadyRead) {
    Map<String, Set<String>> cleanPlannedFiles = new HashMap<>();
    Map<String, Set<String>> rollbackPlannedFiles = new HashMap<>();
    Set<String> plannedRollbackInstants = new HashSet<>();
    Set<String> readFailurePartitions = new HashSet<>();
    boolean unattributedReadFailure = false;
    // Set only when the failing instant is still .requested-only: the writer that will eventually
    // promote it to .inflight can still be mid-write, so this is a narrower, softer fact than
    // unattributedReadFailure -- see PendingServiceReconciler#reconcile.
    boolean requestedOnlyUnattributedReadFailure = false;

    for (HoodieInstant instant : notAlreadyRead(
        activeTimeline.getCleanerTimeline().filterInflightsAndRequested().getInstants(), alreadyRead)) {

      try {
        HoodieCleanerPlan plan = CleanerUtils.getCleanerPlan(metaClient, instant);
        Map<String, List<HoodieCleanFileInfo>> perPartition = plan.getFilePathsToBeDeletedPerPartition();
        if (perPartition == null) {
          continue;
        }
        for (Map.Entry<String, List<HoodieCleanFileInfo>> entry : perPartition.entrySet()) {
          String partition = entry.getKey();
          try {
            Set<String> bare = new HashSet<>();
            if (entry.getValue() != null) {
              for (HoodieCleanFileInfo fileInfo : entry.getValue()) {
                bare.add(new StoragePath(fileInfo.getFilePath()).getName());
              }
            }
            if (!bare.isEmpty()) {
              cleanPlannedFiles.computeIfAbsent(partition, p -> new HashSet<>()).addAll(bare);
            }
          } catch (Exception e) {
            LOG.warn("Could not normalise clean plan file names for partition {} at instant {} of table {}",
                partition, instant, metaClient.getBasePath(), e);
            readFailurePartitions.add(partition);
          }
        }
      } catch (Exception e) {
        LOG.warn("Could not read clean plan for instant {} of table {}", instant, metaClient.getBasePath(), e);
        if (instant.getState() == HoodieInstant.State.REQUESTED) {
          // A .clean.requested-only plan may simply not be fully flushed yet.
          requestedOnlyUnattributedReadFailure = true;
        } else {
          unattributedReadFailure = true;
        }
      }
    }

    for (HoodieInstant instant : notAlreadyRead(
        activeTimeline.getRollbackTimeline().filterInflightsAndRequested().getInstants(), alreadyRead)) {
      try {
        // The plan lives only in the .rollback.requested file: transitionRollbackRequestedToInflight
        // writes .rollback.inflight with no content (ActiveTimelineV2#transitionRollbackRequestedToInflight
        // passes Option.empty()), and readNonEmptyInstantContent throws on empty/missing content. An
        // inflight rollback is routine, not corrupt, so it must be resolved to its requested-state
        // instant before reading -- mirroring RollbackUtils#getRollbackPlan and the clean path above
        // (CleanerUtils#getCleanRequestInstant), not read directly off whatever state was enumerated.
        HoodieInstant requestedInstant = metaClient.getInstantGenerator().getRollbackRequestedInstant(instant);
        HoodieRollbackPlan plan = activeTimeline.readRollbackPlan(requestedInstant);
        if (plan.getInstantToRollback() != null) {
          plannedRollbackInstants.add(plan.getInstantToRollback().getCommitTime());
        }
        List<HoodieRollbackRequest> requests = plan.getRollbackRequests();
        if (requests == null) {
          continue;
        }
        for (HoodieRollbackRequest request : requests) {
          String partition = request.getPartitionPath();
          try {
            Set<String> bare = new HashSet<>();
            addBareNames(bare, request.getFilesToBeDeleted());
            addBareNames(bare, keysOf(request.getLogBlocksToBeDeleted()));
            if (!bare.isEmpty()) {
              rollbackPlannedFiles.computeIfAbsent(partition, p -> new HashSet<>()).addAll(bare);
            }
          } catch (Exception e) {
            LOG.warn("Could not normalise rollback plan file names for partition {} at instant {} of table {}",
                partition, instant, metaClient.getBasePath(), e);
            readFailurePartitions.add(partition);
          }
        }
      } catch (Exception e) {
        LOG.warn("Could not read rollback plan for instant {} of table {}", instant, metaClient.getBasePath(), e);
        if (instant.getState() == HoodieInstant.State.REQUESTED) {
          // A .rollback.requested-only plan may simply not be fully flushed yet.
          requestedOnlyUnattributedReadFailure = true;
        } else {
          unattributedReadFailure = true;
        }
      }
    }

    return new PendingServiceState(cleanPlannedFiles, rollbackPlannedFiles, plannedRollbackInstants,
        readFailurePartitions, unattributedReadFailure, requestedOnlyUnattributedReadFailure);
  }

  /** {@code instants} minus those a previous walk already reduced. */
  private static List<HoodieInstant> notAlreadyRead(List<HoodieInstant> instants,
                                                    Set<String> alreadyRead) {
    if (alreadyRead.isEmpty()) {
      return instants;
    }
    List<HoodieInstant> remaining = new ArrayList<>(instants.size());
    for (HoodieInstant instant : instants) {
      if (!alreadyRead.contains(instant.requestedTime())) {
        remaining.add(instant);
      }
    }
    return remaining;
  }

  private static Collection<String> keysOf(Map<String, Long> map) {
    return map == null ? Collections.emptySet() : map.keySet();
  }

  private static void addBareNames(Set<String> target, Collection<String> paths) {
    if (paths == null) {
      return;
    }
    for (String path : paths) {
      target.add(new StoragePath(path).getName());
    }
  }

  /** Bare file names a requested or inflight clean plan at {@code T_prev} had already planned to delete. */
  public Set<String> cleanPlannedFiles(String partition) {
    return cleanPlannedFiles.getOrDefault(partition, Collections.emptySet());
  }

  /** Bare file names a requested or inflight rollback plan at {@code T_prev} had already planned to delete. */
  public Set<String> rollbackPlannedFiles(String partition) {
    return rollbackPlannedFiles.getOrDefault(partition, Collections.emptySet());
  }

  /** Every instant time a requested or inflight rollback plan at {@code T_prev} names as its target. */
  public Set<String> plannedRollbackInstants() {
    return Collections.unmodifiableSet(plannedRollbackInstants);
  }

  /**
   * True if a plan covering this specific partition is known to have failed to read, OR if some plan
   * failed to read before any partition inside it could be identified -- such a failure's true scope
   * is unknown, so it must never be reported as "this partition is fine" for any partition asked
   * about. A failure already attributed to a different, specific partition does not count.
   */
  public boolean readFailedFor(String partition) {
    return unattributedReadFailure || readFailurePartitions.contains(partition);
  }

  /**
   * True if a requested-only clean or rollback plan failed to read, and no harder failure (see
   * {@link #readFailedFor}) is known anywhere. Not partition-scoped, for the same reason the
   * unattributed half of {@link #readFailedFor} is not: which partition a plan that never parsed
   * would have covered is unknown. A {@code .requested}-only plan may simply not be fully flushed
   * yet, which is why {@link PendingServiceReconciler#reconcile} treats this as weaker evidence than
   * a hard failure, never stronger.
   */
  public boolean hasRequestedOnlyPlanReadFailure() {
    return requestedOnlyUnattributedReadFailure;
  }

  /** A builder for tests that need no real table on disk. */
  static Builder forTesting() {
    return new Builder();
  }

  static final class Builder {

    private final Map<String, Set<String>> cleanPlannedFiles = new HashMap<>();
    private final Map<String, Set<String>> rollbackPlannedFiles = new HashMap<>();
    private final Set<String> plannedRollbackInstants = new HashSet<>();
    private final Set<String> readFailurePartitions = new HashSet<>();

    Builder withCleanPlannedFile(String partition, String path) {
      cleanPlannedFiles.computeIfAbsent(partition, p -> new HashSet<>()).add(new StoragePath(path).getName());
      return this;
    }

    Builder withRollbackPlannedFile(String partition, String path) {
      rollbackPlannedFiles.computeIfAbsent(partition, p -> new HashSet<>()).add(new StoragePath(path).getName());
      return this;
    }

    Builder withPlannedRollbackInstant(String instantTime) {
      plannedRollbackInstants.add(instantTime);
      return this;
    }

    Builder withReadFailure(String partition) {
      readFailurePartitions.add(partition);
      return this;
    }

    PendingServiceState build() {
      return new PendingServiceState(cleanPlannedFiles, rollbackPlannedFiles, plannedRollbackInstants,
          readFailurePartitions, false, false);
    }
  }
}
