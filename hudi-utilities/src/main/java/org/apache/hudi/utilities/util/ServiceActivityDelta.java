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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <b>clean and rollback activity</b> that took place between {@code T_prev} and {@code T_aft},
 * pre-reduced on the driver so it is small enough to broadcast to executors. It answers one
 * question for {@link PendingServiceReconciler}: does a service that ran during this run account for
 * this file? Instant states come from {@link InstantStateIndex}, and services still pending at both
 * reads from {@link PendingServiceState#across}.
 *
 * <p>{@link #between} is the only storage access, so the reconciler does no IO on executors. Every
 * file name is reduced to its bare name because rollback and clean metadata mix absolute paths and
 * bare names, and an unnormalised form silently matches nothing. A plan that cannot be read is
 * recorded ({@link #readFailed()}, {@link #readFailedFor(String)}), never treated as "nothing
 * explains this file".
 */
public class ServiceActivityDelta implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ServiceActivityDelta.class);

  private final Map<String, Set<String>> rollbackFreed;
  private final Map<String, Set<String>> cleanFreed;
  private final Set<String> rolledBackInstants;
  private final Set<String> readFailurePartitions;
  private final boolean readFailed;
  private final boolean unattributedReadFailure;
  private final boolean requestedOnlyUnattributedReadFailure;

  private ServiceActivityDelta(Map<String, Set<String>> rollbackFreed,
                        Map<String, Set<String>> cleanFreed,
                        Set<String> rolledBackInstants,
                        Set<String> readFailurePartitions,
                        boolean readFailed,
                        boolean unattributedReadFailure,
                        boolean requestedOnlyUnattributedReadFailure) {
    this.rollbackFreed = rollbackFreed;
    this.cleanFreed = cleanFreed;
    this.rolledBackInstants = rolledBackInstants;
    this.readFailurePartitions = readFailurePartitions;
    this.readFailed = readFailed;
    this.unattributedReadFailure = unattributedReadFailure;
    this.requestedOnlyUnattributedReadFailure = requestedOnlyUnattributedReadFailure;
  }

  /**
   * Walks the aft active timeline once and reduces every rollback, restore and clean that completed
   * during the run (see {@link #completedDuringTheRun}) to per-partition bare file names. Pending
   * rollback plans are read regardless, so a rollback's target is known before it completes. Each
   * read is wrapped individually: a failure marks its partition, or the whole object when no
   * partition can be attributed, and the walk continues.
   */
  public static ServiceActivityDelta between(InstantStateIndex prev, HoodieTimeline aft, HoodieTableMetaClient metaClient) {
    Map<String, Set<String>> rollbackFreed = new HashMap<>();
    Map<String, Set<String>> cleanFreed = new HashMap<>();
    Set<String> rolledBackInstants = new HashSet<>();
    Set<String> readFailurePartitions = new HashSet<>();
    boolean readFailed = false;
    // A plan read that failed before any partition inside it was known.
    boolean unattributedReadFailure = false;
    // Weaker than unattributedReadFailure: a .requested-only plan may still be mid-write.
    boolean requestedOnlyUnattributedReadFailure = false;

    for (HoodieInstant instant : aft.getRollbackTimeline().filterInflightsAndRequested().getInstants()) {
      try {
        HoodieRollbackPlan plan = aft.readRollbackPlan(instant);
        if (plan.getInstantToRollback() != null) {
          rolledBackInstants.add(plan.getInstantToRollback().getCommitTime());
        }
      } catch (Exception e) {
        LOG.warn("Could not read rollback plan for instant {} of table {}", instant, metaClient.getBasePath(), e);
        readFailed = true;
        if (instant.getState() == HoodieInstant.State.REQUESTED) {
          requestedOnlyUnattributedReadFailure = true;
        } else {
          unattributedReadFailure = true;
        }
      }
    }

    for (HoodieInstant instant : aft.getRollbackTimeline().filterCompletedInstants().getInstants()) {
      if (!completedDuringTheRun(prev, instant)) {
        continue;
      }
      try {
        HoodieRollbackMetadata metadata = aft.readRollbackMetadata(instant);
        // Defensive: the schema says non-null, but an NPE here would be misreported as an unreadable
        // rollback by the catch below.
        if (metadata.getCommitsRollback() != null) {
          rolledBackInstants.addAll(metadata.getCommitsRollback());
        }
        if (!absorbRollbackPartitions(metadata.getPartitionMetadata(), rollbackFreed, readFailurePartitions)) {
          readFailed = true;
        }
      } catch (Exception e) {
        LOG.warn("Could not read rollback metadata for instant {} of table {}", instant, metaClient.getBasePath(), e);
        readFailed = true;
        unattributedReadFailure = true;
      }
    }

    for (HoodieInstant instant : aft.getRestoreTimeline().filterCompletedInstants().getInstants()) {
      if (!completedDuringTheRun(prev, instant)) {
        continue;
      }
      try {
        HoodieRestoreMetadata metadata = aft.readRestoreMetadata(instant);
        for (List<HoodieRollbackMetadata> rollbacks : metadata.getHoodieRestoreMetadata().values()) {
          if (rollbacks == null) {
            continue;
          }
          for (HoodieRollbackMetadata rollback : rollbacks) {
            if (rollback == null) {
              continue;
            }
            if (rollback.getCommitsRollback() != null) {
              rolledBackInstants.addAll(rollback.getCommitsRollback());
            }
            if (!absorbRollbackPartitions(rollback.getPartitionMetadata(), rollbackFreed, readFailurePartitions)) {
              readFailed = true;
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Could not read restore metadata for instant {} of table {}", instant, metaClient.getBasePath(), e);
        readFailed = true;
        unattributedReadFailure = true;
      }
    }

    for (HoodieInstant instant : aft.getCleanerTimeline().filterCompletedInstants().getInstants()) {
      if (!completedDuringTheRun(prev, instant)) {
        continue;
      }
      try {
        HoodieCleanMetadata metadata = aft.readCleanMetadata(instant);
        for (Map.Entry<String, HoodieCleanPartitionMetadata> entry : metadata.getPartitionMetadata().entrySet()) {
          String partition = entry.getKey();
          try {
            // Both delete sets. failedDeleteFiles sanctions an FS-exclusive survivor. successDeleteFiles
            // sanctions the MDT-exclusive case: a clean during the run deletes a file the T_prev MDT
            // view still names, and without it an undamaged table fails with OUTBOUND_DELETION.
            // deletePathPatterns is the union of the two for non-bootstrap files, so it adds nothing.
            Set<String> freed = new HashSet<>();
            addBareNames(freed, entry.getValue().getSuccessDeleteFiles());
            addBareNames(freed, entry.getValue().getFailedDeleteFiles());
            if (!freed.isEmpty()) {
              cleanFreed.computeIfAbsent(partition, p -> new HashSet<>()).addAll(freed);
            }
          } catch (Exception e) {
            LOG.warn("Could not normalise clean file names for partition {} at instant {} of table {}",
                partition, instant, metaClient.getBasePath(), e);
            readFailurePartitions.add(partition);
            readFailed = true;
          }
        }
      } catch (Exception e) {
        LOG.warn("Could not read clean metadata for instant {} of table {}", instant, metaClient.getBasePath(), e);
        readFailed = true;
        unattributedReadFailure = true;
      }
    }

    return new ServiceActivityDelta(rollbackFreed, cleanFreed, rolledBackInstants,
        readFailurePartitions, readFailed, unattributedReadFailure,
        requestedOnlyUnattributedReadFailure);
  }

  /**
   * Whether this aft-completed service instant was not yet completed at {@code T_prev}. A service
   * completed before {@code T_prev} is already reflected in the validated snapshot, so a file it
   * names is a genuine finding; absorbing the whole active window would sanction every orphan inside
   * the archival horizon.
   */
  private static boolean completedDuringTheRun(InstantStateIndex prev, HoodieInstant instant) {
    InstantStateIndex.Resolution atPrev = prev.resolveAtPrev(instant.requestedTime());
    return !atPrev.isResolved() || atPrev.getState() != InstantState.COMPLETED;
  }

  /**
   * Reduces one rollback's partition metadata to the bare names of {@code failedDeleteFiles},
   * {@code rollbackLogFiles} and {@code logFilesFromFailedCommit}. {@code successDeleteFiles} is
   * skipped: those files were written by an instant in {@link #rolledBackInstants()}, which already
   * sanctions them.
   *
   * @return false if any partition's file names could not be normalised; that partition is recorded
   *     in {@code readFailurePartitions} and the rest are still processed.
   */
  private static boolean absorbRollbackPartitions(Map<String, HoodieRollbackPartitionMetadata> partitionMetadata,
                                                    Map<String, Set<String>> rollbackFreed,
                                                    Set<String> readFailurePartitions) {
    boolean allOk = true;
    for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : partitionMetadata.entrySet()) {
      String partition = entry.getKey();
      try {
        HoodieRollbackPartitionMetadata partitionMd = entry.getValue();
        Set<String> freed = new HashSet<>();
        addBareNames(freed, partitionMd.getFailedDeleteFiles());
        addBareNames(freed, keysOf(partitionMd.getRollbackLogFiles()));
        addBareNames(freed, keysOf(partitionMd.getLogFilesFromFailedCommit()));
        if (!freed.isEmpty()) {
          rollbackFreed.computeIfAbsent(partition, p -> new HashSet<>()).addAll(freed);
        }
      } catch (Exception e) {
        LOG.warn("Could not normalise rollback file names for partition {}", partition, e);
        readFailurePartitions.add(partition);
        allOk = false;
      }
    }
    return allOk;
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

  /** True when no service touched the table during the run and every plan read succeeded. */
  public boolean noServiceActivity() {
    return !readFailed && rollbackFreed.isEmpty() && cleanFreed.isEmpty()
        && rolledBackInstants.isEmpty();
  }

  /** Every instant time named by a completed or in-flight rollback/restore during the run. */
  public Set<String> rolledBackInstants() {
    return Collections.unmodifiableSet(rolledBackInstants);
  }

  /** Bare file names freed by a rollback during the run, for one partition. */
  public Set<String> fileNamesFreedByRollback(String partition) {
    return rollbackFreed.getOrDefault(partition, Collections.emptySet());
  }

  /** Bare file names freed by a clean during the run, for one partition. */
  public Set<String> fileNamesFreedByClean(String partition) {
    return cleanFreed.getOrDefault(partition, Collections.emptySet());
  }

  /** True if any plan this object read, anywhere, failed to read. */
  public boolean readFailed() {
    return readFailed;
  }

  /**
   * True if a read failed for this partition, or failed before any partition could be identified
   * (unknown scope counts against every partition). Failures attributed to other partitions do not.
   */
  public boolean readFailedFor(String partition) {
    return unattributedReadFailure || readFailurePartitions.contains(partition);
  }

  /**
   * True if a requested-only rollback plan failed to read. Not partition-scoped, and weaker evidence
   * than {@link #readFailedFor} (see {@link PendingServiceState#hasRequestedOnlyPlanReadFailure()}).
   */
  public boolean hasRequestedOnlyPlanReadFailure() {
    return requestedOnlyUnattributedReadFailure;
  }

  /** A builder for tests that need no real table on disk. */
  static Builder forTesting() {
    return new Builder();
  }

  static final class Builder {


    private final Map<String, Set<String>> rollbackFreed = new HashMap<>();
    private final Map<String, Set<String>> cleanFreed = new HashMap<>();
    private final Set<String> rolledBackInstants = new HashSet<>();
    private final Set<String> readFailurePartitions = new HashSet<>();

    Builder withRollbackFreedFile(String partition, String path) {
      rollbackFreed.computeIfAbsent(partition, p -> new HashSet<>()).add(new StoragePath(path).getName());
      return this;
    }

    Builder withCleanFreedFile(String partition, String path) {
      cleanFreed.computeIfAbsent(partition, p -> new HashSet<>()).add(new StoragePath(path).getName());
      return this;
    }

    Builder withReadFailure(String partition) {
      readFailurePartitions.add(partition);
      return this;
    }

    Builder withRolledBackInstant(String instantTime) {
      rolledBackInstants.add(instantTime);
      return this;
    }

    ServiceActivityDelta build() {
      return new ServiceActivityDelta(rollbackFreed, cleanFreed, rolledBackInstants,
          readFailurePartitions, !readFailurePartitions.isEmpty(), false, false);
    }
  }
}
