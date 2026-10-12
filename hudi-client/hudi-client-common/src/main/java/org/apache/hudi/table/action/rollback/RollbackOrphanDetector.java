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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects parquet/log files left behind by a rollback that did not fully complete.
 *
 * <p>This is the building block used by the archive precondition (see
 * issue #18783) and the {@code hudi-cli} repair tool. It does not delete
 * anything; callers decide what to do with the returned paths.
 *
 * <p>Two detection modes are supported:
 * <ul>
 *   <li>{@link Mode#LIGHT} — reads {@code HoodieRollbackMetadata} and returns
 *       paths recorded in {@code failedDeleteFiles}. O(metadata size). Catches
 *       files the rollback explicitly tried and failed to delete but misses
 *       files that landed on storage <em>after</em> the rollback completed
 *       (for example, a blocked storage {@code close()} returning late).</li>
 *   <li>{@link Mode#THOROUGH} — additionally lists the partitions named in the
 *       rollback metadata and matches filenames against the rollback's target
 *       instant time(s). Catches post-rollback late landings. Bounded by the
 *       partition count in the rollback metadata, not whole-table size.</li>
 * </ul>
 *
 * <p>A safety floor cross-checks every candidate against the completed
 * timeline: a file whose embedded instant appears as a {@code COMPLETED}
 * commit in either the active or archived timeline is never flagged as an
 * orphan, even if its filename matches the regex.
 */
public class RollbackOrphanDetector {

  private static final Logger LOG = LoggerFactory.getLogger(RollbackOrphanDetector.class);

  /** Matches Hudi base/log filenames carrying an embedded instant time at the trailing position. */
  private static final String INSTANT_REGEX_BASE = "_(\\d+)\\.parquet$";
  private static final String INSTANT_REGEX_LOG = "\\.log\\.\\d+_(\\d+)(?:_[^/]*)?$";

  /** Detection mode controlling how aggressively we look for orphans. */
  public enum Mode {
    OFF,
    LIGHT,
    THOROUGH;

    public static Mode parse(String value) {
      if (value == null || value.isEmpty()) {
        return OFF;
      }
      try {
        return Mode.valueOf(value.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        LOG.warn("Unknown RollbackOrphanDetector mode '{}'; falling back to OFF", value);
        return OFF;
      }
    }
  }

  /**
   * Returns true iff any orphan files are detected for {@code rollbackInstant}.
   * Optimised to short-circuit as soon as the first orphan is found in
   * {@link Mode#THOROUGH}.
   */
  public boolean hasOrphans(HoodieTable<?, ?, ?, ?> table,
                            HoodieInstant rollbackInstant,
                            Mode mode) throws IOException {
    return hasOrphans(table.getMetaClient(), rollbackInstant, mode);
  }

  /** Overload usable from contexts that only have a metaClient (e.g. {@code hudi-cli}). */
  public boolean hasOrphans(HoodieTableMetaClient metaClient,
                            HoodieInstant rollbackInstant,
                            Mode mode) throws IOException {
    if (mode == Mode.OFF) {
      return false;
    }
    return !detectOrphans(metaClient, rollbackInstant, mode).isEmpty();
  }

  /**
   * Returns the set of orphan file paths (absolute) still on storage for
   * {@code rollbackInstant}. Empty if no orphans or {@code mode == OFF}.
   */
  public Set<String> detectOrphans(HoodieTable<?, ?, ?, ?> table,
                                   HoodieInstant rollbackInstant,
                                   Mode mode) throws IOException {
    return detectOrphans(table.getMetaClient(), rollbackInstant, mode);
  }

  /** Overload usable from contexts that only have a metaClient (e.g. {@code hudi-cli}). */
  public Set<String> detectOrphans(HoodieTableMetaClient metaClient,
                                   HoodieInstant rollbackInstant,
                                   Mode mode) throws IOException {
    if (mode == Mode.OFF) {
      return Collections.emptySet();
    }
    if (!rollbackInstant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)
        || !rollbackInstant.isCompleted()) {
      return Collections.emptySet();
    }

    HoodieRollbackMetadata metadata =
        metaClient.getActiveTimeline().readRollbackMetadata(rollbackInstant);
    Set<String> orphans = new LinkedHashSet<>(collectFailedDeleteFiles(metadata));

    if (mode == Mode.LIGHT) {
      return filterAgainstCompletedTimeline(metaClient, orphans, extractTargetInstants(metadata));
    }

    Set<String> targetInstants = extractTargetInstants(metadata);
    if (targetInstants.isEmpty() || metadata.getPartitionMetadata() == null) {
      return filterAgainstCompletedTimeline(metaClient, orphans, targetInstants);
    }
    Pattern[] patterns = buildPatterns(targetInstants);
    HoodieStorage storage = metaClient.getStorage();
    StoragePath basePath = metaClient.getBasePath();
    for (String partitionPath : metadata.getPartitionMetadata().keySet()) {
      orphans.addAll(scanPartition(storage, basePath, partitionPath, patterns));
    }
    return filterAgainstCompletedTimeline(metaClient, orphans, targetInstants);
  }

  private static Set<String> collectFailedDeleteFiles(HoodieRollbackMetadata metadata) {
    if (metadata == null || metadata.getPartitionMetadata() == null) {
      return Collections.emptySet();
    }
    Set<String> failed = new LinkedHashSet<>();
    for (HoodieRollbackPartitionMetadata pm : metadata.getPartitionMetadata().values()) {
      List<String> ff = pm.getFailedDeleteFiles();
      if (ff != null) {
        failed.addAll(ff);
      }
    }
    return failed;
  }

  /** Returns target-instant times from either the typed or legacy field. */
  static Set<String> extractTargetInstants(HoodieRollbackMetadata metadata) {
    if (metadata == null) {
      return Collections.emptySet();
    }
    Set<String> out = new HashSet<>();
    List<HoodieInstantInfo> typed = metadata.getInstantsRollback();
    if (typed != null) {
      for (HoodieInstantInfo info : typed) {
        if (info != null && info.getCommitTime() != null) {
          out.add(info.getCommitTime());
        }
      }
    }
    List<String> legacy = metadata.getCommitsRollback();
    if (legacy != null) {
      out.addAll(legacy);
    }
    return out;
  }

  private static Pattern[] buildPatterns(Set<String> targetInstants) {
    String alt = String.join("|", targetInstants);
    return new Pattern[] {
        Pattern.compile("_(" + alt + ")\\.parquet$"),
        Pattern.compile("\\.log\\.\\d+_(" + alt + ")(?:_[^/]*)?$")
    };
  }

  private static Set<String> scanPartition(HoodieStorage storage,
                                           StoragePath basePath,
                                           String partitionPath,
                                           Pattern[] patterns) throws IOException {
    StoragePath dir = partitionPath == null || partitionPath.isEmpty()
        ? basePath
        : new StoragePath(basePath, partitionPath);
    if (!storage.exists(dir)) {
      return Collections.emptySet();
    }
    Set<String> hits = new LinkedHashSet<>();
    for (StoragePathInfo info : storage.listDirectEntries(dir)) {
      if (!info.isFile()) {
        continue;
      }
      String name = info.getPath().getName();
      for (Pattern p : patterns) {
        if (p.matcher(name).find()) {
          hits.add(info.getPath().toString());
          break;
        }
      }
    }
    return hits;
  }

  /**
   * Safety floor: removes any candidate whose embedded instant time matches a
   * COMPLETED commit anywhere in the active or archived timeline. Prevents
   * false-positive flagging of legitimate committed files that happen to
   * share a filename pattern.
   */
  private static Set<String> filterAgainstCompletedTimeline(HoodieTableMetaClient metaClient,
                                                            Set<String> candidates,
                                                            Set<String> targetInstants) {
    if (candidates.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> completedInstants = new HashSet<>();
    metaClient.getActiveTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::requestedTime).forEach(completedInstants::add);
    // Archived timeline lookup is intentionally not loaded here — it can be
    // expensive on tables with long history. Callers needing a stricter
    // check can extend this to also consult the archived timeline.
    Set<String> filtered = new LinkedHashSet<>();
    for (String path : candidates) {
      String embedded = extractEmbeddedInstant(path);
      if (embedded != null && completedInstants.contains(embedded)) {
        // Legitimate committed file — never flag.
        continue;
      }
      if (embedded != null && !targetInstants.isEmpty() && !targetInstants.contains(embedded)) {
        // Embedded instant is not one of this rollback's targets — out of scope.
        continue;
      }
      filtered.add(path);
    }
    return filtered;
  }

  private static String extractEmbeddedInstant(String path) {
    String name = path.substring(path.lastIndexOf('/') + 1);
    java.util.regex.Matcher m = Pattern.compile(INSTANT_REGEX_BASE).matcher(name);
    if (m.find()) {
      return m.group(1);
    }
    m = Pattern.compile(INSTANT_REGEX_LOG).matcher(name);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }
}
