/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.repair;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * Utils for table repair tool.
 */
public final class RepairUtils {
  /**
   * Tags the instant time of each base or log file from the input file paths.
   *
   * @param basePath Base path of the table.
   * @param allPaths A {@link List} of file paths to tag.
   * @return A {@link Map} of instant time in {@link String} to a {@link List} of relative file paths.
   */
  public static Map<String, List<String>> tagInstantsOfBaseAndLogFiles(
      String basePath, List<StoragePath> allPaths) {
    // Instant time -> Set of base and log file paths
    Map<String, List<String>> instantToFilesMap = new HashMap<>();
    allPaths.forEach(path -> {
      String instantTime = FSUtils.getCommitTime(path.getName());
      instantToFilesMap.computeIfAbsent(instantTime, k -> new ArrayList<>());
      instantToFilesMap.get(instantTime).add(
          FSUtils.getRelativePartitionPath(new StoragePath(basePath), path));
    });
    return instantToFilesMap;
  }

  /**
   * Gets the base and log file paths written for a given instant from the timeline.
   * This reads the details of the instant metadata.
   *
   * @param timeline {@link HoodieTimeline} instance, can be active or archived timeline.
   * @param instant  Instant for lookup.
   * @return A {@link Option} of {@link Set} of relative file paths to base path
   * if the instant action is supported; empty {@link Option} otherwise.
   * @throws IOException if reading instant details fail.
   */
  public static Option<Set<String>> getBaseAndLogFilePathsFromTimeline(
      HoodieTimeline timeline, HoodieInstant instant) throws IOException {
    if (!instant.isCompleted()) {
      throw new HoodieException("Cannot get base and log file paths from "
          + "instant not completed: " + instant.getTimestamp());
    }

    switch (instant.getAction()) {
      case COMMIT_ACTION:
      case DELTA_COMMIT_ACTION:
        final HoodieCommitMetadata commitMetadata =
            HoodieCommitMetadata.fromBytes(
                timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        return Option.of(commitMetadata.getPartitionToWriteStats().values().stream().flatMap(List::stream)
            .map(HoodieWriteStat::getPath).collect(Collectors.toSet()));
      case REPLACE_COMMIT_ACTION:
        final HoodieReplaceCommitMetadata replaceCommitMetadata =
            HoodieReplaceCommitMetadata.fromBytes(
                timeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
        return Option.of(replaceCommitMetadata.getPartitionToWriteStats().values().stream().flatMap(List::stream)
            .map(HoodieWriteStat::getPath).collect(Collectors.toSet()));
      default:
        return Option.empty();
    }
  }

  /**
   * Finds the dangling files to remove for a given instant to repair.
   *
   * @param instantToRepair       Instant timestamp to repair.
   * @param baseAndLogFilesFromFs A {@link List} of base and log files based on the file system.
   * @param activeTimeline        {@link HoodieActiveTimeline} instance.
   * @param archivedTimeline      {@link HoodieArchivedTimeline} instance.
   * @return A {@link List} of relative file paths to base path for removing.
   */
  public static List<String> findInstantFilesToRemove(
      String instantToRepair, List<String> baseAndLogFilesFromFs,
      HoodieActiveTimeline activeTimeline, HoodieArchivedTimeline archivedTimeline) {
    // Skips the instant if it is requested or inflight in active timeline
    if (!activeTimeline.filter(instant -> instant.getTimestamp().equals(instantToRepair)
        && !instant.isCompleted()).empty()) {
      return Collections.emptyList();
    }

    try {
      boolean doesInstantExist = false;
      Option<Set<String>> filesFromTimeline = Option.empty();
      Option<HoodieInstant> instantOption = activeTimeline.filterCompletedInstants().filter(
          instant -> instant.getTimestamp().equals(instantToRepair)).firstInstant();
      if (instantOption.isPresent()) {
        // Completed instant in active timeline
        doesInstantExist = true;
        filesFromTimeline = RepairUtils.getBaseAndLogFilePathsFromTimeline(
            activeTimeline, instantOption.get());
      } else {
        instantOption = archivedTimeline.filterCompletedInstants().filter(
            instant -> instant.getTimestamp().equals(instantToRepair)).firstInstant();
        if (instantOption.isPresent()) {
          // Completed instant in archived timeline
          doesInstantExist = true;
          filesFromTimeline = RepairUtils.getBaseAndLogFilePathsFromTimeline(
              archivedTimeline, instantOption.get());
        }
      }

      if (doesInstantExist) {
        if (!filesFromTimeline.isPresent() || filesFromTimeline.get().isEmpty()) {
          // Skips if no instant details
          return Collections.emptyList();
        }
        // Excludes committed base and log files from timeline
        Set<String> filesToRemove = new HashSet<>(baseAndLogFilesFromFs);
        filesToRemove.removeAll(filesFromTimeline.get());
        return new ArrayList<>(filesToRemove);
      } else {
        // The instant does not exist in the whole timeline (neither completed nor requested/inflight),
        // this means the files from this instant are dangling, which should be removed
        return baseAndLogFilesFromFs;
      }
    } catch (IOException e) {
      // In case of failure, does not remove any files for the instant
      return Collections.emptyList();
    }
  }

  /**
   * Serializable path filter class for Spark job.
   */
  public interface SerializablePathFilter extends PathFilter, Serializable {
  }
}
