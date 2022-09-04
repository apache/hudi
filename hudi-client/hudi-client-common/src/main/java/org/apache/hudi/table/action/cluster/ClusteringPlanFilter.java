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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ClusteringPlanFilter {

  public static List<FileSlice> filter(List<FileSlice> fileSlices, Option<HoodieDefaultTimeline> activeTimeline,
                                       ClusteringPlanFilterMode filterMode) {
    switch (filterMode) {
      case NONE:
        return fileSlices;
      case RECENTLY_INSERTED_FILES:
        return recentlyAlteredFilter(fileSlices, activeTimeline, true, filterMode);
      case RECENTLY_UPDATED_FILES:
        return recentlyAlteredFilter(fileSlices, activeTimeline, false, filterMode);
      default:
        throw new HoodieClusteringException("Unknown filter, filter mode: " + filterMode);
    }
  }

  private static List<FileSlice> recentlyAlteredFilter(List<FileSlice> fileSlices, Option<HoodieDefaultTimeline> activeTimeline,
                                                       boolean filterForFilesWithInserts, ClusteringPlanFilterMode filterMode) {
    List<String> fileIdsOfInterest = new ArrayList<>();
    // if active timeline is set, we need to trim for fileIds that was changed as part of the timeline.
    if (filterMode != ClusteringPlanFilterMode.NONE) {
      List<HoodieInstant> instants = activeTimeline.get().getCommitsTimeline().filterCompletedInstants().getInstants().collect(Collectors.toList());
      instants.forEach(instant -> {
        try {
          fileIdsOfInterest.addAll(getFileIdsForCommit(instant, activeTimeline.get(), filterForFilesWithInserts));
        } catch (IOException e) {
          throw new HoodieIOException("Failed to filter recent files for clustering ", e);
        }
      });
    }
    return fileIdsOfInterest.isEmpty() ? fileSlices
        : fileSlices.stream().filter(fileSlice -> fileIdsOfInterest.contains(fileSlice.getFileId())).collect(Collectors.toList());
  }

  private static List<String> getFileIdsForCommit(HoodieInstant instant, HoodieDefaultTimeline activeTimeline, boolean filterForFilesWithInserts) throws IOException {
    List<String> fileIdsOfInterest = new ArrayList<>();
    switch (instant.getAction()) {
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata
            .fromBytes(activeTimeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
        replaceCommitMetadata.getPartitionToWriteStats()
            .forEach((k, v) -> v.stream().filter(writeStat -> writeStat.getNumInserts() > 0).forEach(writeStat -> fileIdsOfInterest.add(writeStat.getFileId())));
        break;
      case HoodieTimeline.DELTA_COMMIT_ACTION:
      case HoodieTimeline.COMMIT_ACTION:
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(activeTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        commitMetadata.getPartitionToWriteStats().forEach((k, v) -> v.stream().filter(writeStat -> {
              if (filterForFilesWithInserts) {
                return writeStat.getNumInserts() > 0;
              } else {
                return writeStat.getNumWrites() > 0;
              }
            }
        ).forEach(writeStat -> fileIdsOfInterest.add(writeStat.getFileId())));
        break;
      default:
        throw new IllegalArgumentException("Unknown instant action" + instant.getAction());
    }
    return fileIdsOfInterest;
  }
}
