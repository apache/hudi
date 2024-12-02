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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.exception.HoodieStreamerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_RESET_KEY_V2;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.util.ConfigUtils.removeConfigFromProps;

public class StreamerCheckpointUtils {
  private static final Logger LOG = LoggerFactory.getLogger(StreamerCheckpointUtils.class);

  public static Option<Checkpoint> getCheckpointToResumeFrom(Option<HoodieTimeline> commitsTimelineOpt,
                                                             HoodieStreamer.Config streamerConfig,
                                                             TypedProperties props) throws IOException {
    Option<Checkpoint> checkpoint = Option.empty();
    if (commitsTimelineOpt.isPresent()) {
      checkpoint = getCheckpointToResumeString(commitsTimelineOpt, streamerConfig, props);
    }

    LOG.debug("Checkpoint from config: " + streamerConfig.checkpoint);
    if (!checkpoint.isPresent() && streamerConfig.checkpoint != null) {
      int writeTableVersion = ConfigUtils.getIntWithAltKeys(props, HoodieWriteConfig.WRITE_TABLE_VERSION);
      checkpoint = Option.of(CheckpointUtils.targetCheckpointV2(writeTableVersion)
          ? new StreamerCheckpointV2(streamerConfig.checkpoint) : new StreamerCheckpointV1(streamerConfig.checkpoint));
    }
    return checkpoint;
  }

  /**
   * Process previous commit metadata and checkpoint configs set by user to determine the checkpoint to resume from.
   *
   * @param commitsTimelineOpt commits timeline of interest, including .commit and .deltacommit.
   *
   * @return the checkpoint to resume from if applicable.
   * @throws IOException
   */
  @VisibleForTesting
  static Option<Checkpoint> getCheckpointToResumeString(Option<HoodieTimeline> commitsTimelineOpt,
                                                        HoodieStreamer.Config streamerConfig,
                                                        TypedProperties props) throws IOException {
    Option<Checkpoint> resumeCheckpoint = Option.empty();
    // try get checkpoint from commits(including commit and deltacommit)
    // in COW migrating to MOR case, the first batch of the deltastreamer will lost the checkpoint from COW table, cause the dataloss
    HoodieTimeline deltaCommitTimeline = commitsTimelineOpt.get().filter(instant -> instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));
    // has deltacommit and this is a MOR table, then we should get checkpoint from .deltacommit
    // if changing from mor to cow, before changing we must do a full compaction, so we can only consider .commit in such case
    if (streamerConfig.tableType.equals(HoodieTableType.MERGE_ON_READ.name()) && !deltaCommitTimeline.empty()) {
      commitsTimelineOpt = Option.of(deltaCommitTimeline);
    }
    Option<HoodieInstant> lastCommit = commitsTimelineOpt.get().lastInstant();
    if (lastCommit.isPresent()) {
      // if previous commit metadata did not have the checkpoint key, try traversing previous commits until we find one.
      Option<HoodieCommitMetadata> commitMetadataOption = getLatestCommitMetadataWithValidCheckpointInfo(commitsTimelineOpt.get());
      int writeTableVersion = ConfigUtils.getIntWithAltKeys(props, HoodieWriteConfig.WRITE_TABLE_VERSION);
      if (commitMetadataOption.isPresent()) {
        HoodieCommitMetadata commitMetadata = commitMetadataOption.get();
        Checkpoint checkpointFromCommit = CheckpointUtils.getCheckpoint(commitMetadata);
        LOG.debug("Checkpoint reset from metadata: " + checkpointFromCommit.getCheckpointResetKey());
        if (streamerConfig.ignoreCheckpoint != null && (StringUtils.isNullOrEmpty(checkpointFromCommit.getCheckpointIgnoreKey())
            || !streamerConfig.ignoreCheckpoint.equals(checkpointFromCommit.getCheckpointIgnoreKey()))) {
          // we ignore any existing checkpoint and start ingesting afresh
          resumeCheckpoint = Option.empty();
        } else if (streamerConfig.checkpoint != null && (StringUtils.isNullOrEmpty(checkpointFromCommit.getCheckpointResetKey())
            || !streamerConfig.checkpoint.equals(checkpointFromCommit.getCheckpointResetKey()))) {
          resumeCheckpoint = Option.of(CheckpointUtils.targetCheckpointV2(writeTableVersion)
              ? new StreamerCheckpointV2(streamerConfig.checkpoint) : new StreamerCheckpointV1(streamerConfig.checkpoint));
        } else if (!StringUtils.isNullOrEmpty(checkpointFromCommit.getCheckpointKey())) {
          //if previous checkpoint is an empty string, skip resume use Option.empty()
          resumeCheckpoint = Option.of(checkpointFromCommit);
        } else if (compareTimestamps(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
            LESSER_THAN, lastCommit.get().requestedTime())) {
          throw new HoodieStreamerException(
              "Unable to find previous checkpoint. Please double check if this table "
                  + "was indeed built via delta streamer. Last Commit :" + lastCommit + ", Instants :"
                  + commitsTimelineOpt.get().getInstants());
        }
        // KAFKA_CHECKPOINT_TYPE will be honored only for first batch.
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(HoodieStreamer.CHECKPOINT_RESET_KEY))) {
          removeConfigFromProps(props, KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE);
        }
      } else if (streamerConfig.checkpoint != null) {
        // getLatestCommitMetadataWithValidCheckpointInfo(commitTimelineOpt.get()) will never return a commit metadata w/o any checkpoint key set.
        resumeCheckpoint = Option.of(CheckpointUtils.targetCheckpointV2(writeTableVersion)
            ? new StreamerCheckpointV2(streamerConfig.checkpoint) : new StreamerCheckpointV1(streamerConfig.checkpoint));
      }
    }
    return resumeCheckpoint;
  }

  public static Option<Pair<String, HoodieCommitMetadata>> getLatestInstantAndCommitMetadataWithValidCheckpointInfo(HoodieTimeline timeline)
      throws IOException {
    return (Option<Pair<String, HoodieCommitMetadata>>) timeline.getReverseOrderedInstants().map(instant -> {
      try {
        TimelineLayout layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion());
        HoodieCommitMetadata commitMetadata = layout.getCommitMetadataSerDe()
            .deserialize(instant, timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(HoodieStreamer.CHECKPOINT_KEY))
            || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(HoodieStreamer.CHECKPOINT_RESET_KEY))
            || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(STREAMER_CHECKPOINT_KEY_V2))
            || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(STREAMER_CHECKPOINT_RESET_KEY_V2))) {
          return Option.of(Pair.of(instant.toString(), commitMetadata));
        } else {
          return Option.empty();
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse HoodieCommitMetadata for " + instant.toString(), e);
      }
    }).filter(Option::isPresent).findFirst().orElse(Option.empty());
  }

  public static Option<HoodieCommitMetadata> getLatestCommitMetadataWithValidCheckpointInfo(HoodieTimeline timeline) throws IOException {
    return getLatestInstantAndCommitMetadataWithValidCheckpointInfo(timeline).map(pair -> pair.getRight());
  }

  public static Option<String> getLatestInstantWithValidCheckpointInfo(Option<HoodieTimeline> timelineOpt) {
    return timelineOpt.map(timeline -> {
      try {
        return getLatestInstantAndCommitMetadataWithValidCheckpointInfo(timeline).map(pair -> pair.getLeft());
      } catch (IOException e) {
        throw new HoodieIOException("failed to get latest instant with ValidCheckpointInfo", e);
      }
    }).orElse(Option.empty());
  }
}
