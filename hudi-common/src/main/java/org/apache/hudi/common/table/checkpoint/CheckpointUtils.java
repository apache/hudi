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

package org.apache.hudi.common.table.checkpoint;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_RESET_KEY_V2;
import static org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.USE_TRANSITION_TIME;

public class CheckpointUtils {

  public static final Set<String> DATASOURCES_NOT_SUPPORTED_WITH_CKPT_V2 = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource",
      "org.apache.hudi.utilities.sources.GcsEventsHoodieIncrSource",
      "org.apache.hudi.utilities.sources.MockS3EventsHoodieIncrSource",
      "org.apache.hudi.utilities.sources.MockGcsEventsHoodieIncrSource"
  )));

  public static final Set<String> HOODIE_INCREMENTAL_SOURCES;

  static {
    HashSet<String> hoodieIncSource = new HashSet<>(DATASOURCES_NOT_SUPPORTED_WITH_CKPT_V2);
    hoodieIncSource.add("org.apache.hudi.utilities.sources.MockGeneralHoodieIncrSource");
    hoodieIncSource.add("org.apache.hudi.utilities.sources.HoodieIncrSource");
    HOODIE_INCREMENTAL_SOURCES = Collections.unmodifiableSet(hoodieIncSource);
  }

  public static final String RESET_CHECKPOINT_V2_SEPARATOR = ":";
  public static final String REQUEST_TIME_PREFIX = "resumeFromInstantRequestTime";
  public static final String COMPLETION_TIME_PREFIX = "resumeFromInstantCompletionTime";

  public static Checkpoint getCheckpoint(HoodieCommitMetadata commitMetadata) {
    if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(STREAMER_CHECKPOINT_KEY_V2))
        || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(STREAMER_CHECKPOINT_RESET_KEY_V2))) {
      return new StreamerCheckpointV2(commitMetadata);
    }
    if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(STREAMER_CHECKPOINT_KEY_V1))
        || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(STREAMER_CHECKPOINT_RESET_KEY_V1))) {
      return new StreamerCheckpointV1(commitMetadata);
    }
    throw new HoodieException("Checkpoint is not found in the commit metadata: " + commitMetadata.getExtraMetadata());
  }

  /**
   * For sources that do not have a semantic change in the checkpoint, always use checkpoint V1.
   *
   * @param checkpointToResume value of the checkpoint to resume
   * @return {@link Checkpoint} instance
   */
  public static Checkpoint createCheckpoint(String checkpointToResume) {
    return new StreamerCheckpointV1(checkpointToResume);
  }

  /**
   * For sources that do not have a semantic change in the checkpoint, always use checkpoint V1.
   *
   * @param checkpointToResume the checkpoint to resume
   * @return {@link Checkpoint} instance
   */
  public static Checkpoint createCheckpoint(Checkpoint checkpointToResume) {
    return new StreamerCheckpointV1(checkpointToResume);
  }

  // TODO(yihua): for checkpoint translation, handle cases where the checkpoint is not exactly the
  // instant or completion time
  public static StreamerCheckpointV2 convertToCheckpointV2ForCommitTime(
      Checkpoint checkpoint, HoodieTableMetaClient metaClient, TimelineUtils.HollowCommitHandling hollowCommitHandlingMode) {
    if (checkpoint.checkpointKey.equals(HoodieTimeline.INIT_INSTANT_TS)) {
      return new StreamerCheckpointV2(HoodieTimeline.INIT_INSTANT_TS);
    }
    if (checkpoint instanceof StreamerCheckpointV2) {
      return (StreamerCheckpointV2) checkpoint;
    }
    if (checkpoint instanceof StreamerCheckpointV1) {
      // V1 -> V2 translation
      if (hollowCommitHandlingMode.equals(USE_TRANSITION_TIME)) {
        return new StreamerCheckpointV2(checkpoint);
      }
      // TODO(yihua): handle different ordering between requested and completion time
      // TODO(yihua): handle timeline history / archived timeline
      String instantTime = checkpoint.getCheckpointKey();
      Option<String> completionTime = metaClient.getActiveTimeline()
          .getInstantsAsStream()
          .filter(s -> instantTime.equals(s.requestedTime()))
          .map(HoodieInstant::getCompletionTime)
          .filter(Objects::nonNull)
          .findFirst().map(Option::of).orElse(Option.empty());
      if (completionTime.isEmpty()) {
        throw new UnsupportedOperationException("Unable to find completion time for " + instantTime);
      }
      return new StreamerCheckpointV2(completionTime.get());
    }
    throw new UnsupportedOperationException("Unsupported checkpoint type: " + checkpoint.getClass());
  }

  public static StreamerCheckpointV1 convertToCheckpointV1ForCommitTime(
      Checkpoint checkpoint, HoodieTableMetaClient metaClient) {
    if (checkpoint.checkpointKey.equals(HoodieTimeline.INIT_INSTANT_TS)) {
      return new StreamerCheckpointV1(HoodieTimeline.INIT_INSTANT_TS);
    }
    if (checkpoint instanceof StreamerCheckpointV1) {
      return (StreamerCheckpointV1) checkpoint;
    }
    if (checkpoint instanceof StreamerCheckpointV2) {
      // V2 -> V1 translation
      // TODO(yihua): handle USE_TRANSITION_TIME in V1
      // TODO(yihua): handle different ordering between requested and completion time
      // TODO(yihua): handle timeline history / archived timeline
      String completionTime = checkpoint.getCheckpointKey();
      Option<String> instantTime = metaClient.getActiveTimeline()
          .getInstantsAsStream()
          .filter(s -> completionTime.equals(s.getCompletionTime()))
          .map(HoodieInstant::requestedTime)
          .filter(Objects::nonNull)
          .findFirst().map(Option::of).orElse(Option.empty());
      if (instantTime.isEmpty()) {
        throw new UnsupportedOperationException("Unable to find requested time for " + completionTime);
      }
      return new StreamerCheckpointV1(instantTime.get());
    }
    throw new UnsupportedOperationException("Unsupported checkpoint type: " + checkpoint.getClass());

  }

  /**
   * For hoodie incremental source ingestion, if the target table is version 8 or higher, the checkpoint
   * key set by streamer config can be in either of the following format:
   * - resumeFromInstantRequestTime:[checkpoint value based on request time]
   * - resumeFromInstantCompletionTime:[checkpoint value based on completion time]
   *
   * StreamerCheckpointV2FromCfgCkp class itself captured the fact that this is version 8 and higher, plus
   * the checkpoint source is from streamer config override.
   *
   * When the checkpoint is consumed by individual data sources, we need to convert them to either vanilla
   * checkpoint v1 (request time based) or checkpoint v2 (completion time based).
   */
  public static Checkpoint resolveToActualCheckpointVersion(UnresolvedStreamerCheckpointBasedOnCfg checkpoint) {
    String[] parts = extractKeyValues(checkpoint);
    switch (parts[0]) {
      case REQUEST_TIME_PREFIX: {
        return new StreamerCheckpointV1(checkpoint).setCheckpointKey(parts[1]);
      }
      case COMPLETION_TIME_PREFIX: {
        return new StreamerCheckpointV2(checkpoint).setCheckpointKey(parts[1]);
      }
      default:
        throw new IllegalArgumentException("Unknown event ordering mode " + parts[0]);
    }
  }

  private static String[] extractKeyValues(UnresolvedStreamerCheckpointBasedOnCfg checkpoint) {
    String checkpointKey = checkpoint.getCheckpointKey();
    String[] parts = checkpointKey.split(RESET_CHECKPOINT_V2_SEPARATOR);
    if (parts.length != 2
        || (
          !parts[0].trim().equals(REQUEST_TIME_PREFIX)
          && !parts[0].trim().equals(COMPLETION_TIME_PREFIX)
        )) {
      throw new IllegalArgumentException(
          "Illegal checkpoint key override `" + checkpointKey + "`. Valid format is either `resumeFromInstantRequestTime:<checkpoint value>` or "
          + "`resumeFromInstantCompletionTime:<checkpoint value>`.");
    }
    parts[0] = parts[0].trim();
    parts[1] = parts[1].trim();
    return parts;
  }
}
