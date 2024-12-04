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
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import java.util.Objects;

import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1.STREAMER_CHECKPOINT_RESET_KEY_V1;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_KEY_V2;
import static org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2.STREAMER_CHECKPOINT_RESET_KEY_V2;

public class CheckpointUtils {

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

  public static boolean targetCheckpointV2(int writeTableVersion) {
    return writeTableVersion >= HoodieTableVersion.EIGHT.versionCode();
  }

  // TODO(yihua): for checkpoint translation, handle cases where the checkpoint is not exactly the
  // instant or completion time
  public static StreamerCheckpointV2 convertToCheckpointV2ForCommitTime(
      Checkpoint checkpoint, HoodieTableMetaClient metaClient) {
    if (checkpoint.checkpointKey.equals(HoodieTimeline.INIT_INSTANT_TS)) {
      return new StreamerCheckpointV2(HoodieTimeline.INIT_INSTANT_TS);
    }
    if (checkpoint instanceof StreamerCheckpointV2) {
      return (StreamerCheckpointV2) checkpoint;
    }
    if (checkpoint instanceof StreamerCheckpointV1) {
      // V1 -> V2 translation
      // TODO(yihua): handle USE_TRANSITION_TIME in V1
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
}
