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

package org.apache.hudi.utilities.streamer.checkpoint;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.config.HoodieWriteConfig.STREAMER_CHECKPOINT_KEY;
import static org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_RESET_KEY;

public class CheckpointV1 extends Checkpoint {
  public CheckpointV1(String key) {
    this.checkpointKey = key;
    this.checkpointResetKey = null;
    this.checkpointIgnoreKey = null;
  }

  public CheckpointV1(Checkpoint checkpoint) {
    this.checkpointKey = checkpoint.getCheckpointKey();
    this.checkpointResetKey = checkpoint.getCheckpointResetKey();
    this.checkpointIgnoreKey = checkpoint.getCheckpointIgnoreKey();
  }

  public CheckpointV1(HoodieCommitMetadata commitMetadata) {
    this.checkpointKey = commitMetadata.getMetadata(STREAMER_CHECKPOINT_KEY);
    this.checkpointResetKey = commitMetadata.getMetadata(CHECKPOINT_RESET_KEY);
    this.checkpointIgnoreKey = commitMetadata.getMetadata(CHECKPOINT_IGNORE_KEY);
  }

  @Override
  public Map<String, String> getCheckpointCommitMetadata(HoodieStreamer.Config streamerConfig) {
    Map<String, String> checkpointCommitMetadata = new HashMap<>();
    if (checkpointKey != null) {
      checkpointCommitMetadata.put(STREAMER_CHECKPOINT_KEY, getCheckpointKey());
    }
    // TODO(yihua): handle reset key translation?
    if (streamerConfig.checkpoint != null) {
      checkpointCommitMetadata.put(CHECKPOINT_RESET_KEY, streamerConfig.checkpoint);
    }
    if (streamerConfig.ignoreCheckpoint != null) {
      checkpointCommitMetadata.put(CHECKPOINT_IGNORE_KEY, streamerConfig.ignoreCheckpoint);
    }
    return checkpointCommitMetadata;
  }
}
