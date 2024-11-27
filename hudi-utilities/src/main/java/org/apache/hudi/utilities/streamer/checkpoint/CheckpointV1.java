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

import org.apache.hudi.utilities.streamer.HoodieStreamer;

import java.util.Collections;
import java.util.Map;

public class CheckpointV1 extends Checkpoint {
  public CheckpointV1(String value) {
    this.checkpointKey = value;
  }

  @Override
  public Map<String, String> getCheckpointCommitMetadata(HoodieStreamer.Config streamerConfig) {
    return Collections.emptyMap();
  }

  @Override
  public CheckpointV1 convertToCheckpointV1() {
    return null;
  }

  @Override
  public CheckpointV2 convertToCheckpointV2() {
    return null;
  }
}
