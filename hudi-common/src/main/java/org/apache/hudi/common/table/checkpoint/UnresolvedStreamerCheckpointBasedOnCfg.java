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

/**
 * A special checkpoint v2 class that indicates its checkpoint key comes from streamer config checkpoint
 * overrides.
 *
 * For hoodie incremental source, based on the content of the checkpoint override value, it can indicate
 * either request time based checkpoint or completion time based. So the class serves as an indicator to
 * data sources of interest that it needs to be further parsed and resolved to either checkpoint v1 or v2.
 *
 * For all the other data sources, it behaves exactly the same as checkpoint v2.
 *
 * To keep the checkpoint class design ignorant of which data source it serves, the class only indicates where
 * the checkpoint key comes from.
 * */
public class UnresolvedStreamerCheckpointBasedOnCfg extends StreamerCheckpointV2 {
  public UnresolvedStreamerCheckpointBasedOnCfg(String key) {
    super(key);
  }

  public UnresolvedStreamerCheckpointBasedOnCfg(Checkpoint checkpoint) {
    super(checkpoint);
  }

  public UnresolvedStreamerCheckpointBasedOnCfg(HoodieCommitMetadata commitMetadata) {
    super(commitMetadata);
  }
}
