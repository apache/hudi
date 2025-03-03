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

package org.apache.hudi.utilities.checkpointing;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hudi.utilities.streamer.HoodieStreamer.CHECKPOINT_KEY;

/**
 * This is used to set a checkpoint from latest commit of another (mirror) hudi dataset.
 * Used by integration test.
 */
public class InitialCheckpointFromAnotherHoodieTimelineProvider extends InitialCheckPointProvider {

  private HoodieTableMetaClient anotherDsHoodieMetaClient;

  public InitialCheckpointFromAnotherHoodieTimelineProvider(TypedProperties props) {
    super(props);
  }

  @Override
  public void init(Configuration config) throws HoodieException {
    super.init(config);
    this.anotherDsHoodieMetaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(config))
        .setBasePath(path.toString()).build();
  }

  @Override
  public String getCheckpoint() throws HoodieException {
    return anotherDsHoodieMetaClient.getCommitsTimeline().filterCompletedInstants().getReverseOrderedInstants()
        .map(instant -> {
          try {
            HoodieCommitMetadata commitMetadata =
                anotherDsHoodieMetaClient.getActiveTimeline().readCommitMetadata(instant);
            return commitMetadata.getMetadata(CHECKPOINT_KEY);
          } catch (IOException e) {
            return null;
          }
        }).filter(Objects::nonNull).findFirst().get();
  }
}
