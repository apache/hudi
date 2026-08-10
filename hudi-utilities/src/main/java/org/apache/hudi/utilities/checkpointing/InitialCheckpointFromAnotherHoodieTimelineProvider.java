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
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

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
    // Use getWriteTimeline() to include compaction/logcompaction in addition to
    // commit/deltacommit/replacecommit, so checkpoint metadata rolled into any
    // non-ingestion commit type is discoverable after archival.
    return anotherDsHoodieMetaClient.getActiveTimeline().getWriteTimeline()
        .filterCompletedInstants().getReverseOrderedInstants()
        .map(instant -> {
          try {
            HoodieCommitMetadata commitMetadata =
                anotherDsHoodieMetaClient.getActiveTimeline().readCommitMetadata(instant);
            // Use CheckpointUtils to handle both V1 and V2 checkpoint keys
            return CheckpointUtils.getCheckpoint(commitMetadata).getCheckpointKey();
          } catch (HoodieException e) {
            // No checkpoint found in this commit
            return null;
          } catch (IOException e) {
            throw new HoodieIOException("Failed to read commit metadata for instant " + instant.requestedTime(), e);
          }
          // Filter out null (from HoodieException) and empty strings (from commits
          // that don't have checkpoint metadata, e.g. when rollover is not configured)
        }).filter(key -> !StringUtils.isNullOrEmpty(key)).findFirst()
        .orElseThrow(() -> new HoodieException("Unable to find checkpoint in source table at: "
            + path + ". This table may not have been created with checkpoint tracking enabled."));
  }
}
