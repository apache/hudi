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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;

/**
 * This is used as an extraPreCommitFunc in BaseHoodieWriteClient
 * It adds the checkpoint to deltacommit metadata. It must be implemented this way
 * because it needs the lock to ensure that it does not overwrite another deltastreamers
 * latest checkpoint with an older one.
 */
public class HoodieDeltaStreamerMultiwriterCheckpoint implements BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata> {
  private static final ObjectMapper OM = new ObjectMapper();

  //deltastreamer id
  private final String id;

  //the deltastreamer
  private final DeltaSync ds;
  private final String checkpoint;
  private final String latestCheckpointWritten;
  
  public HoodieDeltaStreamerMultiwriterCheckpoint(DeltaSync ds, String checkpoint, String latestCheckpointWritten) {
    this.ds = ds;
    this.id = ds.getId();
    this.checkpoint = checkpoint;
    this.latestCheckpointWritten = latestCheckpointWritten;
  }

  @Override
  public void accept(HoodieTableMetaClient metaClient, HoodieCommitMetadata commitMetadata) {
    //Get last completed deltacommit
    Option<HoodieCommitMetadata> latestCommitMetadata;
    try {
      ds.refreshTimeline();
      latestCommitMetadata = ds.getLatestCommitMetadataWithValidCheckpointInfo(metaClient.getActiveTimeline().getCommitsTimeline());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get the latest commit metadata", e);
    }

    //Get checkpoint map
    Map<String,String> checkpointMap;
    if (latestCommitMetadata.isPresent()) {
      String value = commitMetadata.getMetadata(CHECKPOINT_KEY);
      try {
        checkpointMap = OM.readValue(value, Map.class);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse checkpoint as map", e);
      }
    } else {
      checkpointMap = new HashMap<>();
    }

    if (!StringUtils.isNullOrEmpty(latestCheckpointWritten)
        && checkpointMap.containsKey(id) && !checkpointMap.get(id).equals(latestCheckpointWritten)) {
      throw new HoodieException(String.format("Multiple DeltaStreamer instances with id: %s detected. Each Deltastreamer must have a unique id.", id));
    }

    //Add map to metadata
    checkpointMap.put(id, checkpoint);
    try {
      commitMetadata.addMetadata(CHECKPOINT_KEY, OM.writeValueAsString(checkpointMap));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Option<String> readCheckpointValue(String value, String id) {
    try {
      Map<String,String> checkpointMap = OM.readValue(value, Map.class);
      if (!checkpointMap.containsKey(id)) {
        return Option.empty();
      }
      String checkpointVal = checkpointMap.get(id);
      return Option.of(checkpointVal);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to parse checkpoint as map", e);
    }
  }
}
