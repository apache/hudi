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
 * because it needs the lock to ensure that it does not overwrite another deltastreamer's
 * latest checkpoint with an older one.
 */
public class DeltastreamerMultiWriterCkptUpdateFunc implements BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  //deltastreamer id
  private final String id;

  //the deltastreamer
  private final DeltaSync ds;
  private final String newCheckpoint;
  private final String latestCheckpointWritten;

  public DeltastreamerMultiWriterCkptUpdateFunc(DeltaSync ds, String newCheckpoint, String latestCheckpointWritten) {
    this.ds = ds;
    this.id = ds.getMultiwriterIdentifier();
    this.newCheckpoint = newCheckpoint;
    this.latestCheckpointWritten = latestCheckpointWritten;
  }

  @Override
  public void accept(HoodieTableMetaClient metaClient, HoodieCommitMetadata commitMetadata) {
    //Get last completed deltacommit
    Option<HoodieCommitMetadata> latestCommitMetadata;
    try {
      latestCommitMetadata = ds.getLatestCommitMetadataWithValidCheckpointInfo(metaClient.reloadActiveTimeline().getCommitsTimeline());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get the latest commit metadata", e);
    }

    //Get checkpoint map
    Map<String,String> checkpointMap;
    if (latestCommitMetadata.isPresent()) {
      String value = latestCommitMetadata.get().getExtraMetadata().get(CHECKPOINT_KEY);
      try {
        checkpointMap = OBJECT_MAPPER.readValue(value, Map.class);
      } catch (Exception e) {
        throw new HoodieException("Failed to parse checkpoint as map", e);
      }
    } else {
      checkpointMap = new HashMap<>();
    }

    if (!StringUtils.isNullOrEmpty(latestCheckpointWritten)
        && checkpointMap.containsKey(id) && !checkpointMap.get(id).equals(latestCheckpointWritten)) {
      throw new HoodieException(String.format("Multiple DeltaStreamer instances with id: %s detected. Each Deltastreamer must have a unique id.", id));
    }

    //Add map to metadata
    checkpointMap.put(id, newCheckpoint);
    try {
      commitMetadata.addMetadata(CHECKPOINT_KEY, OBJECT_MAPPER.writeValueAsString(checkpointMap));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
