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

package org.apache.hudi.utilities;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Metadata representing the state of a table sync process. This metadata is stored in the target
 * table's properties and is used to track the status of previous sync operation.
 */

@Getter
@AllArgsConstructor
public class SyncMetadata {
  private static final int CURRENT_VERSION = 0;
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  public static final String TABLE_SYNC_METADATA = "TABLE_SYNC_METADATA";

  public SyncMetadata() {

  }

  @JsonProperty
  Instant lastSyncInstant;

  @JsonProperty
  List<TableCheckpointInfo> tableCheckpointInfos;

  @JsonProperty
  int version;

  public static SyncMetadata of(
      Instant lastInstantSynced, List<TableCheckpointInfo> tableCheckpointInfos) {
    return new SyncMetadata(
        lastInstantSynced,
        tableCheckpointInfos,
        CURRENT_VERSION);
  }

  public String toJson() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      throw new HoodieException("Failed to serialize TableSyncMetadata", e);
    }
  }

  public static Option<SyncMetadata> fromJson(String metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return Option.empty();
    } else {
      try {
        SyncMetadata parsedMetadata = MAPPER.readValue(metadata, SyncMetadata.class);
        return Option.of(parsedMetadata);
      } catch (IOException e) {
        throw new HoodieException("Failed to deserialize TableSyncMetadata", e);
      }
    }
  }
}
