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

package org.apache.hudi.common.table.timeline.dto;

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.collection.Pair;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The data transfer object of clustering.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusteringOpDTO {

  @JsonProperty("id")
  private String fileId;

  @JsonProperty("partition")
  private String partitionPath;

  @JsonProperty("instantTime")
  private String instantTime;

  @JsonProperty("instantState")
  private String instantState;

  @JsonProperty("instantAction")
  private String instantAction;

  public static ClusteringOpDTO fromClusteringOp(HoodieFileGroupId fileGroupId, HoodieInstant instant) {
    ClusteringOpDTO dto = new ClusteringOpDTO();
    dto.fileId = fileGroupId.getFileId();
    dto.partitionPath = fileGroupId.getPartitionPath();
    dto.instantAction = instant.getAction();
    dto.instantState = instant.getState().name();
    dto.instantTime = instant.requestedTime();
    return dto;
  }

  public static Pair<HoodieFileGroupId, HoodieInstant> toClusteringOperation(ClusteringOpDTO dto, InstantGenerator factory) {
    return Pair.of(new HoodieFileGroupId(dto.partitionPath, dto.fileId),
        factory.createNewInstant(HoodieInstant.State.valueOf(dto.instantState), dto.instantAction, dto.instantTime));
  }
}
