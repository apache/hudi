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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The data transfer object of timeline.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimelineDTO {

  @JsonProperty("instants")
  List<InstantDTO> instants;

  public static TimelineDTO fromTimeline(HoodieTimeline timeline) {
    TimelineDTO dto = new TimelineDTO();
    dto.instants = timeline.getInstants().map(InstantDTO::fromInstant).collect(Collectors.toList());
    return dto;
  }

  public static HoodieTimeline toTimeline(TimelineDTO dto, HoodieTableMetaClient metaClient) {
    // TODO: For Now, we will assume, only active-timeline will be transferred.
    return new HoodieDefaultTimeline(dto.instants.stream().map(InstantDTO::toInstant),
        metaClient.getActiveTimeline()::getInstantDetails);
  }
}
