/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.timeline.dto;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.storage.HoodieFileStatus;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data transfer object for instant state.
 * <p>
 * see org.apache.hudi.sink.meta.CkpMessage.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InstantStateDTO {

  /**
   * The instant time.
   *
   * @see org.apache.hudi.sink.meta.CkpMessage#instant
   */
  @JsonProperty("instant")
  String instant;

  /**
   * The instant state.
   *
   * @see org.apache.hudi.sink.meta.CkpMessage#state
   */
  @JsonProperty("state")
  String state;

  public static InstantStateDTO fromFileStatus(HoodieFileStatus fileStatus) {
    InstantStateDTO ret = new InstantStateDTO();
    String fileName = fileStatus.getLocation().getName();
    String[] nameAndExt = fileName.split("\\.");
    ValidationUtils.checkState(nameAndExt.length == 2);
    ret.instant = nameAndExt[0];
    ret.state = nameAndExt[1];
    return ret;
  }

  public String getInstant() {
    return instant;
  }

  public String getState() {
    return state;
  }
}
