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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.Serializable;

/**
 * A serializable FS Permission.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FSPermissionDTO implements Serializable {

  @JsonProperty("useraction")
  FsAction useraction;

  @JsonProperty("groupaction")
  FsAction groupaction;

  @JsonProperty("otheraction")
  FsAction otheraction;

  @JsonProperty("stickyBit")
  boolean stickyBit;

  public static FSPermissionDTO fromFsPermission(FsPermission permission) {
    if (null == permission) {
      return null;
    }
    FSPermissionDTO dto = new FSPermissionDTO();
    dto.useraction = permission.getUserAction();
    dto.groupaction = permission.getGroupAction();
    dto.otheraction = permission.getOtherAction();
    dto.stickyBit = permission.getStickyBit();
    return dto;
  }

  public static FsPermission fromFsPermissionDTO(FSPermissionDTO dto) {
    if (null == dto) {
      return null;
    }
    return new FsPermission(dto.useraction, dto.groupaction, dto.otheraction, dto.stickyBit);
  }
}
