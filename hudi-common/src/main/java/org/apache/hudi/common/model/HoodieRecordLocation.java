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

package org.apache.hudi.common.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Location of a HoodieRecord within the partition it belongs to. Ultimately, this points to an actual file on disk
 */
public class HoodieRecordLocation implements Serializable {

  protected String instantTime;
  protected String fileId;

  public HoodieRecordLocation() {
  }

  public HoodieRecordLocation(String instantTime, String fileId) {
    this.instantTime = instantTime;
    this.fileId = fileId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieRecordLocation otherLoc = (HoodieRecordLocation) o;
    return Objects.equals(instantTime, otherLoc.instantTime) && Objects.equals(fileId, otherLoc.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(instantTime, fileId);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieRecordLocation {");
    sb.append("instantTime=").append(instantTime).append(", ");
    sb.append("fileId=").append(fileId);
    sb.append('}');
    return sb.toString();
  }

  public String getInstantTime() {
    return instantTime;
  }

  public void setInstantTime(String instantTime) {
    this.instantTime = instantTime;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }
}
