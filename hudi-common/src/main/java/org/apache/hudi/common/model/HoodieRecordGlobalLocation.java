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

package org.apache.hudi.common.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.Objects;

/**
 * Similar with {@link org.apache.hudi.common.model.HoodieRecordLocation} but with partition path.
 */
public final class HoodieRecordGlobalLocation extends HoodieRecordLocation {
  private static final long serialVersionUID = 1L;

  private String partitionPath;

  public HoodieRecordGlobalLocation() {
  }

  public HoodieRecordGlobalLocation(String partitionPath, String instantTime, String fileId) {
    super(instantTime, fileId);
    this.partitionPath = partitionPath;
  }

  public HoodieRecordGlobalLocation(String partitionPath, String instantTime, String fileId, long position) {
    super(instantTime, fileId, position);
    this.partitionPath = partitionPath;
  }

  @Override
  public String toString() {
    return "HoodieGlobalRecordLocation {" + "partitionPath=" + partitionPath + ", "
        + "instantTime=" + instantTime + ", "
        + "fileId=" + fileId + ", "
        + "position=" + position
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieRecordGlobalLocation otherLoc = (HoodieRecordGlobalLocation) o;
    return Objects.equals(partitionPath, otherLoc.partitionPath)
        && Objects.equals(instantTime, otherLoc.instantTime)
        && Objects.equals(fileId, otherLoc.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionPath, instantTime, fileId);
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  /**
   * Returns the global record location from local.
   */
  public static HoodieRecordGlobalLocation fromLocal(String partitionPath, HoodieRecordLocation localLoc) {
    return new HoodieRecordGlobalLocation(partitionPath, localLoc.getInstantTime(), localLoc.getFileId());
  }

  /**
   * Returns the record location as local.
   */
  public HoodieRecordLocation toLocal(String instantTime) {
    return new HoodieRecordLocation(instantTime, fileId, position);
  }

  /**
   * Copy the location with given partition path.
   */
  public HoodieRecordGlobalLocation copy(String partitionPath) {
    return new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId, position);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    super.write(kryo, output);

    kryo.writeObjectOrNull(output, partitionPath, String.class);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    super.read(kryo, input);

    this.partitionPath = kryo.readObject(input, String.class);
  }
}

