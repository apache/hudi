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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.Objects;

/**
 * Location of a HoodieRecord within the partition it belongs to. Ultimately, this points to an actual file on disk
 */
public class HoodieRecordLocation implements Serializable, KryoSerializable {
  public static final long INVALID_POSITION = -1L;

  protected String instantTime;
  protected String fileId;
  // Position of the record in the file, e.g., row position starting from 0 in the Parquet file
  // Valid position should be non-negative. Negative position, i.e., -1, means it's invalid
  // and should not be used
  protected long position;

  public HoodieRecordLocation() {
  }

  public HoodieRecordLocation(String instantTime, String fileId) {
    this(instantTime, fileId, INVALID_POSITION);
  }

  public HoodieRecordLocation(String instantTime, String fileId, long position) {
    this.instantTime = instantTime;
    this.fileId = fileId;
    this.position = position;
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
    return "HoodieRecordLocation {" + "instantTime=" + instantTime + ", "
        + "fileId=" + fileId + ", "
        + "position=" + position
        + '}';
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

  public static boolean isPositionValid(long position) {
    return position > INVALID_POSITION;
  }

  public long getPosition() {
    return position;
  }

  public void setPosition(long position) {
    this.position = position;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    output.writeString(instantTime);
    output.writeString(fileId);
    output.writeLong(position);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    this.instantTime = input.readString();
    this.fileId = input.readString();
    this.position = input.readLong();
  }
}
