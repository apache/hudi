/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.delta.sync;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Add implements Serializable {

  String path;
  Map<String, String> partitionValues = new HashMap<>();
  long size;
  long modificationTime;
  boolean dataChange;
  Long deletionTimestamp;
  Boolean extendedFileMetadata;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  public void setPartitionValues(Map<String, String> partitionValues) {
    this.partitionValues = partitionValues;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public boolean isDataChange() {
    return dataChange;
  }

  public void setDataChange(boolean dataChange) {
    this.dataChange = dataChange;
  }

  public Long getDeletionTimestamp() {
    return deletionTimestamp;
  }

  public void setDeletionTimestamp(Long deletionTimestamp) {
    this.deletionTimestamp = deletionTimestamp;
  }

  public Boolean getExtendedFileMetadata() {
    return extendedFileMetadata;
  }

  public void setExtendedFileMetadata(Boolean extendedFileMetadata) {
    this.extendedFileMetadata = extendedFileMetadata;
  }
}
