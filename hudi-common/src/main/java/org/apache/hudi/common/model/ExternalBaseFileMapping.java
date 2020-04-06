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
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.bootstrap.FileStatusUtils;

/**
 * POJO storing (partitionPath, hoodieFileId) -> external data file path.
 */
public class ExternalBaseFileMapping implements Serializable {

  private final HoodieFileGroupId fileGroupId;

  private final HoodieFileStatus externalFileStatus;

  public ExternalBaseFileMapping(HoodieFileGroupId fileGroupId, HoodieFileStatus externalFileStatus) {
    this.fileGroupId = fileGroupId;
    this.externalFileStatus = externalFileStatus;
  }

  public HoodieFileGroupId getFileGroupId() {
    return fileGroupId;
  }

  public BaseFile getExternalBaseFile() {
    return new BaseFile(FileStatusUtils.toFileStatus(externalFileStatus));
  }

  public HoodieFileStatus getExternalFileStatus() {
    return externalFileStatus;
  }

  @Override
  public String toString() {
    return "ExternalBaseFileMapping{"
        + "fileGroupId=" + fileGroupId
        + ", externalFileStatus=" + externalFileStatus
        + '}';
  }
}
