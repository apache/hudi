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

import org.apache.hudi.avro.model.StorageLocationInfo;
import org.apache.hudi.common.bootstrap.FileStatusUtils;

import java.io.Serializable;

/**
 * POJO storing (partitionPath, hoodieFileId) -> external base file path.
 */
public class BootstrapBaseFileMapping implements Serializable {

  private final HoodieFileGroupId fileGroupId;

  private final StorageLocationInfo bootstrapFileLocationInfo;

  public BootstrapBaseFileMapping(HoodieFileGroupId fileGroupId, StorageLocationInfo bootstrapFileLocationInfo) {
    this.fileGroupId = fileGroupId;
    this.bootstrapFileLocationInfo = bootstrapFileLocationInfo;
  }

  public HoodieFileGroupId getFileGroupId() {
    return fileGroupId;
  }

  public BaseFile getBootstrapBaseFile() {
    return new BaseFile(FileStatusUtils.toStoragePathInfo(bootstrapFileLocationInfo));
  }

  @Override
  public String toString() {
    return "BootstrapBaseFileMapping{"
        + "fileGroupId=" + fileGroupId
        + ", bootstrapFileLocationInfo=" + bootstrapFileLocationInfo
        + '}';
  }
}
