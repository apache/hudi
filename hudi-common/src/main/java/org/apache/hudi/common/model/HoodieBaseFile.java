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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileStatus;

/**
 * Hoodie base file - Represents metadata about Hudi file in DFS.
 * Supports APIs to get Hudi FileId, Commit Time and external file (for bootstrap).
 */
public class HoodieBaseFile extends BaseFile {

  /**
   * In case of index-only bootstrap, this points to the actual data file.
   */
  private Option<BaseFile> externalBaseFile;

  public HoodieBaseFile(HoodieBaseFile dataFile) {
    super(dataFile);
    this.externalBaseFile = dataFile.externalBaseFile;
  }

  public HoodieBaseFile(FileStatus fileStatus) {
    this(fileStatus, null);
  }

  public HoodieBaseFile(FileStatus fileStatus, BaseFile externalBaseFile) {
    super(fileStatus);
    this.externalBaseFile = Option.ofNullable(externalBaseFile);
  }

  public HoodieBaseFile(String filePath) {
    super(filePath);
  }

  public HoodieBaseFile(String filePath, BaseFile externalBaseFile) {
    super(filePath);
    this.externalBaseFile = Option.ofNullable(externalBaseFile);
  }

  public String getFileId() {
    return FSUtils.getFileId(getFileName());
  }

  public String getCommitTime() {
    return FSUtils.getCommitTime(getFileName());
  }

  public Option<BaseFile> getExternalBaseFile() {
    return externalBaseFile;
  }

  public void setExternalBaseFile(BaseFile externalBaseFile) {
    this.externalBaseFile = Option.ofNullable(externalBaseFile);
  }

  @Override
  public String toString() {
    return "HoodieBaseFile{fullPath=" + getPath() + ", fileLen=" + getFileLen()
        + ", ExternalDataFile=" + externalBaseFile.orElse(null) + '}';
  }
}
