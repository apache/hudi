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

import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileStatus;

/**
 * Hoodie base file - Represents metadata about Hudi file in DFS.
 * Supports APIs to get Hudi FileId, Commit Time and bootstrap file (if any).
 */
public class HoodieBaseFile extends BaseFile {
  private static final long serialVersionUID = 1L;
  private static final char UNDERSCORE = '_';
  private static final char DOT = '.';
  private final String fileId;
  private final String commitTime;

  private Option<BaseFile> bootstrapBaseFile;

  public HoodieBaseFile(HoodieBaseFile dataFile) {
    super(dataFile);
    this.bootstrapBaseFile = dataFile.bootstrapBaseFile;
    this.fileId = dataFile.getFileId();
    this.commitTime = dataFile.getCommitTime();
  }

  public HoodieBaseFile(FileStatus fileStatus) {
    this(fileStatus, null);
  }

  public HoodieBaseFile(FileStatus fileStatus, BaseFile bootstrapBaseFile) {
    super(fileStatus);
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
    String[] fileIdAndCommitTime = getFileIdAndCommitTimeFromFileName();
    this.fileId = fileIdAndCommitTime[0];
    this.commitTime = fileIdAndCommitTime[1];
  }

  public HoodieBaseFile(String filePath) {
    this(filePath, null);
  }

  public HoodieBaseFile(String filePath, BaseFile bootstrapBaseFile) {
    super(filePath);
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
    String[] fileIdAndCommitTime = getFileIdAndCommitTimeFromFileName();
    this.fileId = fileIdAndCommitTime[0];
    this.commitTime = fileIdAndCommitTime[1];
  }

  /**
   * Parses the file ID and commit time from the fileName.
   * @return String array of size 2 with fileId as the first and commitTime as the second element.
   */
  private String[] getFileIdAndCommitTimeFromFileName() {
    String[] values = new String[2];
    short underscoreCount = 0;
    short lastUnderscoreIndex = 0;
    for (int i = 0; i < fileName.length(); i++) {
      char c = fileName.charAt(i);
      if (c == UNDERSCORE) {
        if (underscoreCount == 0) {
          values[0] = fileName.substring(0, i);
        }
        lastUnderscoreIndex = (short) i;
        underscoreCount++;
      } else if (c == DOT) {
        if (underscoreCount == 2) {
          values[1] = fileName.substring(lastUnderscoreIndex + 1, i);
          return values;
        }
      }
    }
    // case where there is no '.' in file name (no file suffix like .parquet)
    values[1] = fileName.substring(lastUnderscoreIndex + 1);
    return values;
  }

  public String getFileId() {
    return fileId;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public Option<BaseFile> getBootstrapBaseFile() {
    return bootstrapBaseFile;
  }

  public void setBootstrapBaseFile(BaseFile bootstrapBaseFile) {
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
  }

  @Override
  public String toString() {
    return "HoodieBaseFile{fullPath=" + getPath() + ", fileLen=" + getFileLen()
        + ", BootstrapBaseFile=" + bootstrapBaseFile.orElse(null) + '}';
  }
}
