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

import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.Objects;

/**
 * Hoodie data file.
 */
public class HoodieDataFile implements Serializable {

  private transient FileStatus fileStatus;
  private final String fullPath;
  private long fileLen;

  /**
   * In case of index-only bootstrap, this file points to the actual data
   */
  private Option<String> externalDataFile;

  public HoodieDataFile(HoodieDataFile dataFile) {
    this.fileStatus = dataFile.fileStatus;
    this.fullPath = dataFile.fullPath;
    this.fileLen = dataFile.fileLen;
    this.externalDataFile = dataFile.externalDataFile;
  }

  public HoodieDataFile(FileStatus fileStatus) {
    this(fileStatus, null);
  }

  public HoodieDataFile(FileStatus fileStatus, String externalDataFile) {
    this.fileStatus = fileStatus;
    this.fullPath = fileStatus.getPath().toString();
    this.fileLen = fileStatus.getLen();
    this.externalDataFile = Option.ofNullable(externalDataFile);
  }

  public HoodieDataFile(String filePath) {
    this(filePath, null);
  }

  public HoodieDataFile(String filePath, String externalDataFile) {
    this.fileStatus = null;
    this.fullPath = filePath;
    this.fileLen = -1;
    this.externalDataFile = Option.ofNullable(externalDataFile);
  }

  public String getFileId() {
    return FSUtils.getFileId(getFileName());
  }

  public String getCommitTime() {
    return FSUtils.getCommitTime(getFileName());
  }

  public String getPath() {
    return fullPath;
  }

  public String getFileName() {
    return new Path(fullPath).getName();
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public long getFileSize() {
    return fileLen;
  }

  public void setFileLen(long fileLen) {
    this.fileLen = fileLen;
  }

  public long getFileLen() {
    return fileLen;
  }

  public Option<String> getExternalDataFile() {
    return externalDataFile;
  }

  public void setExternalDataFile(String externalDataFile) {
    this.externalDataFile = Option.ofNullable(externalDataFile);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieDataFile dataFile = (HoodieDataFile) o;
    return Objects.equals(fullPath, dataFile.fullPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullPath);
  }

  @Override
  public String toString() {
    return "HoodieDataFile{" + "fullPath=" + fullPath + ", fileLen=" + fileLen
        + ", ExternalDataFile=" + externalDataFile.orElse(null) + '}';
  }
}
