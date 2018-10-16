/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.util.FSUtils;
import java.io.Serializable;
import org.apache.hadoop.fs.FileStatus;

public class HoodieDataFile implements Serializable {

  // FileStatus not serializable in HDP 2.x (See: https://issues.apache.org/jira/browse/HADOOP-13895)
  private transient FileStatus fileStatus;
  private final String fileName;
  private final String fullPath;
  private final long fileLen;

  public HoodieDataFile(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
    this.fileName =  hasFileStatus() ? fileStatus.getPath().getName() : null;
    this.fullPath = hasFileStatus() ? fileStatus.getPath().toString() : null;
    this.fileLen = hasFileStatus() ?  fileStatus.getLen() : -1;
  }

  public String getFileId() {
    return FSUtils.getFileId(fileName);
  }

  public String getCommitTime() {
    return FSUtils.getCommitTime(fileName);
  }

  public String getPath() {
    return fullPath;
  }

  public String getFileName() {
    return fileName;
  }

  public boolean hasFileStatus() {
    return fileStatus != null;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public long getFileSize() {
    return fileLen;
  }

  @Override
  public String toString() {
    return "HoodieDataFile{"
        + "fullPath=" + fullPath
        + ", fileName='" + fileName + '\''
        + ", fileLen=" + fileLen
        + '}';
  }
}
