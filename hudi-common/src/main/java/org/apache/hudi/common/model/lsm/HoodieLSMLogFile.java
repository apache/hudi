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

package org.apache.hudi.common.model.lsm;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Objects;

/**
 * Abstracts a single log file. Contains methods to extract metadata like the fileId, version and extension from the log
 * file path.
 * <p>
 * Also contains logic to roll-over the log file
 */
public class HoodieLSMLogFile extends HoodieLogFile {

  private static final long serialVersionUID = 1L;
  private String min;
  private String max;

  public HoodieLSMLogFile(HoodieLSMLogFile logFile) {
    super(logFile);
  }

  public HoodieLSMLogFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  public HoodieLSMLogFile(Path logPath) {
    super(logPath);
  }

  public HoodieLSMLogFile(Path logPath, Long fileLen) {
    super(logPath, fileLen);
  }

  public HoodieLSMLogFile(String logPathStr) {
    super(logPathStr);
  }

  // used for test only
  public HoodieLSMLogFile(String logPathStr, long fileLength, String min, String max) {
    super(new Path(logPathStr), fileLength);
    this.min = min;
    this.max = max;
  }

  public String getUUID() {
    return FSUtils.getUUIDFromLog(getPath());
  }

  public int getLevelNumber() {
    return FSUtils.getLevelNumFromLog(getPath());
  }

  public String getFilePrefix() {
    return FSUtils.getLSMFilePrefix(getPath());
  }

  public String getMin() {
    return min;
  }

  public void setMin(String min) {
    this.min = min;
  }

  public String getMax() {
    return max;
  }

  public void setMax(String max) {
    this.max = max;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieLSMLogFile that = (HoodieLSMLogFile) o;
    return Objects.equals(pathStr, that.pathStr);
  }
}
