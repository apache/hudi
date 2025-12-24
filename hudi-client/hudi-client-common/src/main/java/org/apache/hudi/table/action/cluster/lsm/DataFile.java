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

package org.apache.hudi.table.action.cluster.lsm;

import org.apache.hudi.common.model.HoodieLogFile;

public class DataFile {

  private final String min;

  private final String max;
  private final String fileFullPath;
  private final long dataSize;

  private final int levelNumber;
  private final HoodieLogFile logFile;

  public DataFile(int levelNumber, String min, String max, long dataSize, String fileFullPath, HoodieLogFile logfile) {
    this.levelNumber = levelNumber;
    this.min = min;
    this.max = max;
    this.dataSize = dataSize;
    this.fileFullPath = fileFullPath;
    this.logFile = logfile;
  }

  public String getMax() {
    return max;
  }

  public String getMin() {
    return min;
  }

  public String getPath() {
    return fileFullPath;
  }

  public long getDataSize() {
    return dataSize;
  }

  public int getLevelNumber() {
    return levelNumber;
  }

  public HoodieLogFile getLogFile() {
    return this.logFile;
  }

  @Override
  public String toString() {
    return String.format(
        "{filePath: %s, fileSize: %d, min: %s, max: %s}",
        fileFullPath,
        dataSize,
        min,
        max);
  }
}
