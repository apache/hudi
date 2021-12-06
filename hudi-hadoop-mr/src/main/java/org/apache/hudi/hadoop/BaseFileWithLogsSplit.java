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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.model.HoodieLogFile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Encode additional information in split to track matching log file and base files.
 * Hence, this class tracks a log/base file split.
 */
public class BaseFileWithLogsSplit extends FileSplit {
  // a flag to mark this split is produced by incremental query or not.
  private boolean belongToIncrementalSplit = false;
  // the log file paths of this split.
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();
  // max commit time of current split.
  private String maxCommitTime = "";
  // the basePath of current hoodie table.
  private String basePath = "";
  // the base file belong to this split.
  private String baseFilePath = "";

  public BaseFileWithLogsSplit(Path file, long start, long length, String[] hosts) {
    super(file, start, length, hosts);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeBoolean(belongToIncrementalSplit);
    Text.writeString(out, maxCommitTime);
    Text.writeString(out, basePath);
    Text.writeString(out, baseFilePath);
    out.writeInt(deltaLogFiles.size());
    for (HoodieLogFile logFile : deltaLogFiles) {
      Text.writeString(out, logFile.getPath().toString());
      out.writeLong(logFile.getFileSize());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    belongToIncrementalSplit = in.readBoolean();
    maxCommitTime = Text.readString(in);
    basePath = Text.readString(in);
    baseFilePath = Text.readString(in);
    int deltaLogSize = in.readInt();
    List<HoodieLogFile> tempDeltaLogs = new ArrayList<>();
    for (int i = 0; i < deltaLogSize; i++) {
      String logPath = Text.readString(in);
      long logFileSize = in.readLong();
      tempDeltaLogs.add(new HoodieLogFile(new Path(logPath), logFileSize));
    }
    deltaLogFiles = tempDeltaLogs;
  }

  public boolean getBelongToIncrementalSplit() {
    return belongToIncrementalSplit;
  }

  public void setBelongToIncrementalSplit(boolean belongToIncrementalSplit) {
    this.belongToIncrementalSplit = belongToIncrementalSplit;
  }

  public List<HoodieLogFile> getDeltaLogFiles() {
    return deltaLogFiles;
  }

  public void setDeltaLogFiles(List<HoodieLogFile> deltaLogFiles) {
    this.deltaLogFiles = deltaLogFiles;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  public void setMaxCommitTime(String maxCommitTime) {
    this.maxCommitTime = maxCommitTime;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public String getBaseFilePath() {
    return baseFilePath;
  }

  public void setBaseFilePath(String baseFilePath) {
    this.baseFilePath = baseFilePath;
  }
}
