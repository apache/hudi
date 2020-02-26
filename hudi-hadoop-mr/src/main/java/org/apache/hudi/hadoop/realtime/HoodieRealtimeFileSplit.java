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

package org.apache.hudi.hadoop.realtime;

import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Filesplit that wraps the base split and a list of log files to merge deltas from.
 */
public class HoodieRealtimeFileSplit extends FileSplit implements RealtimeSplit {

  private List<String> deltaLogPaths;

  private String maxCommitTime;

  private String basePath;

  public HoodieRealtimeFileSplit() {
    super();
  }

  public HoodieRealtimeFileSplit(FileSplit baseSplit, String basePath, List<String> deltaLogPaths, String maxCommitTime)
      throws IOException {
    super(baseSplit.getPath(), baseSplit.getStart(), baseSplit.getLength(), baseSplit.getLocations());
    this.deltaLogPaths = deltaLogPaths;
    this.maxCommitTime = maxCommitTime;
    this.basePath = basePath;
  }

  public List<String> getDeltaLogPaths() {
    return deltaLogPaths;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setDeltaLogPaths(List<String> deltaLogPaths) {
    this.deltaLogPaths = deltaLogPaths;
  }

  public void setMaxCommitTime(String maxCommitTime) {
    this.maxCommitTime = maxCommitTime;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    writeToOutput(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    readFromInput(in);
  }

  @Override
  public String toString() {
    return "HoodieRealtimeFileSplit{DataPath=" + getPath() + ", deltaLogPaths=" + deltaLogPaths
        + ", maxCommitTime='" + maxCommitTime + '\'' + ", basePath='" + basePath + '\'' + '}';
  }
}
