/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop.realtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Filesplit that wraps the base split and a list of log files to merge deltas from.
 */
public class HoodieRealtimeFileSplit extends FileSplit {

  private List<String> deltaFilePaths;

  private String maxCommitTime;

  private String basePath;

  public HoodieRealtimeFileSplit() {
    super();
  }

  public HoodieRealtimeFileSplit(FileSplit baseSplit, String basePath, List<String> deltaLogFiles,
      String maxCommitTime) throws IOException {
    super(baseSplit.getPath(), baseSplit.getStart(), baseSplit.getLength(),
        baseSplit.getLocations());
    this.deltaFilePaths = deltaLogFiles;
    this.maxCommitTime = maxCommitTime;
    this.basePath = basePath;
  }

  public List<String> getDeltaFilePaths() {
    return deltaFilePaths;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  public String getBasePath() {
    return basePath;
  }

  private static void writeString(String str, DataOutput out) throws IOException {
    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }


  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    writeString(basePath, out);
    writeString(maxCommitTime, out);
    out.writeInt(deltaFilePaths.size());
    for (String logFilePath : deltaFilePaths) {
      writeString(logFilePath, out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    basePath = readString(in);
    maxCommitTime = readString(in);
    int totalLogFiles = in.readInt();
    deltaFilePaths = new ArrayList<>(totalLogFiles);
    for (int i = 0; i < totalLogFiles; i++) {
      deltaFilePaths.add(readString(in));
    }
  }
}
