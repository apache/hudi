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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.BootstrapBaseFileSplit;

import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Realtime File Split with external base file.
 */
public class RealtimeBootstrapBaseFileSplit extends BootstrapBaseFileSplit implements RealtimeSplit {

  private List<String> deltaLogPaths;
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();

  private String maxInstantTime;

  private String basePath;

  public RealtimeBootstrapBaseFileSplit() {
    super();
  }

  public RealtimeBootstrapBaseFileSplit(FileSplit baseSplit, String basePath, List<HoodieLogFile> deltaLogFiles,
                                        String maxInstantTime, FileSplit externalFileSplit) throws IOException {
    super(baseSplit, externalFileSplit);
    this.maxInstantTime = maxInstantTime;
    this.deltaLogFiles = deltaLogFiles;
    this.deltaLogPaths = deltaLogFiles.stream().map(entry -> entry.getPath().toString()).collect(Collectors.toList());
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
  public List<String> getDeltaLogPaths() {
    return deltaLogPaths;
  }

  @Override
  public List<HoodieLogFile> getDeltaLogFiles() {
    return deltaLogFiles;
  }

  @Override
  public String getMaxCommitTime() {
    return maxInstantTime;
  }

  @Override
  public String getBasePath() {
    return basePath;
  }

  @Override
  public Option<HoodieVirtualKeyInfo> getHoodieVirtualKeyInfo() {
    return Option.empty();
  }

  @Override
  public void setDeltaLogPaths(List<String> deltaLogPaths) {
    this.deltaLogPaths = deltaLogPaths;
  }

  @Override
  public void setMaxCommitTime(String maxInstantTime) {
    this.maxInstantTime = maxInstantTime;
  }

  @Override
  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public void setHoodieVirtualKeyInfo(Option<HoodieVirtualKeyInfo> hoodieVirtualKeyInfo) {}

}
