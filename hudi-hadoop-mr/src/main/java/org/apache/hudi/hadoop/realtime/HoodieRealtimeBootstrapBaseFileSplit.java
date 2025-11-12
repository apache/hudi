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

/**
 * Realtime {@link FileSplit} with external base file
 *
 * NOTE: If you're adding fields here you need to make sure that you appropriately de-/serialize them
 *       in {@link #readFromInput(DataInput)} and {@link #writeToOutput(DataOutput)}
 */
public class HoodieRealtimeBootstrapBaseFileSplit extends BootstrapBaseFileSplit implements RealtimeSplit {
  /**
   * Marks whether this path produced as part of Incremental Query
   */
  private boolean belongsToIncrementalQuery = false;
  /**
   * List of delta log-files holding updated records for this base-file
   */
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();
  /**
   * Latest commit instant available at the time of the query in which all of the files
   * pertaining to this split are represented
   */
  private String maxCommitTime;
  /**
   * Base path of the table this path belongs to
   */
  private String basePath;
  /**
   * Virtual key configuration of the table this split belongs to
   */
  private Option<HoodieVirtualKeyInfo> virtualKeyInfo = Option.empty();

  /**
   * NOTE: This ctor is necessary for Hive to be able to serialize and
   *       then instantiate it when deserializing back
   */
  public HoodieRealtimeBootstrapBaseFileSplit() {
  }

  public HoodieRealtimeBootstrapBaseFileSplit(FileSplit baseSplit,
                                              String basePath,
                                              List<HoodieLogFile> deltaLogFiles,
                                              String maxInstantTime,
                                              FileSplit externalFileSplit,
                                              boolean belongsToIncrementalQuery,
                                              Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt) throws IOException {
    super(baseSplit, externalFileSplit);
    this.maxCommitTime = maxInstantTime;
    this.deltaLogFiles = deltaLogFiles;
    this.basePath = basePath;
    this.belongsToIncrementalQuery = belongsToIncrementalQuery;
    this.virtualKeyInfo = virtualKeyInfoOpt;
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
  public List<HoodieLogFile> getDeltaLogFiles() {
    return deltaLogFiles;
  }

  @Override
  public void setDeltaLogFiles(List<HoodieLogFile> deltaLogFiles) {
    this.deltaLogFiles = deltaLogFiles;
  }

  @Override
  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  @Override
  public String getBasePath() {
    return basePath;
  }

  @Override
  public Option<HoodieVirtualKeyInfo> getVirtualKeyInfo() {
    return virtualKeyInfo;
  }

  @Override
  public boolean getBelongsToIncrementalQuery() {
    return belongsToIncrementalQuery;
  }

  @Override
  public void setBelongsToIncrementalQuery(boolean belongsToIncrementalPath) {
    this.belongsToIncrementalQuery = belongsToIncrementalPath;
  }

  @Override
  public void setMaxCommitTime(String maxInstantTime) {
    this.maxCommitTime = maxInstantTime;
  }

  @Override
  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public void setVirtualKeyInfo(Option<HoodieVirtualKeyInfo> virtualKeyInfo) {
    this.virtualKeyInfo = virtualKeyInfo;
  }
}
