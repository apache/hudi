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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Statistics about a single Hoodie delta log operation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieDeltaWriteStat extends HoodieWriteStat {

  private int logVersion;
  private long logOffset;
  private String baseFile;
  private List<String> logFiles = new ArrayList<>();

  public void setLogVersion(int logVersion) {
    this.logVersion = logVersion;
  }

  public int getLogVersion() {
    return logVersion;
  }

  public void setLogOffset(long logOffset) {
    this.logOffset = logOffset;
  }

  public long getLogOffset() {
    return logOffset;
  }

  public void setBaseFile(String baseFile) {
    this.baseFile = baseFile;
  }

  public String getBaseFile() {
    return baseFile;
  }

  public void setLogFiles(List<String> logFiles) {
    this.logFiles = logFiles;
  }

  public void addLogFiles(String logFile) {
    logFiles.add(logFile);
  }

  public List<String> getLogFiles() {
    return logFiles;
  }
}
