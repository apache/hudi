/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.clustering;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Set;

/**
 * Represents a cluster command from the clustering plan task {@link ClusteringPlanSourceFunction}.
 */
public class ClusteringFileEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private String fileID;

  private String partitionPath;

  private HoodieLSMLogFile logFile;

  private FileStatus fileStatus;

  private Pair<Set<String>, Set<String>> missingAndCompletedInstants;

  public ClusteringFileEvent() {
  }

  public ClusteringFileEvent(HoodieLSMLogFile logFile, String fileID, String partitionPath, FileStatus fileStatus) {
    this.logFile = logFile;
    this.fileID = fileID;
    this.partitionPath = partitionPath;
    this.fileStatus = fileStatus;
  }

  public ClusteringFileEvent(HoodieLSMLogFile logFile, String fileID, String partitionPath, FileStatus fileStatus, Pair<Set<String>, Set<String>> missingAndCompletedInstants) {
    this.logFile = logFile;
    this.fileID = fileID;
    this.partitionPath = partitionPath;
    this.fileStatus = fileStatus;
    this.missingAndCompletedInstants = missingAndCompletedInstants;
  }

  public HoodieLSMLogFile getLogFile() {
    return logFile;
  }

  public String  getFileID() {
    return fileID;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setFileID(String fileID) {
    this.fileID = fileID;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public void setLogFile(HoodieLSMLogFile logFile) {
    this.logFile = logFile;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public void setFileStatus(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
  }

  public Pair<Set<String>, Set<String>> getMissingAndCompletedInstants() {
    return missingAndCompletedInstants;
  }

  public void setMissingAndCompletedInstants(Pair<Set<String>, Set<String>> missingAndCompletedInstants) {
    this.missingAndCompletedInstants = missingAndCompletedInstants;
  }
}
