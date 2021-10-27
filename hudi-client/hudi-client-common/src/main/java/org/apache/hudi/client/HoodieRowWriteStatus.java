/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieWriteStat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HoodieRowWriteStatus implements ReportsWrite, Serializable {

  private static final long serialVersionUID = 1L;
  private static final long RANDOM_SEED = 9038412832L;

  private String fileId;
  private String partitionPath;
  private List<String> successRecordKeys = new ArrayList<>();
  private List<String> failedRecordKeys = new ArrayList<>();

  private HoodieWriteStat stat;

  private long totalRecords = 0;
  private long totalErrorRecords = 0;
  private String globalError = null;

  private final double failureFraction;
  private final boolean trackSuccessRecords;
  private final transient Random random;

  public HoodieRowWriteStatus() {
    this(true, 0.1);
  }

  public HoodieRowWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
    this.trackSuccessRecords = trackSuccessRecords;
    this.failureFraction = failureFraction;
    this.random = new Random(RANDOM_SEED);
  }

  public void markSuccess(String recordKey) {
    if (trackSuccessRecords) {
      this.successRecordKeys.add(recordKey);
    }
    totalRecords++;
  }

  public void markSuccess() {
    totalRecords++;
  }

  public void markFailure(String recordKey, Throwable t) {
    if (failedRecordKeys.isEmpty() || (random.nextDouble() <= failureFraction)) {
      failedRecordKeys.add(recordKey);
    }
    totalRecords++;
  }

  public boolean hasErrors() {
    return failedRecordKeys.size() != 0;
  }

  @Override
  public boolean hasGlobalError() {
    return globalError != null;
  }

  public HoodieWriteStat getStat() {
    return stat;
  }

  public void setStat(HoodieWriteStat stat) {
    this.stat = stat;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public List<String> getSuccessRecordKeys() {
    return successRecordKeys;
  }

  public long getFailedRowsSize() {
    return failedRecordKeys.size();
  }

  public List<String> getFailedRecordKeys() {
    return failedRecordKeys;
  }

  public void setFailedRecordKeys(List<String> failedRecordKeys) {
    this.failedRecordKeys = failedRecordKeys;
  }

  public long getTotalRecords() {
    return totalRecords;
  }

  public void setTotalRecords(long totalRecords) {
    this.totalRecords = totalRecords;
  }

  public long getTotalErrorRecords() {
    return totalErrorRecords;
  }

  public void setTotalErrorRecords(long totalErrorRecords) {
    this.totalErrorRecords = totalErrorRecords;
  }

  public String getGlobalError() {
    return globalError;
  }

  public void setGlobalError(String globalError) {
    this.globalError = globalError;
  }

  public void setSuccessRecordKeys(List<String> successRecordKeys) {
    this.successRecordKeys = successRecordKeys;
  }

  @Override
  public String toString() {
    return "PartitionPath " + partitionPath + ", FileID " + fileId + ", Success records "
        + totalRecords + ", errored Rows " + totalErrorRecords
        + ", global error " + (globalError != null);
  }
}
