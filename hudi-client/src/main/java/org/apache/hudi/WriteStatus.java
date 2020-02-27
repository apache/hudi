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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Status of a write operation.
 */
public class WriteStatus implements Serializable {

  private static final long RANDOM_SEED = 9038412832L;

  private final HashMap<HoodieKey, Throwable> errors = new HashMap<>();

  private final List<HoodieRecord> writtenRecords = new ArrayList<>();

  private final List<HoodieRecord> failedRecords = new ArrayList<>();

  private Throwable globalError = null;

  private String fileId = null;

  private String partitionPath = null;

  private HoodieWriteStat stat = null;

  private long totalRecords = 0;
  private long totalErrorRecords = 0;

  private final double failureFraction;
  private final boolean trackSuccessRecords;
  private final transient Random random;

  public WriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
    this.trackSuccessRecords = trackSuccessRecords;
    this.failureFraction = failureFraction;
    this.random = new Random(RANDOM_SEED);
  }

  /**
   * Mark write as success, optionally using given parameters for the purpose of calculating some aggregate metrics.
   * This method is not meant to cache passed arguments, since WriteStatus objects are collected in Spark Driver.
   *
   * @param record deflated {@code HoodieRecord} containing information that uniquely identifies it.
   * @param optionalRecordMetadata optional metadata related to data contained in {@link HoodieRecord} before deflation.
   */
  public void markSuccess(HoodieRecord record, Option<Map<String, String>> optionalRecordMetadata) {
    if (trackSuccessRecords) {
      writtenRecords.add(record);
    }
    totalRecords++;
  }

  /**
   * Mark write as failed, optionally using given parameters for the purpose of calculating some aggregate metrics. This
   * method is not meant to cache passed arguments, since WriteStatus objects are collected in Spark Driver.
   *
   * @param record deflated {@code HoodieRecord} containing information that uniquely identifies it.
   * @param optionalRecordMetadata optional metadata related to data contained in {@link HoodieRecord} before deflation.
   */
  public void markFailure(HoodieRecord record, Throwable t, Option<Map<String, String>> optionalRecordMetadata) {
    if (failedRecords.isEmpty() || (random.nextDouble() <= failureFraction)) {
      // Guaranteed to have at-least one error
      failedRecords.add(record);
      errors.put(record.getKey(), t);
    }
    totalRecords++;
    totalErrorRecords++;
  }

  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public boolean hasErrors() {
    return totalErrorRecords > 0;
  }

  public boolean isErrored(HoodieKey key) {
    return errors.containsKey(key);
  }

  public HashMap<HoodieKey, Throwable> getErrors() {
    return errors;
  }

  public boolean hasGlobalError() {
    return globalError != null;
  }

  public Throwable getGlobalError() {
    return this.globalError;
  }

  public void setGlobalError(Throwable t) {
    this.globalError = t;
  }

  public List<HoodieRecord> getWrittenRecords() {
    return writtenRecords;
  }

  public List<HoodieRecord> getFailedRecords() {
    return failedRecords;
  }

  public HoodieWriteStat getStat() {
    return stat;
  }

  public void setStat(HoodieWriteStat stat) {
    this.stat = stat;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WriteStatus {");
    sb.append("fileId=").append(fileId);
    sb.append(", globalError='").append(globalError).append('\'');
    sb.append(", hasErrors='").append(hasErrors()).append('\'');
    sb.append(", errorCount='").append(totalErrorRecords).append('\'');
    sb.append(", errorPct='").append((100.0 * totalErrorRecords) / totalRecords).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
