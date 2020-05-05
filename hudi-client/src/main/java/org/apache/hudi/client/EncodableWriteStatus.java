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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple3;

public class EncodableWriteStatus implements Serializable {

  private static final long serialVersionUID = 1L;
  private String recordKeyProp;
  public String fileId;
  public String partitionPath;
  public List<Row> successRows = new ArrayList<>();
  public List<Tuple3<Row, String, Throwable>> failedRows = new ArrayList<>();
  public Throwable globalError;
  public Path path;
  public long endTime;
  public long recordsWritten;
  public long insertRecordsWritten;
  private HoodieWriteStat stat;

  public EncodableWriteStatus() {
  }

  public EncodableWriteStatus(String recordKeyProp) {
    this.recordKeyProp = recordKeyProp;
  }

  public void markSuccess(Row row) {
    this.successRows.add(row);
  }

  public void markFailure(Row row, Throwable t, Option<Map<String, String>> optionalRecordMetadata) {
    // Guaranteed to have at-least one error
    failedRows.add(new Tuple3<>(row, row.getAs(recordKeyProp), t));
  }

  public void markFailure(Row row, String recordKey, Throwable t) {
    // Guaranteed to have at-least one error
    failedRows.add(new Tuple3<>(row, recordKey, t));
  }

  public boolean hasErrors() {
    return failedRows.size() != 0;
  }

  public HoodieWriteStat getStat() {
    return stat;
  }

  public void setStat(HoodieWriteStat stat) {
    this.stat = stat;
  }

  /*public String getFileId() {
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

  public List<Row> getSuccessRows() {
    return successRows;
  }

  public void setSuccessRows(List<Row> successRows) {
    this.successRows = successRows;
  }

  public List<Tuple3<Row, String, Throwable>> getFailedRows() {
    return failedRows;
  }

  public void setFailedRows(List<Tuple3<Row, String, Throwable>> failedRows) {
    this.failedRows = failedRows;
  }

  public Throwable getGlobalError() {
    return globalError;
  }

  public void setGlobalError(Throwable globalError) {
    this.globalError = globalError;
  }

  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getRecordsWritten() {
    return recordsWritten;
  }

  public void setRecordsWritten(long recordsWritten) {
    this.recordsWritten = recordsWritten;
  }

  public long getInsertRecordsWritten() {
    return insertRecordsWritten;
  }

  public void setInsertRecordsWritten(long insertRecordsWritten) {
    this.insertRecordsWritten = insertRecordsWritten;
  } */

  @Override
  public String toString() {
    return "PartitionPath " + partitionPath + ", FileID " + fileId + ", Success records " + successRows.size()
        + ", errored Rows " + failedRows.size() + ", global error " + (globalError != null) + ", end time " + endTime;
  }
}
