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

package org.apache.hudi.delta.sync;

import java.io.Serializable;

public class OperationMetrics implements Serializable {

  String numFiles;
  String numOutputBytes;
  String numOutputRows;
  Long numTargetRowsCopied;
  Long numTargetRowsDeleted;
  Long numTargetFilesAdded;
  Long executionTimeMs;
  Long numTargetRowsInserted;
  Long scanTimeMs;
  Long numTargetRowsUpdated;
  Long numSourceRows;
  Long numTargetFilesRemoved;
  Long rewriteTimeMs;

  public String getNumFiles() {
    return numFiles;
  }

  public void setNumFiles(String numFiles) {
    this.numFiles = numFiles;
  }

  public String getNumOutputBytes() {
    return numOutputBytes;
  }

  public void setNumOutputBytes(String numOutputBytes) {
    this.numOutputBytes = numOutputBytes;
  }

  public String getNumOutputRows() {
    return numOutputRows;
  }

  public void setNumOutputRows(String numOutputRows) {
    this.numOutputRows = numOutputRows;
  }

  public Long getNumTargetRowsCopied() {
    return numTargetRowsCopied;
  }

  public void setNumTargetRowsCopied(Long numTargetRowsCopied) {
    this.numTargetRowsCopied = numTargetRowsCopied;
  }

  public Long getNumTargetRowsDeleted() {
    return numTargetRowsDeleted;
  }

  public void setNumTargetRowsDeleted(Long numTargetRowsDeleted) {
    this.numTargetRowsDeleted = numTargetRowsDeleted;
  }

  public Long getNumTargetFilesAdded() {
    return numTargetFilesAdded;
  }

  public void setNumTargetFilesAdded(Long numTargetFilesAdded) {
    this.numTargetFilesAdded = numTargetFilesAdded;
  }

  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }

  public void setExecutionTimeMs(Long executionTimeMs) {
    this.executionTimeMs = executionTimeMs;
  }

  public Long getNumTargetRowsInserted() {
    return numTargetRowsInserted;
  }

  public void setNumTargetRowsInserted(Long numTargetRowsInserted) {
    this.numTargetRowsInserted = numTargetRowsInserted;
  }

  public Long getScanTimeMs() {
    return scanTimeMs;
  }

  public void setScanTimeMs(Long scanTimeMs) {
    this.scanTimeMs = scanTimeMs;
  }

  public Long getNumTargetRowsUpdated() {
    return numTargetRowsUpdated;
  }

  public void setNumTargetRowsUpdated(Long numTargetRowsUpdated) {
    this.numTargetRowsUpdated = numTargetRowsUpdated;
  }

  public Long getNumSourceRows() {
    return numSourceRows;
  }

  public void setNumSourceRows(Long numSourceRows) {
    this.numSourceRows = numSourceRows;
  }

  public Long getNumTargetFilesRemoved() {
    return numTargetFilesRemoved;
  }

  public void setNumTargetFilesRemoved(Long numTargetFilesRemoved) {
    this.numTargetFilesRemoved = numTargetFilesRemoved;
  }

  public Long getRewriteTimeMs() {
    return rewriteTimeMs;
  }

  public void setRewriteTimeMs(Long rewriteTimeMs) {
    this.rewriteTimeMs = rewriteTimeMs;
  }
}
