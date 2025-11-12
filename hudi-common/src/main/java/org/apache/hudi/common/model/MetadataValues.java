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

package org.apache.hudi.common.model;

public class MetadataValues {

  // NOTE: These fields are laid out in the same order as they are encoded in
  //       each record and that should be preserved
  private String commitTime;
  private String commitSeqNo;
  private String recordKey;
  private String partitionPath;
  private String fileName;
  private String operation;

  private boolean set = false;

  public MetadataValues() {
  }

  public String getCommitTime() {
    return commitTime;
  }

  public String getCommitSeqNo() {
    return commitSeqNo;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getFileName() {
    return fileName;
  }

  public String getOperation() {
    return operation;
  }

  public MetadataValues setCommitTime(String value) {
    this.commitTime = value;
    this.set = true;
    return this;
  }

  public MetadataValues setCommitSeqno(String value) {
    this.commitSeqNo = value;
    this.set = true;
    return this;
  }

  public MetadataValues setRecordKey(String value) {
    this.recordKey = value;
    this.set = true;
    return this;
  }

  public MetadataValues setPartitionPath(String value) {
    this.partitionPath = value;
    this.set = true;
    return this;
  }

  public MetadataValues setFileName(String value) {
    this.fileName = value;
    this.set = true;
    return this;
  }

  public MetadataValues setOperation(String value) {
    this.operation = value;
    this.set = true;
    return this;
  }

  public boolean isEmpty() {
    return !set;
  }

  public String[] getValues() {
    return new String[] {
        // NOTE: These fields are laid out in the same order as they are encoded in
        //       each record and that should be preserved
        commitTime,
        commitSeqNo,
        recordKey,
        partitionPath,
        fileName,
        operation
    };
  }
}
