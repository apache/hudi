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

/**
 * Lean version of {@link WriteStatus} where all additional stats like rli, col sats etc are trimmed off.
 */
public class LeanWriteStatus extends WriteStatus {

  public LeanWriteStatus(WriteStatus writeStatus) {
    super();
    this.setFileId(writeStatus.getFileId());
    this.setGlobalError(writeStatus.getGlobalError());
    this.setStat(writeStatus.getStat());
    this.setIsMetadata(writeStatus.isMetadataTable());
    this.setPartitionPath(writeStatus.getPartitionPath());
    this.setTotalErrorRecords(writeStatus.getTotalErrorRecords());
    this.setTotalRecords(writeStatus.getTotalRecords());
    this.setStat(writeStatus.getStat());
    setFailedRecords(writeStatus);
    setErrors(writeStatus);
  }

  private void setFailedRecords(WriteStatus writeStatus) {
    this.failedRecords = writeStatus.getFailedRecords();
  }

  private void setErrors(WriteStatus writeStatus) {
    this.errors = writeStatus.getErrors();
  }
}
