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

public class CommitInfo implements Serializable {

  public Long timestamp;
  public String operation;
  public OperationParameters operationParameters;
  public Integer readVersion;
  public Boolean isBlindAppend;
  public OperationMetrics operationMetrics;

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public OperationParameters getOperationParameters() {
    return operationParameters;
  }

  public void setOperationParameters(OperationParameters operationParameters) {
    this.operationParameters = operationParameters;
  }

  public Boolean getIsBlindAppend() {
    return isBlindAppend;
  }

  public void setIsBlindAppend(Boolean blindAppend) {
    isBlindAppend = blindAppend;
  }

  public OperationMetrics getOperationMetrics() {
    return operationMetrics;
  }

  public void setOperationMetrics(OperationMetrics operationMetrics) {
    this.operationMetrics = operationMetrics;
  }

  public Integer getReadVersion() {
    return readVersion;
  }

  public void setReadVersion(Integer readVersion) {
    this.readVersion = readVersion;
  }
}
