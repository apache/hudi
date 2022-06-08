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

package org.apache.hudi.table.management.entity;

public enum InstanceStatus {

  SCHEDULED(0, "scheduled"),
  RUNNING(1, "running"),
  FAILED(2, "failed"),
  INVALID(3, "invalid"),
  COMPLETED(4, "completed");

  private int status;
  private String desc;

  InstanceStatus(int status, String desc) {
    this.status = status;
    this.desc = desc;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }

  public static InstanceStatus getInstance(int status) {
    for (InstanceStatus instanceStatus : InstanceStatus.values()) {
      if (instanceStatus.getStatus() == status) {
        return instanceStatus;
      }
    }
    throw new RuntimeException("Invalid instance status: " + status);
  }
}
