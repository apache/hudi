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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.Serializable;
import java.util.ArrayList;

public class LockInfo implements Serializable {
  // FIXME-vc: without serializable, these fail: TestAlterTable, ..
  private String lockCreateTime;
  private String lockThreadName;
  private ArrayList<String> lockStacksInfo;

  public String getLockCreateTime() {
    return lockCreateTime;
  }

  public void setLockCreateTime(String lockCreateTime) {
    this.lockCreateTime = lockCreateTime;
  }

  public String getLockThreadName() {
    return lockThreadName;
  }

  public void setLockThreadName(String lockThreadName) {
    this.lockThreadName = lockThreadName;
  }

  public ArrayList<String> getLockStacksInfo() {
    return lockStacksInfo;
  }

  public void setLockStacksInfo(StackTraceElement[] stacks) {
    lockStacksInfo = new ArrayList<>();
    for (StackTraceElement ste : stacks) {
      lockStacksInfo.add(String.format("%s.%s (%s:%s)", ste.getClassName(), ste.getMethodName(), ste.getFileName(), ste.getLineNumber()));
    }
  }

  @Override
  public String toString() {
    try {
      return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return e.toString();
    }
  }
}
