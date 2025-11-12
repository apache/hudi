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

package org.apache.hudi.common.table.read.buffer;

import java.util.Objects;

class TestRecord {
  private final String recordKey;
  private final int value;

  public TestRecord(String recordKey, int value) {
    this.recordKey = recordKey;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestRecord that = (TestRecord) o;
    return value == that.value && Objects.equals(recordKey, that.recordKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordKey, value);
  }

  public String getRecordKey() {
    return recordKey;
  }

  public int getValue() {
    return value;
  }
}
