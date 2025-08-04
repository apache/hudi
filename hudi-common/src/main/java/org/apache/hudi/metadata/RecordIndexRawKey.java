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

package org.apache.hudi.metadata;

import java.util.Objects;

/**
 * Represents a record index key that requires no encoding (identity encoding).
 */
public class RecordIndexRawKey implements MetadataRawKey {
  private final String recordKey;

  public RecordIndexRawKey(String recordKey) {
    this.recordKey = Objects.requireNonNull(recordKey);
  }

  @Override
  public String encode() {
    // Identity encoding - return the key as-is
    return recordKey;
  }

  public String getRecordKey() {
    return recordKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordIndexRawKey that = (RecordIndexRawKey) o;
    return Objects.equals(recordKey, that.recordKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordKey);
  }

  @Override
  public String toString() {
    return "RecordIndexRawKey{" + "recordKey='" + recordKey + '\'' + '}';
  }
}