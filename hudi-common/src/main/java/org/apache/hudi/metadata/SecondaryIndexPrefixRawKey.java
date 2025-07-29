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
 * Represents a secondary index key, whose raw content is the column value of the data table.
 */
public class SecondaryIndexPrefixRawKey implements RawKey {
  private final String secondaryKey;

  public SecondaryIndexPrefixRawKey(String secondaryKey) {
    this.secondaryKey = secondaryKey;
  }

  @Override
  public String encode() {
    return SecondaryIndexKeyUtils.getEscapedSecondaryKeyPrefixFromSecondaryKey(secondaryKey);
  }

  public String getSecondaryKey() {
    return secondaryKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SecondaryIndexPrefixRawKey that = (SecondaryIndexPrefixRawKey) o;
    return Objects.equals(secondaryKey, that.secondaryKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(secondaryKey);
  }

  @Override
  public String toString() {
    return "SecondaryIndexKey{" + "secondaryKey='" + secondaryKey + '\'' + '}';
  }
}