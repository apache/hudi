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
 * Represents a raw key for the FILES partition in the metadata table.
 * This uses identity encoding - the key is used as-is without transformation.
 */
public class FilesIndexRawKey implements RawKey {
  private final String key;

  public FilesIndexRawKey(String key) {
    this.key = Objects.requireNonNull(key);
  }

  @Override
  public String encode() {
    // Identity encoding - return the key as-is
    return key;
  }

  public String getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilesIndexRawKey that = (FilesIndexRawKey) o;
    return Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }

  @Override
  public String toString() {
    return "FilesPartitionRawKey{" + "key='" + key + '\'' + '}';
  }
}