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

package org.apache.hudi.index.bloom;

import lombok.Value;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata about a given file group, useful for index lookup.
 */
@Value
public class BloomIndexFileInfo implements Serializable {

  String fileId;

  String minRecordKey;

  String maxRecordKey;

  public BloomIndexFileInfo(String fileId, String minRecordKey, String maxRecordKey) {
    this.fileId = fileId;
    this.minRecordKey = minRecordKey;
    this.maxRecordKey = maxRecordKey;
  }

  public BloomIndexFileInfo(String fileId) {
    this.fileId = fileId;
    this.minRecordKey = null;
    this.maxRecordKey = null;
  }

  public boolean hasKeyRanges() {
    return minRecordKey != null && maxRecordKey != null;
  }

  /**
   * Does the given key fall within the range (inclusive).
   */
  public boolean isKeyInRange(String recordKey) {
    return Objects.requireNonNull(minRecordKey).compareTo(recordKey) <= 0
        && Objects.requireNonNull(maxRecordKey).compareTo(recordKey) >= 0;
  }
}
