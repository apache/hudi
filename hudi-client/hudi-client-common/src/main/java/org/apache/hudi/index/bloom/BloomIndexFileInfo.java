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

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.Option;

import lombok.EqualsAndHashCode;
import lombok.ToString;
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

  /**
   * The latest base file of the file group, when the loader resolved it. It is not part of the
   * equality, which covers the file group and its key range.
   */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  Option<HoodieBaseFile> baseFile;

  public BloomIndexFileInfo(String fileId, String minRecordKey, String maxRecordKey) {
    this(fileId, minRecordKey, maxRecordKey, Option.empty());
  }

  public BloomIndexFileInfo(String fileId) {
    this(fileId, null, null, Option.empty());
  }

  public BloomIndexFileInfo(HoodieBaseFile baseFile, String minRecordKey, String maxRecordKey) {
    this(baseFile.getFileId(), minRecordKey, maxRecordKey, Option.of(baseFile));
  }

  public BloomIndexFileInfo(HoodieBaseFile baseFile) {
    this(baseFile.getFileId(), null, null, Option.of(baseFile));
  }

  private BloomIndexFileInfo(String fileId, String minRecordKey, String maxRecordKey, Option<HoodieBaseFile> baseFile) {
    this.fileId = fileId;
    this.minRecordKey = minRecordKey;
    this.maxRecordKey = maxRecordKey;
    this.baseFile = baseFile;
  }

  /**
   * Returns this file info without the base file, for when only the file group and its key range are needed.
   */
  public BloomIndexFileInfo withoutBaseFile() {
    return baseFile.isPresent() ? new BloomIndexFileInfo(fileId, minRecordKey, maxRecordKey) : this;
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
