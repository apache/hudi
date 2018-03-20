/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.index.bloom;

import com.google.common.base.Objects;
import java.io.Serializable;

/**
 * Metadata about a given file, useful for index lookup
 */
public class BloomIndexFileInfo implements Serializable {

  private final String fileName;

  private final String minRecordKey;

  private final String maxRecordKey;

  public BloomIndexFileInfo(String fileName, String minRecordKey, String maxRecordKey) {
    this.fileName = fileName;
    this.minRecordKey = minRecordKey;
    this.maxRecordKey = maxRecordKey;
  }

  public BloomIndexFileInfo(String fileName) {
    this.fileName = fileName;
    this.minRecordKey = null;
    this.maxRecordKey = null;
  }

  public String getFileName() {
    return fileName;
  }

  public String getMinRecordKey() {
    return minRecordKey;
  }

  public String getMaxRecordKey() {
    return maxRecordKey;
  }

  public boolean hasKeyRanges() {
    return minRecordKey != null && maxRecordKey != null;
  }

  /**
   * Does the given key fall within the range (inclusive)
   */
  public boolean isKeyInRange(String recordKey) {
    return minRecordKey.compareTo(recordKey) <= 0 && maxRecordKey.compareTo(recordKey) >= 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BloomIndexFileInfo that = (BloomIndexFileInfo) o;
    return Objects.equal(that.fileName, fileName) && Objects.equal(that.minRecordKey, minRecordKey)
        && Objects.equal(that.maxRecordKey, maxRecordKey);

  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fileName, minRecordKey, maxRecordKey);
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder("BloomIndexFileInfo {");
    sb.append(" fileName=").append(fileName);
    sb.append(" minRecordKey=").append(minRecordKey);
    sb.append(" maxRecordKey=").append(maxRecordKey);
    sb.append('}');
    return sb.toString();
  }
}
