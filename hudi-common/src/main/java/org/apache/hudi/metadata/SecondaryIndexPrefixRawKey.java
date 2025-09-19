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
 * Represents a secondary index key prefix for index lookup path.
 * <p>
 * The SECONDARY_INDEX partition stores mappings from secondary column values to record keys,
 * enabling efficient lookups by non-primary key columns. Each secondary index is created
 * for a specific column to support fast queries on that column.
 * <p>
 * Raw prefix key format: <escaped_secondary_key>$
 * - The secondary key (column value) is escaped to handle special characters
 * - A '$' separator is appended for prefix matching
 * <p>
 * Examples:
 * - For secondary index on email "john@example.com":
 *   key = "john@example.com$"
 *   (Special characters like null and $ are escaped as needed)
 * <p>
 * - For secondary index on status "ACTIVE":
 *   key = "ACTIVE$"
 * <p>
 * - For secondary index on product_id "PROD-123-XYZ":
 *   key = "PROD-123-XYZ$"
 * <p>
 * - For secondary index with special characters "user$name@domain":
 *   key = "user\$name@domain$"
 *   (The '$' in the value is escaped to distinguish from the separator)
 * <p>
 * The complete secondary index record key format is: <escaped_secondary_key>$<escaped_record_key>
 * This prefix key enables range scans to find all records with a specific secondary key value.
 */
public class SecondaryIndexPrefixRawKey implements MetadataRawKey {
  private final String secondaryKey;

  public SecondaryIndexPrefixRawKey(String secondaryKey) {
    this.secondaryKey = secondaryKey;
  }

  @Override
  public String encode() {
    return SecondaryIndexKeyUtils.getEscapedSecondaryKeyPrefixFromSecondaryKey(secondaryKey);
  }

  public String secondaryKey() {
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