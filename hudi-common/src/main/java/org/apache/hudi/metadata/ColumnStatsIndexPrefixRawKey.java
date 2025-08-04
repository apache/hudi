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

package org.apache.hudi.metadata;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;

import java.util.Objects;

/**
 * Represents a raw key for column stats index consisting of column name and optional partition name.
 */
public class ColumnStatsIndexPrefixRawKey implements MetadataRawKey {
  private static final long serialVersionUID = 1L;
  
  private final String columnName;
  private final Option<String> partitionName;
  
  public ColumnStatsIndexPrefixRawKey(String columnName) {
    this(columnName, Option.empty());
  }
  
  public ColumnStatsIndexPrefixRawKey(String columnName, String partitionName) {
    this(columnName, Option.of(partitionName));
  }
  
  public ColumnStatsIndexPrefixRawKey(String columnName, Option<String> partitionName) {
    this.columnName = Objects.requireNonNull(columnName, "Column name cannot be null");
    this.partitionName = Objects.requireNonNull(partitionName, "Partition name option cannot be null");
  }
  
  public String getColumnName() {
    return columnName;
  }
  
  public Option<String> getPartitionName() {
    return partitionName;
  }
  
  @Override
  public String encode() {
    ColumnIndexID columnIndexID = new ColumnIndexID(columnName);
    String encodedValue;
    
    if (partitionName.isPresent()) {
      PartitionIndexID partitionIndexId = new PartitionIndexID(
          HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier(partitionName.get()));
      encodedValue = columnIndexID.asBase64EncodedString()
          .concat(partitionIndexId.asBase64EncodedString());
    } else {
      encodedValue = columnIndexID.asBase64EncodedString();
    }
    
    return encodedValue;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnStatsIndexPrefixRawKey that = (ColumnStatsIndexPrefixRawKey) o;
    return Objects.equals(columnName, that.columnName) && Objects.equals(partitionName, that.partitionName);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(columnName, partitionName);
  }
  
  @Override
  public String toString() {
    return "ColumnStatsIndexKey{columnName='" + columnName + "', partitionName=" + partitionName + "}";
  }
}