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

import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;

import java.util.Objects;

/**
 * Represents a raw key for column stats indexed by partition, file and column.
 * This is different from ColumnStatsIndexPrefixRawKey which is used for prefix lookups.
 * <p>
 * The COLUMN_STATS partition stores statistics for each column in each file to enable
 * query optimization through data skipping. Statistics include min/max values, null counts, etc.
 * <p>
 * Raw key format: base64(column_name) + base64(partition_identifier) + base64(file_name)
 * - column_name: The name of the column
 * - partition_identifier: The partition path (or "." for non-partitioned tables)
 * - file_name: Name of the data file
 * <p>
 * Examples:
 * - For column "price" in file "f1.parquet" in partition "2023/01/15":
 *   key = base64("price") + base64("2023/01/15") + base64("f1.parquet")
 *   Example encoded: "cHJpY2U=" + "MjAyMy8wMS8xNQ==" + "ZjEucGFycXVldA=="
 * <p>
 * - For column "user_id" in file "data.parquet" in non-partitioned table:
 *   key = base64("user_id") + base64(".") + base64("data.parquet")
 *   Example encoded: "dXNlcl9pZA==" + "X19ISVZFX0RFRkFVTFRfUEFSVElUSU9OX18=" + "ZGF0YS5wYXJxdWV0"
 * <p>
 * - For column "revenue" in file "sales.parquet" in partition "country=US/state=CA":
 *   key = base64("revenue") + base64("country=US/state=CA") + base64("sales.parquet")
 *   Example encoded: "cmV2ZW51ZQ==" + "Y291bnRyeT1VUy9zdGF0ZT1DQQ==" + "c2FsZXMucGFycXVldA=="
 * <p>
 * The value contains statistics like min/max values, null count, value count, and total size.
 */
public class ColumnStatsIndexRawKey implements MetadataRawKey {
  private final String partitionName;
  private final String fileName;
  private final String columnName;

  public ColumnStatsIndexRawKey(String partitionName, String fileName, String columnName) {
    this.partitionName = Objects.requireNonNull(partitionName);
    this.fileName = Objects.requireNonNull(fileName);
    this.columnName = Objects.requireNonNull(columnName);
  }

  @Override
  public String encode() {
    return HoodieMetadataPayload.getColumnStatsIndexKey(
        new PartitionIndexID(HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier(partitionName)),
        new FileIndexID(fileName),
        new ColumnIndexID(columnName));
  }

  public String partitionName() {
    return partitionName;
  }

  public String fileName() {
    return fileName;
  }

  public String columnName() {
    return columnName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnStatsIndexRawKey that = (ColumnStatsIndexRawKey) o;
    return Objects.equals(partitionName, that.partitionName)
        && Objects.equals(fileName, that.fileName)
        && Objects.equals(columnName, that.columnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionName, fileName, columnName);
  }

  @Override
  public String toString() {
    return "ColumnStatsFileRawKey{" + "partitionName='" + partitionName + '\''
        + ", fileName='" + fileName + '\'' + ", columnName='" + columnName + '\'' + '}';
  }
}