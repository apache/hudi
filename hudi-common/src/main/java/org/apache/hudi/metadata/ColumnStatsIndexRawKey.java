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

import lombok.NonNull;
import lombok.Value;

/**
 * Represents a raw key for column stats indexed by partition, file and column.
 * This is different from ColumnStatsIndexPrefixRawKey which is used for prefix lookups.
 */
@Value
public class ColumnStatsIndexRawKey implements RawKey {

  @NonNull
  String partitionName;
  @NonNull
  String fileName;
  @NonNull
  String columnName;

  @Override
  public String encode() {
    return HoodieMetadataPayload.getColumnStatsIndexKey(
        new PartitionIndexID(HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier(partitionName)),
        new FileIndexID(fileName),
        new ColumnIndexID(columnName));
  }
}