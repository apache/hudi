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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.io.MergeContext;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

/**
 * Helper class for a bucket's type (INSERT and UPDATE) and its file location.
 */
@AllArgsConstructor
@Getter
public class BucketInfo implements Serializable {

  BucketType bucketType;
  String fileIdPrefix;
  String partitionPath;
  // The number of update and delete records from input based on tagging. Populated only by the
  // Spark and Java upsert partitioners, which report 0 (known none) for INSERT buckets and
  // insert-only small-file buckets; other producers (e.g., bucket index, insert overwrite,
  // metadata table, Flink) leave it at MergeContext.UNKNOWN_NUM_UPDATES. Intentionally excluded
  // from equals/hashCode: bucket identity is (bucketType, fileIdPrefix, partitionPath).
  long numUpdates;

  public BucketInfo(BucketType bucketType, String fileIdPrefix, String partitionPath) {
    this(bucketType, fileIdPrefix, partitionPath, MergeContext.UNKNOWN_NUM_UPDATES);
  }

  @Override
  public String toString() {
    return "BucketInfo {" + "bucketType=" + bucketType + ", "
        + "fileIdPrefix=" + fileIdPrefix + ", "
        + "partitionPath=" + partitionPath + ", "
        + "numUpdates=" + numUpdates
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BucketInfo that = (BucketInfo) o;
    return bucketType == that.bucketType
        && fileIdPrefix.equals(that.fileIdPrefix)
        && partitionPath.equals(that.partitionPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucketType, fileIdPrefix, partitionPath);
  }
}
