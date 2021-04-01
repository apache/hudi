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

import java.io.Serializable;
import java.util.Objects;

/**
 * Helper class for a bucket's type (INSERT and UPDATE) and its file location.
 */
public class BucketInfo implements Serializable {

  BucketType bucketType;
  String fileIdPrefix;
  String partitionPath;

  public BucketInfo(BucketType bucketType, String fileIdPrefix, String partitionPath) {
    this.bucketType = bucketType;
    this.fileIdPrefix = fileIdPrefix;
    this.partitionPath = partitionPath;
  }

  public BucketType getBucketType() {
    return bucketType;
  }

  public String getFileIdPrefix() {
    return fileIdPrefix;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("BucketInfo {");
    sb.append("bucketType=").append(bucketType).append(", ");
    sb.append("fileIdPrefix=").append(fileIdPrefix).append(", ");
    sb.append("partitionPath=").append(partitionPath);
    sb.append('}');
    return sb.toString();
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
