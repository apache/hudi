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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.index.bucket.BucketIdentifier;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public abstract class BaseSparkBucketIndexBucketInfoGetter implements SparkBucketInfoGetter {

  private final Map<String, Set<String>> updatePartitionPathFileIds;
  private final boolean isOverwrite;
  private final boolean isNonBlockingConcurrencyControl;

  public BaseSparkBucketIndexBucketInfoGetter(Map<String, Set<String>> updatePartitionPathFileIds,
                                              boolean isOverwrite,
                                              boolean isNonBlockingConcurrencyControl) {
    if (isOverwrite) {
      ValidationUtils.checkArgument(!isNonBlockingConcurrencyControl,
          "Insert overwrite is not supported with non-blocking concurrency control");
    }
    this.updatePartitionPathFileIds = updatePartitionPathFileIds;
    this.isOverwrite = isOverwrite;
    this.isNonBlockingConcurrencyControl = isNonBlockingConcurrencyControl;
  }

  protected BucketInfo getBucketInfo(int bucketId, String partitionPath) {
    String bucketIdStr = BucketIdentifier.bucketIdStr(bucketId);
    // Insert overwrite always generates new bucket file id
    if (isOverwrite) {
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketIdStr), partitionPath);
    }
    Option<String> fileIdOption = Option.fromJavaOptional(updatePartitionPathFileIds
        .getOrDefault(partitionPath, Collections.emptySet()).stream()
        .filter(e -> e.startsWith(bucketIdStr))
        .findFirst());
    if (fileIdOption.isPresent()) {
      return new BucketInfo(BucketType.UPDATE, fileIdOption.get(), partitionPath);
    } else {
      // Always write into log file instead of base file if using NB-CC
      if (isNonBlockingConcurrencyControl) {
        return new BucketInfo(BucketType.UPDATE, BucketIdentifier.newBucketFileIdForNBCC(bucketIdStr), partitionPath);
      }
      return new BucketInfo(BucketType.INSERT, BucketIdentifier.newBucketFileIdPrefix(bucketIdStr), partitionPath);
    }
  }
}
